"""P5 — parquet writer that stamps schema version into every file.

Layout::

    <root>/<stream_rel_dir>/<hour_key>.parquet          # canonical hourly
    <root>/<stream_rel_dir>/.parts/<hour_key>_NNNNN.parquet   # parts

Every parquet file (both parts and compacted canonical) carries this
schema-level key/value metadata::

    recorder_version     str  — see version.RECORDER_VERSION
    schema_version       str  — per stream type, version.SCHEMA_VERSIONS
    schema_id            str  — "{stream_type}:{schema_version}"
    source_id            str  — e.g. "binance_futures_ws"
    source_api_version   str  — version.SOURCE_API_VERSIONS[source_id]
    exchange             str
    symbol               str
    stream_type          str  — StreamType.value
    hour_key             str  — "YYYYMMDD_HH"
    host_id              str  — version.HOST_ID
    recorded_at          str  — ISO8601 UTC at file-write time
    producer             str  — "chronos"

Consumers read this with::

    table = pq.read_table(path)
    meta = {k.decode(): v.decode() for k, v in table.schema.metadata.items()}

and dispatch on ``schema_id`` to the right reader function.
"""
from __future__ import annotations

import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .version import (
    HOST_ID,
    RECORDER_VERSION,
    SCHEMA_VERSIONS,
    SOURCE_API_VERSIONS,
    StreamType,
)

logger = logging.getLogger(__name__)

PARTS_SUBDIR = ".parts"
_COMPRESSION = "zstd"  # ~1.3× smaller than snappy at comparable CPU on parquet
_PRODUCER = "chronos"


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _build_metadata(
    *,
    stream_type: StreamType,
    source_id: str,
    exchange: str,
    symbol: str,
    hour_key: str,
    extra: dict[str, str] | None = None,
) -> dict[bytes, bytes]:
    schema_version = SCHEMA_VERSIONS[stream_type]
    source_api_version = SOURCE_API_VERSIONS.get(source_id, "unknown")
    meta: dict[str, str] = {
        "recorder_version": RECORDER_VERSION,
        "schema_version": schema_version,
        "schema_id": f"{stream_type.value}:{schema_version}",
        "source_id": source_id,
        "source_api_version": source_api_version,
        "exchange": exchange,
        "symbol": symbol,
        "stream_type": stream_type.value,
        "hour_key": hour_key,
        "host_id": HOST_ID,
        "recorded_at": _utc_iso_now(),
        "producer": _PRODUCER,
    }
    if extra:
        meta.update(extra)
    return {k.encode(): v.encode() for k, v in meta.items()}


def _stamp(table: pa.Table, meta: dict[bytes, bytes]) -> pa.Table:
    """Attach v2 metadata to the table's schema, preserving user columns."""
    existing = table.schema.metadata or {}
    merged: dict[bytes, bytes] = {**existing, **meta}
    return table.replace_schema_metadata(merged)


class ParquetWriter:
    """Parts → hourly canonical compaction with version metadata.

    Thread-safe across stream keys. A single stream key must be written
    from one thread at a time (the orchestrator serializes this).
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._part_seq: dict[tuple[Path, str], int] = defaultdict(int)
        self._lock = threading.Lock()

    # --- path helpers ---

    def _stream_dir(self, stream_rel_dir: Path) -> Path:
        d = self._root / stream_rel_dir
        d.mkdir(parents=True, exist_ok=True)
        (d / PARTS_SUBDIR).mkdir(parents=True, exist_ok=True)
        return d

    def _next_part_path(self, stream_rel_dir: Path, hour_key: str) -> Path:
        d = self._stream_dir(stream_rel_dir)
        key = (stream_rel_dir, hour_key)
        with self._lock:
            seq = self._part_seq[key]
            self._part_seq[key] = seq + 1
        return d / PARTS_SUBDIR / f"{hour_key}_{seq:05d}.parquet"

    def _canonical_path(self, stream_rel_dir: Path, hour_key: str) -> Path:
        return self._stream_dir(stream_rel_dir) / f"{hour_key}.parquet"

    # --- atomic write ---

    @staticmethod
    def _atomic_write(table: pa.Table, path: Path) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        pq.write_table(
            table,
            tmp,
            compression=_COMPRESSION,
            use_dictionary=True,
            write_statistics=True,
            store_schema=True,
        )
        tmp.replace(path)

    # --- public API ---

    def write_part(
        self,
        *,
        table: pa.Table,
        stream_rel_dir: Path,
        hour_key: str,
        stream_type: StreamType,
        source_id: str,
        exchange: str,
        symbol: str,
        extra_metadata: dict[str, str] | None = None,
    ) -> Path:
        """Write one part file with full v2 schema metadata."""
        meta = _build_metadata(
            stream_type=stream_type,
            source_id=source_id,
            exchange=exchange,
            symbol=symbol,
            hour_key=hour_key,
            extra=extra_metadata,
        )
        stamped = _stamp(table, meta)
        path = self._next_part_path(stream_rel_dir, hour_key)
        self._atomic_write(stamped, path)
        return path

    def compact_hour(
        self,
        *,
        stream_rel_dir: Path,
        hour_key: str,
        stream_type: StreamType,
        source_id: str,
        exchange: str,
        symbol: str,
    ) -> Path | None:
        """Merge all parts for ``hour_key`` into one canonical file.

        Returns the canonical path on success, ``None`` if no parts exist.
        """
        d = self._stream_dir(stream_rel_dir)
        parts_dir = d / PARTS_SUBDIR
        parts = sorted(parts_dir.glob(f"{hour_key}_*.parquet"))
        if not parts:
            return None

        tables: list[pa.Table] = []
        for p in parts:
            try:
                tables.append(pq.read_table(p))
            except Exception as e:
                logger.error("compact: failed to read part %s: %r", p, e)

        canonical = self._canonical_path(stream_rel_dir, hour_key)
        if canonical.exists():
            # Re-entry after a same-hour crash: include the previous compact
            # so we don't drop data. Place it first to preserve time order.
            try:
                tables.insert(0, pq.read_table(canonical))
            except Exception as e:
                logger.error(
                    "compact: failed to read existing canonical %s: %r",
                    canonical, e,
                )

        if not tables:
            return None

        merged = pa.concat_tables(tables, promote_options="default")
        # Re-stamp the compacted file with a fresh recorded_at and the same
        # version fields. Parts' metadata is preserved inside file footer
        # schema, but the canonical file is the authoritative one.
        meta = _build_metadata(
            stream_type=stream_type,
            source_id=source_id,
            exchange=exchange,
            symbol=symbol,
            hour_key=hour_key,
            extra={"compacted_parts": str(len(parts))},
        )
        stamped = _stamp(merged, meta)
        self._atomic_write(stamped, canonical)

        for p in parts:
            try:
                p.unlink()
            except Exception as e:
                logger.error("compact: failed to unlink part %s: %r", p, e)

        logger.info(
            "compacted %d parts → %s (rows=%d)",
            len(parts), canonical, merged.num_rows,
        )
        return canonical

    # --- recovery ---

    def recover_orphan_parts(
        self,
        *,
        stream_rel_dir: Path,
        stream_type: StreamType,
        source_id: str,
        exchange: str,
        symbol: str,
    ) -> None:
        """On startup, compact any leftover parts from a prior run."""
        d = self._stream_dir(stream_rel_dir)
        parts_dir = d / PARTS_SUBDIR
        if not parts_dir.exists():
            return
        # Clean stale .tmp from interrupted writes.
        for tmp in parts_dir.glob("*.tmp"):
            try:
                tmp.unlink()
            except Exception:
                pass
        # Group by hour_key.
        groups: dict[str, list[Path]] = defaultdict(list)
        for p in parts_dir.glob("*.parquet"):
            stem = p.stem
            parts = stem.rsplit("_", 1)
            if len(parts) == 2:
                groups[parts[0]].append(p)
        for hour_key in sorted(groups):
            try:
                self.compact_hour(
                    stream_rel_dir=stream_rel_dir,
                    hour_key=hour_key,
                    stream_type=stream_type,
                    source_id=source_id,
                    exchange=exchange,
                    symbol=symbol,
                )
            except Exception as e:
                logger.error("recovery compact %s/%s failed: %r", stream_rel_dir, hour_key, e)


def read_file_metadata(path: Path) -> dict[str, str]:
    """Convenience: read the v2 metadata dict from a parquet file."""
    schema = pq.read_schema(path)
    raw: dict[bytes, bytes] = schema.metadata or {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        try:
            ks = k.decode()
            vs = v.decode()
        except UnicodeDecodeError:
            continue
        out[ks] = vs
    return out


def parse_metadata(meta_bytes: dict[bytes, bytes] | None) -> dict[str, str]:
    """Helper for when you already hold ``table.schema.metadata``."""
    out: dict[str, str] = {}
    for k, v in (meta_bytes or {}).items():
        try:
            out[k.decode()] = v.decode()
        except UnicodeDecodeError:
            continue
    return out


# Public marker for the metadata keys every v2 parquet file must carry.
REQUIRED_METADATA_KEYS: tuple[str, ...] = (
    "recorder_version",
    "schema_version",
    "schema_id",
    "source_id",
    "source_api_version",
    "exchange",
    "symbol",
    "stream_type",
    "hour_key",
    "host_id",
    "recorded_at",
    "producer",
)


def validate_metadata(meta: dict[str, str]) -> list[str]:
    """Return a list of missing required keys. Empty list ⇒ valid."""
    return [k for k in REQUIRED_METADATA_KEYS if k not in meta]
