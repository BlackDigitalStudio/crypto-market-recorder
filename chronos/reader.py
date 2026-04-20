"""P16 — consumer-side helpers over the v2 archive.

Archive layout (produced by :class:`Recorder`)::

    <root>/<exchange>/<symbol>/<stream_type>/
        <hour_key>.parquet
        raw/<hour_key>.jsonl.gz
        .parts/<hour_key>_NNNNN.parquet

Every parquet carries schema-level metadata (see
:data:`parquet_writer.REQUIRED_METADATA_KEYS`). These helpers scan the
tree and join files while checking the metadata contract so a caller
can:

- List every recorded stream without knowing which venues were running.
- Pull a unified trade or depth table across venues (P16 cross-venue
  normalized layer — schema is already uniform thanks to the preamble).
- Verify that files on disk match a schema version expectation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_writer import REQUIRED_METADATA_KEYS, parse_metadata, read_file_metadata
from .version import StreamType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamLocation:
    """One concrete stream directory discovered on disk."""

    root: Path
    exchange: str
    symbol: str
    stream_type: StreamType
    dir: Path

    @property
    def parquet_files(self) -> list[Path]:
        # Only canonical (compacted) hourly parquet; parts are transient.
        files = [
            p for p in self.dir.iterdir()
            if p.is_file() and p.suffix == ".parquet" and not p.stem.startswith(".")
        ]
        return sorted(files)


def discover_streams(root: Path) -> list[StreamLocation]:
    """Walk ``root`` and return every recorded ``(exchange, symbol,
    stream_type)`` triple."""
    root = Path(root)
    if not root.exists():
        return []
    out: list[StreamLocation] = []
    known = {st.value for st in StreamType}
    for exchange_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for symbol_dir in sorted(p for p in exchange_dir.iterdir() if p.is_dir()):
            for stream_dir in sorted(p for p in symbol_dir.iterdir() if p.is_dir()):
                if stream_dir.name not in known:
                    continue
                try:
                    st = StreamType(stream_dir.name)
                except ValueError:
                    continue
                out.append(StreamLocation(
                    root=root, exchange=exchange_dir.name,
                    symbol=symbol_dir.name, stream_type=st, dir=stream_dir,
                ))
    return out


def _hour_from_stem(stem: str) -> datetime | None:
    # YYYYMMDD_HH
    try:
        return datetime.strptime(stem, "%Y%m%d_%H")
    except ValueError:
        return None


def _files_in_range(
    loc: StreamLocation,
    start: datetime | None,
    end: datetime | None,
) -> list[Path]:
    out: list[Path] = []
    for p in loc.parquet_files:
        h = _hour_from_stem(p.stem)
        if h is None:
            continue
        if start is not None and h < start:
            continue
        if end is not None and h > end:
            continue
        out.append(p)
    return out


def read_stream(
    loc: StreamLocation,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    validate_metadata: bool = True,
) -> pa.Table:
    """Read parquet files for one stream location within an hour window.

    ``start`` / ``end`` are inclusive hour boundaries; ``None`` means
    unbounded on that side. When ``validate_metadata`` is true, files
    missing any required v2 metadata key are skipped with a warning.
    """
    files = _files_in_range(loc, start, end)
    tables: list[pa.Table] = []
    for f in files:
        try:
            if validate_metadata:
                meta = read_file_metadata(f)
                missing = [k for k in REQUIRED_METADATA_KEYS if k not in meta]
                if missing:
                    logger.warning("skipping %s — missing metadata %s", f, missing)
                    continue
            tables.append(pq.read_table(f))
        except Exception as e:
            logger.error("read %s failed: %r", f, e)
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def unified_trades(
    root: Path,
    *,
    symbol_filter: str | None = None,
    exchanges: Iterable[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pa.Table:
    """Concatenate every :attr:`StreamType.TRADE` parquet across venues.

    Output columns match :data:`schemas.TRADE_SCHEMA` — the preamble
    provides ``source_id``, ``exchange_event_ts_us``,
    ``exchange_trans_ts_us``, ``local_ts_us`` and ``symbol`` so a
    downstream consumer can filter by venue without opening file
    metadata.

    A venue-aware ``exchange`` column is injected from directory layout
    (parquet metadata has it, but surfacing it as a row column is more
    convenient for pandas/duckdb merges).
    """
    wanted_exchanges = set(exchanges) if exchanges else None
    tables: list[pa.Table] = []
    for loc in discover_streams(root):
        if loc.stream_type is not StreamType.TRADE:
            continue
        if wanted_exchanges and loc.exchange not in wanted_exchanges:
            continue
        if symbol_filter and loc.symbol != symbol_filter:
            continue
        t = read_stream(loc, start=start, end=end)
        if t.num_rows == 0:
            continue
        # Prepend an exchange column (O(1) vs storing meta lookups).
        ex_arr = pa.array([loc.exchange] * t.num_rows, type=pa.string())
        t = t.append_column("exchange", ex_arr)
        tables.append(t)
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def iter_raw_jsonl(
    loc: StreamLocation,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[dict]:
    """Stream raw ``.jsonl.gz`` lines for a stream. Useful for debugging
    or re-normalizing after a parsing bug fix."""
    import gzip

    import orjson as _oj

    raw_dir = loc.dir / "raw"
    if not raw_dir.exists():
        return
    for p in sorted(raw_dir.glob("*.jsonl.gz")):
        h = _hour_from_stem(p.stem.split(".")[0])
        if h is None:
            continue
        if start is not None and h < start:
            continue
        if end is not None and h > end:
            continue
        try:
            with gzip.open(p, "rb") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    try:
                        yield _oj.loads(line)
                    except Exception:
                        continue
        except Exception as e:
            logger.error("read raw %s failed: %r", p, e)


def summarize_metadata(root: Path) -> list[dict]:
    """Return a list of ``{exchange, symbol, stream_type, schema_id,
    source_id, file_count, first_hour, last_hour}`` across the archive.

    Consumers use this as the v2 equivalent of `ls -R` — one row per
    discovered stream with enough context to dispatch per-schema readers.
    """
    out: list[dict] = []
    for loc in discover_streams(root):
        files = loc.parquet_files
        first = last = None
        schema_id = source_id = "?"
        if files:
            first = files[0].stem
            last = files[-1].stem
            try:
                meta = read_file_metadata(files[-1])
                schema_id = meta.get("schema_id", "?")
                source_id = meta.get("source_id", "?")
            except Exception:
                pass
        out.append({
            "exchange": loc.exchange,
            "symbol": loc.symbol,
            "stream_type": loc.stream_type.value,
            "schema_id": schema_id,
            "source_id": source_id,
            "file_count": len(files),
            "first_hour": first,
            "last_hour": last,
        })
    return out
