"""P2 — raw WS JSON archive (`.jsonl.gz`), partitioned hourly.

Every raw WebSocket frame is recorded verbatim next to the parquet file
derived from it. If a bug is discovered in normalization or schema later,
the raw archive lets us re-parse without asking the exchange to replay
(which generally they cannot).

Layout::

    data_v2/<exchange>/<symbol>/<stream_type>/raw/<hour_key>.jsonl.gz

One line per frame. Line format (JSON object)::

    {
      "local_ts_us":          1713456789123456,
      "exchange_event_ts_us": 1713456789123000 | null,
      "exchange_trans_ts_us": 1713456789122000 | null,
      "source_id":            "binance_futures_ws",
      "channel":              "depthUpdate",
      "msg":                  { ... original frame verbatim ... }
    }

Crash-safety:
- gzip members concatenate — we keep the file open in append mode for the
  current hour and flush on every drain with ``Z_SYNC_FLUSH`` so a crash
  loses at most one flush interval of compressed-but-unflushed bytes.
- On hour rollover (or stop), the current member is closed with a proper
  gzip trailer; the next hour opens a new file.
- If a prior crash left a half-written gzip tail, standard ``gzip.open``
  for read still recovers every complete member up to the truncation
  point — consumers just skip the last partial member.
"""
from __future__ import annotations

import gzip
import logging
import threading
import time
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any

import orjson

logger = logging.getLogger(__name__)


# Tradeoff for gzip level 6: ~3× vs level 1, ~2% worse than level 9 with a
# fraction of the CPU. Standard "append-only-log" default.
_GZIP_LEVEL = 6


def _jsonl_bytes(payload: dict[str, Any]) -> bytes:
    return orjson.dumps(payload) + b"\n"


class _HourFile:
    """One open gzip writer for a single (stream_key, hour_key) pair."""

    __slots__ = ("path", "hour_key", "_fp", "_raw_fp")

    def __init__(self, path: Path, hour_key: str) -> None:
        self.path = path
        self.hour_key = hour_key
        # Open underlying binary file in append mode; wrap with a GzipFile so
        # each session writes a new gzip member concatenated onto the file.
        path.parent.mkdir(parents=True, exist_ok=True)
        self._raw_fp = open(path, "ab", buffering=0)
        self._fp = gzip.GzipFile(
            fileobj=self._raw_fp,
            mode="ab",
            compresslevel=_GZIP_LEVEL,
            filename="",
            mtime=int(time.time()),
        )

    def write(self, line: bytes) -> None:
        self._fp.write(line)

    def flush(self) -> None:
        # Z_SYNC_FLUSH emits all buffered compressed bytes and aligns the
        # stream so a reader can recover up to this point after a crash.
        self._fp.flush(zlib.Z_SYNC_FLUSH)
        try:
            self._raw_fp.flush()
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._fp.close()  # writes gzip trailer
        finally:
            try:
                self._raw_fp.close()
            except Exception:
                pass


class RawArchiveWriter:
    """Append-only, hourly-partitioned, gzip-JSONL writer.

    Thread-safe. Designed for a single process but multiple producer
    coroutines / threads writing to many stream keys concurrently.

    Parameters
    ----------
    root:
        Base directory. A per-stream path is built under
        ``<root>/<stream_rel_dir>/raw/<hour_key>.jsonl.gz``.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._files: dict[tuple[Path, str], _HourFile] = {}
        # Per-stream lock keeps the order of bytes inside one gzip member
        # deterministic under concurrent writers. A global lock would
        # serialize all archives; per-stream is fine since writers touch
        # independent files.
        self._locks: dict[Path, threading.Lock] = defaultdict(threading.Lock)
        self._global_lock = threading.Lock()

    # --- path helpers ---

    def _dir_for(self, stream_rel_dir: Path) -> Path:
        return self._root / stream_rel_dir / "raw"

    def _path_for(self, stream_rel_dir: Path, hour_key: str) -> Path:
        return self._dir_for(stream_rel_dir) / f"{hour_key}.jsonl.gz"

    # --- public API ---

    def append(
        self,
        *,
        stream_rel_dir: Path,
        hour_key: str,
        local_ts_us: int,
        exchange_event_ts_us: int | None,
        exchange_trans_ts_us: int | None,
        source_id: str,
        channel: str,
        msg: dict[str, Any],
    ) -> None:
        """Append one raw frame to the archive.

        ``msg`` is serialized verbatim. ``local_ts_us`` etc. become line-level
        keys so a consumer can filter / index without parsing ``msg``.
        """
        line = _jsonl_bytes({
            "local_ts_us": local_ts_us,
            "exchange_event_ts_us": exchange_event_ts_us,
            "exchange_trans_ts_us": exchange_trans_ts_us,
            "source_id": source_id,
            "channel": channel,
            "msg": msg,
        })
        key = (stream_rel_dir, hour_key)
        path = self._path_for(stream_rel_dir, hour_key)

        # Fast path: file already open.
        hour_file = self._files.get(key)
        if hour_file is None:
            with self._global_lock:
                hour_file = self._files.get(key)
                if hour_file is None:
                    self._rollover_closed_hours_locked(stream_rel_dir, hour_key)
                    hour_file = _HourFile(path, hour_key)
                    self._files[key] = hour_file

        lock = self._lock_for(path)
        with lock:
            hour_file.write(line)

    def flush(self) -> None:
        """Sync all open gzip members to disk."""
        with self._global_lock:
            items = list(self._files.items())
        for (path_key, _hk), f in items:
            with self._lock_for(f.path):
                try:
                    f.flush()
                except Exception as e:
                    logger.error("raw archive flush failed for %s: %r", f.path, e)

    def close_stream(self, stream_rel_dir: Path) -> None:
        """Close any open hour for ``stream_rel_dir`` (used on stop)."""
        with self._global_lock:
            to_close = [
                (k, f) for k, f in self._files.items() if k[0] == stream_rel_dir
            ]
            for k, f in to_close:
                self._files.pop(k, None)
        for _, f in to_close:
            with self._lock_for(f.path):
                try:
                    f.close()
                except Exception as e:
                    logger.error("raw archive close failed for %s: %r", f.path, e)

    def close(self) -> None:
        """Close every open hour."""
        with self._global_lock:
            items = list(self._files.items())
            self._files.clear()
        for (_key, _hk), f in items:
            with self._lock_for(f.path):
                try:
                    f.close()
                except Exception as e:
                    logger.error("raw archive close failed for %s: %r", f.path, e)

    # --- internals ---

    def _lock_for(self, path: Path) -> threading.Lock:
        # defaultdict is already thread-safe for insert-or-get on CPython but
        # we guard explicitly to stay portable.
        with self._global_lock:
            return self._locks[path]

    def _rollover_closed_hours_locked(self, stream_rel_dir: Path, new_hour_key: str) -> None:
        """When a stream writes into a *new* hour, close the prior hour."""
        stale = [
            k for k in self._files
            if k[0] == stream_rel_dir and k[1] != new_hour_key
        ]
        for k in stale:
            f = self._files.pop(k)
            try:
                f.close()
            except Exception as e:
                logger.error("raw archive hour rollover close failed: %r", e)
