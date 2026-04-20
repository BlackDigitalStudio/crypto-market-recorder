"""P2: raw archive round-trip."""
from __future__ import annotations

import gzip
from pathlib import Path

import orjson
import pytest

from chronos.raw_archive import RawArchiveWriter


def _read_all_lines(path: Path) -> list[dict]:
    with gzip.open(path, "rb") as f:
        return [orjson.loads(line) for line in f if line.strip()]


def test_append_round_trip(tmp_path: Path):
    w = RawArchiveWriter(tmp_path)
    rel = Path("binance_futures/BTCUSDT/trade")
    hour = "20260419_10"
    for i in range(5):
        w.append(
            stream_rel_dir=rel,
            hour_key=hour,
            local_ts_us=1_700_000_000_000_000 + i,
            exchange_event_ts_us=1_700_000_000_000_000 + i,
            exchange_trans_ts_us=1_700_000_000_000_000 + i,
            source_id="binance_futures_ws",
            channel="aggTrade",
            msg={"i": i, "p": "50000", "q": "0.1"},
        )
    w.close()

    path = tmp_path / rel / "raw" / f"{hour}.jsonl.gz"
    assert path.exists(), "archive file not created"
    lines = _read_all_lines(path)
    assert len(lines) == 5
    for i, row in enumerate(lines):
        assert row["source_id"] == "binance_futures_ws"
        assert row["channel"] == "aggTrade"
        assert row["msg"]["i"] == i
        assert row["local_ts_us"] == 1_700_000_000_000_000 + i


def test_hour_rollover_splits_files(tmp_path: Path):
    w = RawArchiveWriter(tmp_path)
    rel = Path("binance_futures/BTCUSDT/trade")

    for hour, n in [("20260419_10", 3), ("20260419_11", 2)]:
        for i in range(n):
            w.append(
                stream_rel_dir=rel, hour_key=hour,
                local_ts_us=i, exchange_event_ts_us=None, exchange_trans_ts_us=None,
                source_id="x", channel="y", msg={"i": i},
            )
    w.close()

    p10 = tmp_path / rel / "raw" / "20260419_10.jsonl.gz"
    p11 = tmp_path / rel / "raw" / "20260419_11.jsonl.gz"
    assert len(_read_all_lines(p10)) == 3
    assert len(_read_all_lines(p11)) == 2


def test_concatenated_gzip_members_are_readable(tmp_path: Path):
    """Simulate crash/restart: two archive sessions into the same hour.

    gzip concatenation must still decompress as a single continuous stream.
    """
    rel = Path("ex/SYM/trade")
    hour = "20260419_12"

    w1 = RawArchiveWriter(tmp_path)
    for i in range(2):
        w1.append(
            stream_rel_dir=rel, hour_key=hour,
            local_ts_us=i, exchange_event_ts_us=None, exchange_trans_ts_us=None,
            source_id="x", channel="y", msg={"i": i, "session": 1},
        )
    w1.close()

    w2 = RawArchiveWriter(tmp_path)
    for i in range(3):
        w2.append(
            stream_rel_dir=rel, hour_key=hour,
            local_ts_us=100 + i, exchange_event_ts_us=None, exchange_trans_ts_us=None,
            source_id="x", channel="y", msg={"i": i, "session": 2},
        )
    w2.close()

    path = tmp_path / rel / "raw" / f"{hour}.jsonl.gz"
    lines = _read_all_lines(path)
    assert len(lines) == 5
    assert [r["msg"]["session"] for r in lines] == [1, 1, 2, 2, 2]


def test_flush_persists_before_close(tmp_path: Path):
    w = RawArchiveWriter(tmp_path)
    rel = Path("ex/SYM/trade")
    hour = "20260419_13"
    w.append(
        stream_rel_dir=rel, hour_key=hour,
        local_ts_us=1, exchange_event_ts_us=None, exchange_trans_ts_us=None,
        source_id="x", channel="y", msg={"a": 1},
    )
    w.flush()
    # Peek at raw file: gzip with Z_SYNC_FLUSH is not a complete member yet,
    # but must contain the compressed payload of the line.
    path = tmp_path / rel / "raw" / f"{hour}.jsonl.gz"
    assert path.exists() and path.stat().st_size > 0
    w.close()
    assert len(_read_all_lines(path)) == 1
