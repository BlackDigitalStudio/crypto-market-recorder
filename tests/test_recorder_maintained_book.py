"""End-to-end: maintained book produces depth_snapshot, classified
depth_diff, book_checkpoint, and REST reconcile drift integrity rows."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow.parquet as pq

from chronos import (
    Recorder,
    StreamKey,
    StreamType,
    book_checkpoint_key_for,
    depth_snapshot_key_for,
    integrity_key_for,
)
from chronos.parquet_writer import parse_metadata


def _run(coro):
    return asyncio.run(coro)


def _rest_snapshot(last_id: int = 100) -> dict:
    return {
        "lastUpdateId": last_id,
        "bids": [["50000", "1.0"], ["49999", "2.0"], ["49998", "3.0"]],
        "asks": [["50001", "1.5"], ["50002", "2.5"], ["50003", "3.5"]],
    }


def test_depth_snapshot_and_classified_diff_emitted(tmp_path: Path):
    diff_key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )
    snap_key = depth_snapshot_key_for(diff_key, levels=5)

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        book = rec.register_maintained_book(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", snapshot_levels=5,
            checkpoint_interval_sec=3600,  # disable checkpoint within test
        )
        rec.ingest_rest_snapshot(diff_key, _rest_snapshot(100))
        await rec.start()
        # One diff: removes 50000 bid, adds 49997 bid, changes ask 50001.
        rec.record(diff_key, {
            "e": "depthUpdate", "E": 1, "T": 1,
            "U": 101, "u": 105, "pu": 100,
            "b": [["50000", "0"], ["49997", "4.0"]],
            "a": [["50001", "2.0"]],
        })
        await asyncio.sleep(0.3)
        await rec.stop()
        assert book.applied_diffs == 1

    _run(_body())

    # depth_diff: 3 classified rows (one per change).
    diff_files = list((tmp_path / diff_key.rel_dir).glob("*.parquet"))
    assert diff_files
    diff_tbl = pq.read_table(diff_files[0])
    rows = diff_tbl.to_pylist()
    # Initial seed emitted zero rows; the diff emitted 3 (remove + insert + update).
    kinds = {(r["side"], r["price"], r["update_type"]) for r in rows}
    assert ("bid", 50000.0, "remove") in kinds
    assert ("bid", 49997.0, "insert") in kinds
    assert ("ask", 50001.0, "update") in kinds
    diff_meta = parse_metadata(diff_tbl.schema.metadata)
    assert diff_meta["stream_type"] == "depth_diff"

    # depth_snapshot: initial seed + one per applied diff = 2 rows.
    snap_files = list((tmp_path / snap_key.rel_dir).glob("*.parquet"))
    assert snap_files
    snap_tbl = pq.read_table(snap_files[0])
    assert snap_tbl.num_rows >= 2
    snap_meta = parse_metadata(snap_tbl.schema.metadata)
    assert snap_meta["stream_type"] == "depth_snapshot"
    last_row = snap_tbl.to_pylist()[-1]
    # After the diff: best bid should be 49999 (since we removed 50000 and inserted 49997).
    assert last_row["bid_prices"][0] == 49999.0
    # depth_levels column reflects the requested L5.
    assert last_row["depth_levels"] == 5


def test_book_checkpoint_emitted_on_schedule(tmp_path: Path):
    diff_key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )
    cp_key = book_checkpoint_key_for(diff_key)

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.05)
        rec.register_maintained_book(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", snapshot_levels=5,
            checkpoint_interval_sec=0.0,  # always emit every flush tick
        )
        rec.ingest_rest_snapshot(diff_key, _rest_snapshot(100))
        await rec.start()
        # Let the flush loop tick enough to produce ≥1 checkpoint dump.
        await asyncio.sleep(0.25)
        await rec.stop()

    _run(_body())

    cp_files = list((tmp_path / cp_key.rel_dir).glob("*.parquet"))
    assert cp_files, "no book_checkpoint parquet produced"
    tbl = pq.read_table(cp_files[0])
    rows = tbl.to_pylist()
    # 6 levels × ≥1 dump.
    assert len(rows) >= 6
    assert all(r["last_update_id"] == 100 for r in rows)
    dump_ids = {r["dump_id"] for r in rows}
    assert len(dump_ids) >= 1
    meta = parse_metadata(tbl.schema.metadata)
    assert meta["stream_type"] == "book_checkpoint"


def test_reconcile_with_rest_writes_drift_integrity(tmp_path: Path):
    diff_key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )
    ikey = integrity_key_for(diff_key)

    async def _body() -> int:
        rec = Recorder(tmp_path, flush_interval_sec=0.05)
        rec.register_maintained_book(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", snapshot_levels=5, checkpoint_interval_sec=3600,
        )
        rec.ingest_rest_snapshot(diff_key, _rest_snapshot(100))
        await rec.start()
        drifted = _rest_snapshot(100)
        drifted["bids"][0] = ["50000", "9.99"]
        drifted["asks"].append(["50010", "1.0"])
        n = rec.reconcile_with_rest(diff_key, drifted)
        await asyncio.sleep(0.25)
        await rec.stop()
        return n

    drift_count = _run(_body())
    assert drift_count >= 2  # at least qty_mismatch + missing_local

    files = list((tmp_path / ikey.rel_dir).glob("*.parquet"))
    assert files, "integrity_gap parquet missing"
    rows = pq.read_table(files[0]).to_pylist()
    kinds = {r["gap_kind"] for r in rows}
    assert any(k.startswith("rest_depth_drift_") for k in kinds)


def test_book_unsynced_drops_diffs_but_keeps_raw_archive(tmp_path: Path):
    diff_key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.05)
        rec.register_maintained_book(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", snapshot_levels=5, checkpoint_interval_sec=3600,
        )
        # Intentionally NOT calling ingest_rest_snapshot.
        await rec.start()
        rec.record(diff_key, {
            "e": "depthUpdate", "E": 1, "T": 1,
            "U": 1, "u": 2, "pu": 0,
            "b": [["50000", "1.0"]], "a": [["50001", "1.0"]],
        })
        await asyncio.sleep(0.2)
        await rec.stop()

    _run(_body())

    # No depth_diff parquet rows (unsynced book drops them).
    diff_files = list((tmp_path / diff_key.rel_dir).glob("*.parquet"))
    if diff_files:
        tbl = pq.read_table(diff_files[0])
        assert tbl.num_rows == 0
    # But the raw archive must contain the frame.
    raw_files = list((tmp_path / diff_key.rel_dir / "raw").glob("*.jsonl.gz"))
    assert raw_files
