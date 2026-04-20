"""End-to-end: P3 integrity + P14 funding settlement wired through Recorder."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow.parquet as pq

from chronos import (
    Recorder,
    StreamKey,
    StreamType,
    funding_settlement_key_for,
    integrity_key_for,
)
from chronos.parquet_writer import parse_metadata


def _run(coro):
    return asyncio.run(coro)


def test_depth_gap_produces_integrity_row(tmp_path: Path):
    key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        rec.register(key)
        await rec.start()
        # Contiguous pair
        rec.record(key, {"e": "depthUpdate", "E": 1, "T": 1, "U": 100, "u": 105, "pu": 99, "b": [], "a": []})
        rec.record(key, {"e": "depthUpdate", "E": 2, "T": 2, "U": 106, "u": 110, "pu": 105, "b": [], "a": []})
        # Gap: pu mismatch
        rec.record(key, {"e": "depthUpdate", "E": 3, "T": 3, "U": 200, "u": 210, "pu": 190, "b": [], "a": []})
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    ikey = integrity_key_for(key)
    files = list((tmp_path / ikey.rel_dir).glob("*.parquet"))
    assert files, "integrity parquet missing"
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    rows = table.to_pylist()
    assert rows[0]["gap_kind"] == "depth_sequence_pu_mismatch"
    assert rows[0]["monitored_stream_type"] == "depth_diff"
    assert rows[0]["monitored_channel"] == "depthUpdate"
    assert rows[0]["previous_value"] == 110
    assert rows[0]["current_value"] == 190
    meta = parse_metadata(table.schema.metadata)
    assert meta["stream_type"] == "integrity_gap"


def test_aggtrade_regress_produces_integrity_row(tmp_path: Path):
    key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.TRADE, channel="aggTrade",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        rec.register(key)
        await rec.start()
        rec.record(key, {"a": 100, "p": "1", "q": "1", "m": False, "E": 1, "T": 1, "f": 1, "l": 1})
        rec.record(key, {"a": 50, "p": "1", "q": "1", "m": False, "E": 2, "T": 2, "f": 1, "l": 1})
        rec.record(key, {"a": 105, "p": "1", "q": "1", "m": False, "E": 3, "T": 3, "f": 1, "l": 1})
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    ikey = integrity_key_for(key)
    files = list((tmp_path / ikey.rel_dir).glob("*.parquet"))
    assert files
    table = pq.read_table(files[0])
    kinds = [r["gap_kind"] for r in table.to_pylist()]
    # One regress (50 after 100) + one skip (105 after 100, skip=4).
    assert "aggtrade_id_regress" in kinds
    assert "aggtrade_id_skip" in kinds


def test_no_validators_means_no_integrity_stream(tmp_path: Path):
    key = StreamKey(
        source_id="okx_v5_public", exchange="okx",
        symbol="BTC-USDT-SWAP", stream_type=StreamType.TRADE, channel="trades",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        rec.register(key)
        await rec.start()
        rec.record(key, {"data": [{"ts": "1", "px": "1", "sz": "1", "side": "buy", "tradeId": "x"}]})
        await asyncio.sleep(0.2)
        await rec.stop()

    _run(_body())

    ikey = integrity_key_for(key)
    assert not (tmp_path / ikey.rel_dir).exists(), (
        "integrity stream should not be auto-registered when no validator fires"
    )


def test_funding_settlement_emitted_on_T_advance(tmp_path: Path):
    key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.MARK_PRICE, channel="markPriceUpdate",
    )
    T0 = 1_700_000_900_000
    T1 = T0 + 8 * 3600 * 1000  # 8 hours later

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        rec.register(key)
        await rec.start()
        # Two samples in the same funding interval — rate tightens.
        rec.record(key, {"e": "markPriceUpdate", "E": 1, "p": "50000", "r": "0.0001", "T": T0})
        rec.record(key, {"e": "markPriceUpdate", "E": 2, "p": "50000", "r": "0.00015", "T": T0})
        # Settlement — T advances.
        rec.record(key, {"e": "markPriceUpdate", "E": 3, "p": "50000", "r": "0.0002", "T": T1})
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    skey = funding_settlement_key_for(key)
    files = list((tmp_path / skey.rel_dir).glob("*.parquet"))
    assert files, "funding_settlement parquet missing"
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["realized_funding_rate"] == 0.00015
    assert row["settled_at_ms"] == T0
    meta = parse_metadata(table.schema.metadata)
    assert meta["stream_type"] == "funding_settlement"


def test_integrity_stream_shares_hour_metadata(tmp_path: Path):
    """Integrity rows carry the same hour_key metadata layout as data streams."""
    key = StreamKey(
        source_id="binance_futures_ws", exchange="binance_futures",
        symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        rec.register(key)
        await rec.start()
        rec.record(key, {"U": 1, "u": 2, "pu": 0, "b": [], "a": []})
        rec.record(key, {"U": 100, "u": 110, "pu": 80, "b": [], "a": []})
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    ikey = integrity_key_for(key)
    files = list((tmp_path / ikey.rel_dir).glob("*.parquet"))
    assert files
    meta = parse_metadata(pq.read_table(files[0]).schema.metadata)
    assert meta["hour_key"]
    assert meta["exchange"] == "binance_futures"
    assert meta["symbol"] == "BTCUSDT"
