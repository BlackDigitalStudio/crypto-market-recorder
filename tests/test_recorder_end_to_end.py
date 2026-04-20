"""End-to-end: Recorder produces parquet + raw archive with v2 metadata."""
from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

import orjson
import pyarrow.parquet as pq

from chronos import (
    Recorder,
    StreamKey,
    StreamType,
)
from chronos.parquet_writer import (
    REQUIRED_METADATA_KEYS,
    parse_metadata,
)


def _run(coro):
    return asyncio.run(coro)


def test_record_aggtrade_writes_parquet_and_raw(tmp_path: Path):
    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        key = StreamKey(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", stream_type=StreamType.TRADE, channel="aggTrade",
        )
        rec.register(key)
        await rec.start()
        for i in range(10):
            rec.record(key, {
                "e": "aggTrade", "E": 1_700_000_000_000 + i, "T": 1_700_000_000_000 + i,
                "s": "BTCUSDT", "a": i, "p": "50000", "q": "0.1",
                "f": i, "l": i, "m": bool(i % 2),
            })
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    rel = Path("binance_futures/BTCUSDT/trade")
    parquet_files = list((tmp_path / rel).glob("*.parquet"))
    assert parquet_files, "no parquet file produced"
    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 10
    meta = parse_metadata(table.schema.metadata)
    for k in REQUIRED_METADATA_KEYS:
        assert k in meta, f"missing metadata key: {k}"
    assert meta["stream_type"] == "trade"
    assert meta["exchange"] == "binance_futures"

    raw_files = list((tmp_path / rel / "raw").glob("*.jsonl.gz"))
    assert raw_files, "no raw archive produced"
    with gzip.open(raw_files[0], "rb") as f:
        raw_lines = [orjson.loads(line) for line in f if line.strip()]
    assert len(raw_lines) == 10
    assert raw_lines[0]["source_id"] == "binance_futures_ws"
    assert raw_lines[0]["channel"] == "aggTrade"
    assert raw_lines[0]["msg"]["a"] == 0


def test_record_depth_update_produces_per_level_rows(tmp_path: Path):
    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        key = StreamKey(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", stream_type=StreamType.DEPTH_DIFF, channel="depthUpdate",
        )
        rec.register(key)
        await rec.start()
        rec.record(key, {
            "e": "depthUpdate", "E": 1, "T": 1, "s": "BTCUSDT",
            "U": 1, "u": 3, "pu": 0,
            "b": [["50000", "1.0"], ["49999", "0"]],
            "a": [["50001", "2.0"]],
        })
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    rel = Path("binance_futures/BTCUSDT/depth_diff")
    parquet_files = list((tmp_path / rel).glob("*.parquet"))
    assert parquet_files
    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 3
    meta = parse_metadata(table.schema.metadata)
    assert meta["stream_type"] == "depth_diff"


def test_multi_venue_trade_streams_coexist(tmp_path: Path):
    keys = [
        StreamKey("binance_futures_ws", "binance_futures", "BTCUSDT", StreamType.TRADE, "aggTrade"),
        StreamKey("bybit_v5_public_linear", "bybit", "BTCUSDT", StreamType.TRADE, "publicTrade"),
        StreamKey("okx_v5_public", "okx", "BTC-USDT-SWAP", StreamType.TRADE, "trades"),
    ]

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        for k in keys:
            rec.register(k)
        await rec.start()
        rec.record(keys[0], {"a": 1, "p": "50000", "q": "0.1", "m": False, "E": 1, "T": 1, "f": 1, "l": 1})
        rec.record(keys[1], {"ts": 2, "data": [{"T": 2, "p": "50000", "v": "0.1", "S": "Buy", "i": "b1"}]})
        rec.record(keys[2], {"data": [{"ts": "3", "px": "50000", "sz": "0.1", "side": "sell", "tradeId": "o1"}]})
        await asyncio.sleep(0.3)
        await rec.stop()

    _run(_body())

    for k in keys:
        pq_files = list((tmp_path / k.rel_dir).glob("*.parquet"))
        raw_files = list((tmp_path / k.rel_dir / "raw").glob("*.jsonl.gz"))
        assert pq_files, f"no parquet for {k.exchange}"
        assert raw_files, f"no raw for {k.exchange}"
        meta = parse_metadata(pq.read_table(pq_files[0]).schema.metadata)
        assert meta["source_id"] == k.source_id
        assert meta["exchange"] == k.exchange
