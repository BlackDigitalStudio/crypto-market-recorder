"""P16: reader helpers over the v2 archive."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow as pa

from chronos import Recorder, StreamKey, StreamType
from chronos.reader import (
    discover_streams,
    read_stream,
    summarize_metadata,
    unified_trades,
)


def _run(coro):
    return asyncio.run(coro)


def _populate(tmp_path: Path) -> None:
    keys = [
        StreamKey("binance_futures_ws", "binance_futures", "BTCUSDT", StreamType.TRADE, "aggTrade"),
        StreamKey("bybit_v5_public_linear", "bybit", "BTCUSDT", StreamType.TRADE, "publicTrade"),
    ]

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.1)
        for k in keys:
            rec.register(k)
        await rec.start()
        rec.record(keys[0], {"a": 1, "p": "50000", "q": "0.1", "m": False,
                             "E": 1, "T": 1, "f": 1, "l": 1})
        rec.record(keys[1], {"ts": 2, "data": [{"T": 2, "p": "50000", "v": "0.1",
                                                "S": "Buy", "i": "b1"}]})
        await asyncio.sleep(0.2)
        await rec.stop()

    _run(_body())


def test_discover_streams_lists_every_venue(tmp_path: Path):
    _populate(tmp_path)
    locs = discover_streams(tmp_path)
    triples = {(l.exchange, l.symbol, l.stream_type) for l in locs}
    assert ("binance_futures", "BTCUSDT", StreamType.TRADE) in triples
    assert ("bybit", "BTCUSDT", StreamType.TRADE) in triples


def test_read_stream_returns_nonempty_arrow_table(tmp_path: Path):
    _populate(tmp_path)
    loc = next(l for l in discover_streams(tmp_path) if l.exchange == "bybit")
    t = read_stream(loc)
    assert isinstance(t, pa.Table)
    assert t.num_rows >= 1


def test_unified_trades_merges_venues(tmp_path: Path):
    _populate(tmp_path)
    t = unified_trades(tmp_path, symbol_filter="BTCUSDT")
    assert t.num_rows == 2
    exchanges = set(t.column("exchange").to_pylist())
    assert exchanges == {"binance_futures", "bybit"}


def test_unified_trades_exchange_filter(tmp_path: Path):
    _populate(tmp_path)
    t = unified_trades(tmp_path, exchanges={"bybit"})
    assert t.num_rows == 1
    assert all(e == "bybit" for e in t.column("exchange").to_pylist())


def test_summarize_metadata_produces_one_row_per_stream(tmp_path: Path):
    _populate(tmp_path)
    summary = summarize_metadata(tmp_path)
    # Binance aggTrade auto-registers a sibling integrity_gap stream
    # (dir created even when empty) — discovery returns 3.
    by_type = {(r["exchange"], r["stream_type"]) for r in summary}
    assert ("binance_futures", "trade") in by_type
    assert ("bybit", "trade") in by_type
    trade_rows = [r for r in summary if r["stream_type"] == "trade"]
    for row in trade_rows:
        assert row["schema_id"].startswith("trade:")
        assert row["file_count"] >= 1


def test_empty_root_returns_empty(tmp_path: Path):
    assert discover_streams(tmp_path) == []
    assert unified_trades(tmp_path).num_rows == 0
