"""P15: Coinbase + Deribit timestamp extractors and normalizers."""
from __future__ import annotations

import pyarrow as pa

from chronos import normalize as norm
from chronos.schemas import schema_for
from chronos.timestamps import Timestamps, extract_timestamps
from chronos.version import StreamType


def _ts() -> Timestamps:
    return Timestamps(local_ts_us=1, exchange_event_ts_us=2, exchange_trans_ts_us=3)


def _roundtrip(rows: list[dict], schema: pa.Schema) -> pa.Table:
    import pandas as pd
    df = pd.DataFrame(rows)
    for col in [f.name for f in schema]:
        if col not in df.columns:
            df[col] = None
    df = df[[f.name for f in schema]]
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)


def test_coinbase_extract_timestamps_parses_iso8601():
    msg = {
        "channel": "market_trades",
        "timestamp": "2026-04-19T10:15:30.500Z",
        "events": [{"type": "update", "trades": [
            {"product_id": "BTC-USD", "trade_id": "x", "price": "50000",
             "size": "0.1", "side": "BUY", "time": "2026-04-19T10:15:30.490Z"},
        ]}],
    }
    ts = extract_timestamps(msg, source_id="coinbase_advanced_trade_ws", channel="market_trades")
    assert ts.exchange_event_ts_us is not None
    assert ts.exchange_trans_ts_us is not None
    # Trans should be strictly before event.
    assert ts.exchange_trans_ts_us < ts.exchange_event_ts_us


def test_coinbase_normalize_trade_row():
    msg = {
        "channel": "market_trades",
        "timestamp": "2026-04-19T10:15:30.500Z",
        "events": [{"type": "update", "trades": [
            {"product_id": "BTC-USD", "trade_id": "abc", "price": "50000.5",
             "size": "0.25", "side": "SELL", "time": "2026-04-19T10:15:30.490Z"},
            {"product_id": "BTC-USD", "trade_id": "def", "price": "50001",
             "size": "0.1", "side": "BUY", "time": "2026-04-19T10:15:30.491Z"},
        ]}],
    }
    rows = norm.normalize_coinbase_market_trades(
        msg, ts=_ts(), source_id="coinbase_advanced_trade_ws", symbol="BTC-USD",
    )
    assert len(rows) == 2
    # SELL aggressor → buyer is maker.
    assert rows[0]["is_buyer_maker"] is True
    assert rows[1]["is_buyer_maker"] is False
    assert rows[0]["trade_id"] == "abc"
    assert rows[0]["price"] == 50000.5
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_deribit_extract_timestamps_ms_to_us():
    msg = {
        "params": {
            "channel": "trades.BTC-PERPETUAL.raw",
            "data": [{"trade_id": "x", "instrument_name": "BTC-PERPETUAL",
                       "price": 50000.0, "amount": 100, "direction": "buy",
                       "timestamp": 1_700_000_000_123}],
        },
    }
    ts = extract_timestamps(msg, source_id="deribit_v2_public_ws", channel="trades")
    assert ts.exchange_trans_ts_us == 1_700_000_000_123_000


def test_deribit_normalize_trade_row():
    msg = {
        "params": {
            "channel": "trades.BTC-PERPETUAL.raw",
            "data": [
                {"trade_id": "s1", "instrument_name": "BTC-PERPETUAL",
                 "price": 50000.0, "amount": 100, "direction": "sell",
                 "timestamp": 1_700_000_000_000},
                {"trade_id": "b1", "instrument_name": "BTC-PERPETUAL",
                 "price": 50001.0, "amount": 50, "direction": "buy",
                 "timestamp": 1_700_000_000_001},
            ],
        },
    }
    rows = norm.normalize_deribit_trades(
        msg, ts=_ts(), source_id="deribit_v2_public_ws", symbol="BTC-PERPETUAL",
    )
    assert len(rows) == 2
    assert rows[0]["is_buyer_maker"] is True   # sell aggressor
    assert rows[1]["is_buyer_maker"] is False
    assert rows[0]["trade_id"] == "s1"
    assert rows[0]["qty"] == 100.0
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_unknown_coinbase_channel_returns_empty():
    ts = extract_timestamps({"foo": "bar"}, source_id="coinbase_advanced_trade_ws", channel="unknown")
    # Unknown (source, channel) falls back to local-only.
    assert ts.exchange_event_ts_us is None
