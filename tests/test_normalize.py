"""Normalizer output matches the Arrow schema for each stream type."""
from __future__ import annotations

import pyarrow as pa
import pytest

from chronos import normalize as norm
from chronos.schemas import schema_for
from chronos.timestamps import Timestamps
from chronos.version import StreamType


def _ts() -> Timestamps:
    return Timestamps(local_ts_us=1, exchange_event_ts_us=2, exchange_trans_ts_us=3)


def _roundtrip(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Build an Arrow table from rows under the given schema.

    This is the same path Recorder uses — failure here means the
    normalizer or schema are out of sync.
    """
    import pandas as pd
    df = pd.DataFrame(rows)
    for col in [f.name for f in schema]:
        if col not in df.columns:
            df[col] = None
    df = df[[f.name for f in schema]]
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)


def test_binance_depth_update_emits_one_row_per_level():
    msg = {"U": 1, "u": 10, "pu": 0, "b": [["50000", "1.0"], ["49999", "0"]], "a": [["50001", "2.0"]]}
    rows = norm.normalize_binance_depth_update(msg, ts=_ts(), source_id="binance_futures_ws", symbol="BTCUSDT")
    assert len(rows) == 3
    assert rows[0]["side"] == "bid" and rows[0]["price"] == 50000.0 and rows[0]["update_type"] == "update"
    assert rows[1]["side"] == "bid" and rows[1]["qty"] == 0.0 and rows[1]["update_type"] == "remove"
    assert rows[2]["side"] == "ask" and rows[2]["price"] == 50001.0
    _roundtrip(rows, schema_for(StreamType.DEPTH_DIFF))


def test_binance_agg_trade_row_matches_schema():
    msg = {"a": 42, "p": "50000.5", "q": "0.123", "f": 100, "l": 102, "m": True}
    rows = norm.normalize_binance_agg_trade(msg, ts=_ts(), source_id="binance_futures_ws", symbol="BTCUSDT")
    assert len(rows) == 1
    assert rows[0]["price"] == 50000.5
    assert rows[0]["is_buyer_maker"] is True
    assert rows[0]["first_trade_id"] == 100
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_mark_price_keeps_next_funding_time_but_not_trans():
    msg = {"p": "50000", "i": "49999", "r": "0.0001", "P": "50000.1", "T": 1_700_000_900_000}
    rows = norm.normalize_binance_mark_price(msg, ts=_ts(), source_id="binance_futures_ws", symbol="BTCUSDT")
    assert len(rows) == 1
    assert rows[0]["funding_rate"] == 0.0001
    assert rows[0]["next_funding_time_ms"] == 1_700_000_900_000
    _roundtrip(rows, schema_for(StreamType.MARK_PRICE))


def test_bybit_public_trade_is_buyer_maker_flips_on_sell_aggressor():
    msg = {"ts": 1, "data": [{"T": 1, "p": "50000", "v": "0.1", "S": "Sell", "i": "id1"}, {"T": 1, "p": "50001", "v": "0.2", "S": "Buy", "i": "id2"}]}
    rows = norm.normalize_bybit_public_trade(msg, ts=_ts(), source_id="bybit_v5_public_linear", symbol="BTCUSDT")
    assert rows[0]["is_buyer_maker"] is True     # sell aggressor
    assert rows[1]["is_buyer_maker"] is False    # buy aggressor
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_gateio_sign_encoding_flips_correctly():
    msg = {"result": [{"id": 1, "create_time_ms": 1_700_000_000_000, "price": "50000", "size": -3}, {"id": 2, "create_time_ms": 1_700_000_000_001, "price": "50001", "size": 5}]}
    rows = norm.normalize_gateio_futures_trades(msg, ts=_ts(), source_id="gateio_v4_futures", symbol="BTC_USDT")
    assert rows[0]["is_buyer_maker"] is True and rows[0]["qty"] == 3.0
    assert rows[1]["is_buyer_maker"] is False and rows[1]["qty"] == 5.0
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_okx_and_bitget_trade_round_trip():
    okx_msg = {"data": [{"ts": "1700000000000", "px": "50000", "sz": "0.5", "side": "buy", "tradeId": "x"}]}
    rows = norm.normalize_okx_trades(okx_msg, ts=_ts(), source_id="okx_v5_public", symbol="BTC-USDT-SWAP")
    assert rows[0]["is_buyer_maker"] is False
    _roundtrip(rows, schema_for(StreamType.TRADE))

    bitget_msg = {"ts": 1_700_000_000_000, "data": [{"ts": "1700000000000", "price": "50000", "size": "0.5", "side": "sell", "tradeId": "y"}]}
    rows = norm.normalize_bitget_trade(bitget_msg, ts=_ts(), source_id="bitget_v2_public", symbol="BTCUSDT")
    assert rows[0]["is_buyer_maker"] is True
    _roundtrip(rows, schema_for(StreamType.TRADE))


def test_force_order_extracts_nested_o():
    msg = {"o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "q": "1.0", "p": "50000", "ap": "49999.5", "X": "FILLED", "l": "1.0", "z": "1.0"}}
    rows = norm.normalize_binance_force_order(msg, ts=_ts(), source_id="binance_futures_ws", symbol="BTCUSDT")
    assert rows[0]["side"] == "SELL"
    assert rows[0]["order_type"] == "LIMIT"
    assert rows[0]["price"] == 50000.0
    _roundtrip(rows, schema_for(StreamType.LIQUIDATION))


def test_derivatives_poll_round_trip():
    rows = norm.normalize_derivatives_poll(
        ts=_ts(), source_id="binance_futures_rest", symbol="BTCUSDT",
        open_interest=12345.0, long_short_ratio=1.23, period="5m",
    )
    assert rows[0]["open_interest"] == 12345.0
    _roundtrip(rows, schema_for(StreamType.DERIVATIVES_POLL))
