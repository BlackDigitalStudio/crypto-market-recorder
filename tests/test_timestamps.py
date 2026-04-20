"""P1: extract_timestamps per source/channel."""
from __future__ import annotations

from chronos.timestamps import Timestamps, extract_timestamps


def test_binance_depth_update_uses_E_and_T_ms_to_us():
    msg = {"e": "depthUpdate", "E": 1_700_000_000_123, "T": 1_700_000_000_122, "s": "BTCUSDT", "U": 1, "u": 2, "pu": 0, "b": [], "a": []}
    ts = extract_timestamps(msg, source_id="binance_futures_ws", channel="depthUpdate")
    assert isinstance(ts, Timestamps)
    assert ts.exchange_event_ts_us == 1_700_000_000_123_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_122_000
    assert ts.local_ts_us > 0


def test_binance_agg_trade_uses_E_and_T():
    msg = {"e": "aggTrade", "E": 1_700_000_000_500, "T": 1_700_000_000_499, "a": 42, "p": "50000", "q": "0.1", "m": True}
    ts = extract_timestamps(msg, source_id="binance_futures_ws", channel="aggTrade")
    assert ts.exchange_event_ts_us == 1_700_000_000_500_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_499_000


def test_binance_mark_price_never_uses_T_as_trans():
    # T here is next-funding-time, not a transaction time — must be None.
    msg = {"e": "markPriceUpdate", "E": 1_700_000_000_000, "T": 1_700_000_900_000, "p": "50000", "r": "0.0001"}
    ts = extract_timestamps(msg, source_id="binance_futures_ws", channel="markPriceUpdate")
    assert ts.exchange_event_ts_us == 1_700_000_000_000_000
    assert ts.exchange_trans_ts_us is None


def test_binance_force_order_pulls_T_from_nested_o():
    msg = {"e": "forceOrder", "E": 1_700_000_000_000, "o": {"s": "BTCUSDT", "S": "SELL", "T": 1_700_000_000_111, "p": "50000", "q": "0.5"}}
    ts = extract_timestamps(msg, source_id="binance_futures_ws", channel="forceOrder")
    assert ts.exchange_event_ts_us == 1_700_000_000_000_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_111_000


def test_bybit_public_trade_envelope_plus_item():
    msg = {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1_700_000_000_050, "data": [{"T": 1_700_000_000_049, "p": "50000", "v": "0.01", "S": "Sell", "i": "abc"}]}
    ts = extract_timestamps(msg, source_id="bybit_v5_public_linear", channel="publicTrade")
    assert ts.exchange_event_ts_us == 1_700_000_000_050_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_049_000


def test_okx_trades_ts_from_item():
    msg = {"arg": {"channel": "trades"}, "data": [{"ts": "1700000000012", "px": "50000", "sz": "0.1", "side": "buy", "tradeId": "x"}]}
    ts = extract_timestamps(msg, source_id="okx_v5_public", channel="trades")
    assert ts.exchange_event_ts_us == 1_700_000_000_012_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_012_000


def test_gateio_seconds_fallback():
    msg = {"channel": "futures.trades", "event": "update", "time": 1_700_000_000, "result": [{"create_time": 1_700_000_000, "price": "50000", "size": -1, "id": 1}]}
    ts = extract_timestamps(msg, source_id="gateio_v4_futures", channel="futures.trades")
    assert ts.exchange_event_ts_us == 1_700_000_000_000_000
    assert ts.exchange_trans_ts_us == 1_700_000_000_000_000


def test_unknown_source_falls_back_to_local_only():
    ts = extract_timestamps({"foo": "bar"}, source_id="unknown", channel="xxx")
    assert ts.exchange_event_ts_us is None
    assert ts.exchange_trans_ts_us is None
    assert ts.local_ts_us > 0


def test_malformed_fields_yield_none():
    msg = {"e": "depthUpdate", "E": "not-a-number", "T": -5, "b": [], "a": []}
    ts = extract_timestamps(msg, source_id="binance_futures_ws", channel="depthUpdate")
    assert ts.exchange_event_ts_us is None
    assert ts.exchange_trans_ts_us is None
