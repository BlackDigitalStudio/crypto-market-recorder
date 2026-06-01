"""Per-source normalizers — raw WS frame → list of canonical row dicts.

Each normalizer returns ``list[dict]`` whose dicts match the Arrow schema
for the target :class:`StreamType`. Row preamble (timestamps, source_id,
symbol) is filled here so downstream writers don't need to reach back into
the raw message.

Canonicalization rules applied uniformly:

- ``is_buyer_maker`` — true iff the buy side was the passive (maker) side
  of the trade, equivalently the aggressor was the seller. Aligns with
  Binance's ``m`` field and makes cross-venue trades directly comparable.
- prices / quantities are returned as floats. We never drop precision by
  passing strings through downstream; the raw archive keeps original
  decimals if exact recovery is ever required.
- ``side`` on depth diffs is literal ``"bid"`` / ``"ask"``. ``update_type``
  is ``"remove"`` when qty == 0 (Binance convention), else ``"update"`` —
  Binance does not distinguish insert vs update in the wire format, so we
  collapse both to ``"update"``; consumers can recover insert/update from
  their own book state if needed.
"""
from __future__ import annotations

from typing import Any

from .timestamps import Timestamps, stamp_item, _ms_to_us


def _preamble_row(
    *,
    ts: Timestamps,
    source_id: str,
    symbol: str,
) -> dict[str, Any]:
    return {
        "local_ts_us": ts.local_ts_us,
        "exchange_event_ts_us": ts.exchange_event_ts_us,
        "exchange_trans_ts_us": ts.exchange_trans_ts_us,
        "source_id": source_id,
        "symbol": symbol,
    }


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _i(x: Any) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


# --- Binance futures WS ---


def normalize_binance_depth_update(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    """`@depth@100ms` diff → one row per changed price level."""
    base = _preamble_row(ts=ts, source_id=source_id, symbol=symbol)
    first_uid = _i(msg.get("U"))
    final_uid = _i(msg.get("u"))
    prev_final_uid = _i(msg.get("pu"))
    rows: list[dict] = []
    for side_key, side in (("b", "bid"), ("a", "ask")):
        for entry in msg.get(side_key) or []:
            if not entry or len(entry) < 2:
                continue
            price = _f(entry[0])
            qty = _f(entry[1])
            update_type = "remove" if qty == 0.0 else "update"
            rows.append({
                **base,
                "first_update_id": first_uid,
                "final_update_id": final_uid,
                "prev_final_update_id": prev_final_uid,
                "side": side,
                "price": price,
                "qty": qty,
                "update_type": update_type,
            })
    return rows


def normalize_binance_agg_trade(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    agg_id = msg.get("a")
    return [{
        **_preamble_row(ts=ts, source_id=source_id, symbol=symbol),
        "trade_id": str(agg_id) if agg_id is not None else None,
        "price": _f(msg.get("p")),
        "qty": _f(msg.get("q")),
        "is_buyer_maker": bool(msg.get("m", False)),
        "first_trade_id": _i(msg.get("f")),
        "last_trade_id": _i(msg.get("l")),
    }]


def normalize_binance_trade(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    """Binance USDⓈ-M `@trade` (individual prints). Unlike `@aggTrade` these
    carry a per-trade id `t` and no aggregate `f`/`l` window — closer to the
    Cryptolake `raw/trades` schema (one row per fill with an `id`)."""
    tid = msg.get("t")
    return [{
        **_preamble_row(ts=ts, source_id=source_id, symbol=symbol),
        "trade_id": str(tid) if tid is not None else None,
        "price": _f(msg.get("p")),
        "qty": _f(msg.get("q")),
        "is_buyer_maker": bool(msg.get("m", False)),
        "first_trade_id": None,
        "last_trade_id": None,
    }]


def normalize_binance_mark_price(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    return [{
        **_preamble_row(ts=ts, source_id=source_id, symbol=symbol),
        "mark_price": _f(msg.get("p")),
        "index_price": _f(msg.get("i")) if msg.get("i") is not None else None,
        "estimated_settle_price": _f(msg.get("P")) if msg.get("P") is not None else None,
        "funding_rate": _f(msg.get("r")) if msg.get("r") is not None else None,
        "next_funding_time_ms": _i(msg.get("T")),
    }]


def normalize_binance_force_order(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    o = msg.get("o") or {}
    return [{
        **_preamble_row(ts=ts, source_id=source_id, symbol=symbol),
        "side": str(o.get("S", "")),
        "order_type": str(o.get("o", "")) or None,
        "time_in_force": str(o.get("f", "")) or None,
        "original_qty": _f(o.get("q")) if o.get("q") is not None else None,
        "price": _f(o.get("p")) if o.get("p") is not None else None,
        "avg_price": _f(o.get("ap")) if o.get("ap") is not None else None,
        "order_status": str(o.get("X", "")) or None,
        "last_filled_qty": _f(o.get("l")) if o.get("l") is not None else None,
        "filled_accumulated_qty": _f(o.get("z")) if o.get("z") is not None else None,
    }]


# --- Bybit v5 publicTrade ---


def normalize_bybit_public_trade(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    data = msg.get("data") or []
    rows: list[dict] = []
    for t in data:
        item_ts = stamp_item(
            local_ts_us=ts.local_ts_us,
            envelope_event_ts_us=ts.exchange_event_ts_us,
            item_trans_ms=t.get("T"),
        )
        # Bybit `S` is aggressor side.
        aggressor = str(t.get("S", "")).lower()
        is_buyer_maker = aggressor == "sell"
        rows.append({
            **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
            "trade_id": str(t.get("i", "")) or None,
            "price": _f(t.get("p")),
            "qty": _f(t.get("v")),
            "is_buyer_maker": is_buyer_maker,
            "first_trade_id": None,
            "last_trade_id": None,
        })
    return rows


# --- OKX v5 trades ---


def normalize_okx_trades(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    data = msg.get("data") or []
    rows: list[dict] = []
    for t in data:
        item_ts = stamp_item(
            local_ts_us=ts.local_ts_us,
            envelope_event_ts_us=ts.exchange_event_ts_us,
            item_trans_ms=t.get("ts"),
        )
        aggressor = str(t.get("side", "")).lower()
        rows.append({
            **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
            "trade_id": str(t.get("tradeId", "")) or None,
            "price": _f(t.get("px")),
            "qty": _f(t.get("sz")),
            "is_buyer_maker": aggressor == "sell",
            "first_trade_id": None,
            "last_trade_id": None,
        })
    return rows


# --- Bitget v2 trade ---


def normalize_bitget_trade(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    data = msg.get("data") or []
    rows: list[dict] = []
    for t in data:
        item_ts = stamp_item(
            local_ts_us=ts.local_ts_us,
            envelope_event_ts_us=ts.exchange_event_ts_us,
            item_trans_ms=t.get("ts"),
        )
        aggressor = str(t.get("side", "")).lower()
        rows.append({
            **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
            "trade_id": str(t.get("tradeId", "")) or None,
            "price": _f(t.get("price")),
            "qty": _f(t.get("size")),
            "is_buyer_maker": aggressor == "sell",
            "first_trade_id": None,
            "last_trade_id": None,
        })
    return rows


# --- Gate.io v4 futures.trades ---


def normalize_gateio_futures_trades(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    # Gate.io sends non-trade control frames under `result` too
    # (subscribe acks are `result = {"status": "success"}`). Only trade
    # updates carry a list there.
    result = msg.get("result")
    if not isinstance(result, list):
        return []
    rows: list[dict] = []
    for t in result:
        trans_ms = t.get("create_time_ms")
        if trans_ms is None:
            ct = t.get("create_time")
            trans_ms = int(float(ct) * 1000) if ct is not None else None
        item_ts = stamp_item(
            local_ts_us=ts.local_ts_us,
            envelope_event_ts_us=ts.exchange_event_ts_us,
            item_trans_ms=trans_ms,
        )
        size_signed = t.get("size", 0)
        # Gate.io encodes aggressor in sign of size. Negative = sell aggressor.
        try:
            size_num = float(size_signed)
        except (TypeError, ValueError):
            size_num = 0.0
        is_buyer_maker = size_num < 0
        rows.append({
            **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
            "trade_id": str(t.get("id", "")) or None,
            "price": _f(t.get("price")),
            "qty": abs(size_num),
            "is_buyer_maker": is_buyer_maker,
            "first_trade_id": None,
            "last_trade_id": None,
        })
    return rows


# --- Coinbase Advanced Trade (market_trades) ---


def normalize_coinbase_market_trades(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    from .timestamps import _iso_to_us
    events = msg.get("events") or []
    rows: list[dict] = []
    for ev in events:
        for t in ev.get("trades") or []:
            trans_us = _iso_to_us(t.get("time"))
            item_ts = Timestamps(
                local_ts_us=ts.local_ts_us,
                exchange_event_ts_us=ts.exchange_event_ts_us,
                exchange_trans_ts_us=trans_us,
            )
            # Coinbase `side` is the aggressor ("BUY"/"SELL").
            aggressor = str(t.get("side", "")).upper()
            rows.append({
                **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
                "trade_id": str(t.get("trade_id", "")) or None,
                "price": _f(t.get("price")),
                "qty": _f(t.get("size")),
                "is_buyer_maker": aggressor == "SELL",
                "first_trade_id": None,
                "last_trade_id": None,
            })
    return rows


# --- Deribit trades (JSON-RPC) ---


def normalize_deribit_trades(
    msg: dict, *, ts: Timestamps, source_id: str, symbol: str,
) -> list[dict]:
    params = msg.get("params") or {}
    data = params.get("data") or []
    rows: list[dict] = []
    for t in data:
        trans_us = None
        if t.get("timestamp") is not None:
            trans_us = int(t["timestamp"]) * 1000 if int(t["timestamp"]) > 0 else None
        item_ts = Timestamps(
            local_ts_us=ts.local_ts_us,
            exchange_event_ts_us=ts.exchange_event_ts_us,
            exchange_trans_ts_us=trans_us,
        )
        direction = str(t.get("direction", "")).lower()
        rows.append({
            **_preamble_row(ts=item_ts, source_id=source_id, symbol=symbol),
            "trade_id": str(t.get("trade_id", "")) or None,
            "price": _f(t.get("price")),
            "qty": _f(t.get("amount")),
            "is_buyer_maker": direction == "sell",
            "first_trade_id": None,
            "last_trade_id": None,
        })
    return rows


# --- REST polls (aggregated derivatives state) ---


def normalize_derivatives_poll(
    *,
    ts: Timestamps,
    source_id: str,
    symbol: str,
    open_interest: float | None,
    long_short_ratio: float | None,
    long_account: float | None = None,
    short_account: float | None = None,
    period: str | None = None,
) -> list[dict]:
    return [{
        **_preamble_row(ts=ts, source_id=source_id, symbol=symbol),
        "open_interest": float(open_interest) if open_interest is not None else None,
        "long_short_ratio": float(long_short_ratio) if long_short_ratio is not None else None,
        "long_account": float(long_account) if long_account is not None else None,
        "short_account": float(short_account) if short_account is not None else None,
        "period": period,
    }]
