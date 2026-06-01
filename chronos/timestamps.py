"""P1 — triple timestamps per recorded event.

Every normalized row and every raw-archive line carries three fields:

- ``local_ts_us`` — our host clock, microseconds since epoch, stamped at the
  moment the recorder observed the message. Ground truth for latency and
  gap analysis, bounded by host clock quality (see P4, chrony/PTP).
- ``exchange_event_ts_us`` — exchange envelope "event time" (Binance `E`,
  Bybit envelope `ts`). When the exchange asserted the message existed.
- ``exchange_trans_ts_us`` — matching-engine "transaction time" (Binance
  depth / aggTrade `T`, per-trade `ts` on OKX/Bitget/Gate.io, per-trade
  `T` inside Bybit `data[]`). When the underlying book event happened.

All three are microseconds since epoch UTC. Upstream values are usually
milliseconds; we multiply by 1000 on extraction. Missing fields are ``None``
— do not guess. Consumers that need a single timestamp can fall back
``trans → event → local`` in that order.

Important: ``markPrice`` streams put next-funding-time in `T`. That is
emphatically not a transaction time, so the extractor for mark_price
returns `trans=None` and exposes next-funding elsewhere in normalization.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Timestamps:
    local_ts_us: int
    exchange_event_ts_us: int | None
    exchange_trans_ts_us: int | None

    def as_dict(self) -> dict[str, int | None]:
        return {
            "local_ts_us": self.local_ts_us,
            "exchange_event_ts_us": self.exchange_event_ts_us,
            "exchange_trans_ts_us": self.exchange_trans_ts_us,
        }


def now_us() -> int:
    """Microsecond epoch from the best monotonic-equivalent wall clock."""
    return time.time_ns() // 1_000


def _ms_to_us(v: Any) -> int | None:
    if v is None:
        return None
    try:
        i = int(v)
    except (TypeError, ValueError):
        return None
    if i <= 0:
        return None
    return i * 1000


def _s_to_us(v: Any) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f <= 0:
        return None
    return int(f * 1_000_000)


# --- Per-source extractors ---
#
# Key is (source_id, channel) where source_id matches SOURCE_API_VERSIONS and
# channel is a short identifier of the message kind ("depth", "aggTrade",
# "markPrice", "forceOrder", "publicTrade", "trades"). Each extractor returns
# (event_ts_us, trans_ts_us) — local is stamped by the caller.


def _binance_envelope(msg: dict) -> tuple[int | None, int | None]:
    """Binance futures WS: common `E` (event) + `T` (transaction) on top level.

    Used by depthUpdate, aggTrade. For markPrice `T` is next-funding and must
    not be treated as transaction time.
    """
    return _ms_to_us(msg.get("E")), _ms_to_us(msg.get("T"))


def _binance_mark_price(msg: dict) -> tuple[int | None, int | None]:
    # E is the event time; T here is next funding time, not a trans time.
    return _ms_to_us(msg.get("E")), None


def _binance_force_order(msg: dict) -> tuple[int | None, int | None]:
    # `E` at envelope, matched-trade time under `o.T`.
    event = _ms_to_us(msg.get("E"))
    o = msg.get("o") or {}
    trans = _ms_to_us(o.get("T"))
    return event, trans


def _bybit_envelope_and_first_trade(msg: dict) -> tuple[int | None, int | None]:
    """Bybit publicTrade envelope has `ts`; per-item `T` is the trade ts.

    For a fan-out event the envelope-level extractor returns envelope `ts`
    as the event time and the first item's `T` as a representative trans
    time. The normalizer re-stamps per-item when it splits the message.
    """
    event = _ms_to_us(msg.get("ts"))
    data = msg.get("data") or []
    trans = None
    if isinstance(data, list) and data:
        trans = _ms_to_us(data[0].get("T"))
    return event, trans


def _okx_trades(msg: dict) -> tuple[int | None, int | None]:
    # OKX public trades: no envelope time; per-item `ts` is both event&trans.
    data = msg.get("data") or []
    if isinstance(data, list) and data:
        ts = _ms_to_us(data[0].get("ts"))
        return ts, ts
    return None, None


def _bitget_trades(msg: dict) -> tuple[int | None, int | None]:
    # Bitget v2 envelope has `ts`, per-item `ts` is trade time.
    event = _ms_to_us(msg.get("ts"))
    data = msg.get("data") or []
    trans = None
    if isinstance(data, list) and data:
        trans = _ms_to_us(data[0].get("ts"))
    return event, trans


def _gateio_trades(msg: dict) -> tuple[int | None, int | None]:
    # Gate.io futures.trades: envelope `time_ms` or `time`; per-item
    # `create_time_ms` or `create_time`.
    event = _ms_to_us(msg.get("time_ms"))
    if event is None:
        event = _s_to_us(msg.get("time"))
    result = msg.get("result") or []
    trans = None
    if isinstance(result, list) and result:
        item = result[0]
        trans = _ms_to_us(item.get("create_time_ms"))
        if trans is None:
            trans = _s_to_us(item.get("create_time"))
    return event, trans


def _iso_to_us(v: Any) -> int | None:
    """Parse ISO-8601 UTC timestamp to microseconds since epoch."""
    if v is None:
        return None
    try:
        from datetime import datetime
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1_000_000)
    except Exception:
        return None


def _coinbase_market_trades(msg: dict) -> tuple[int | None, int | None]:
    # Coinbase Advanced Trade market_trades envelope has top-level
    # "timestamp" (ISO8601) and per-trade "time" (also ISO).
    event = _iso_to_us(msg.get("timestamp"))
    events = msg.get("events") or []
    trans = None
    if isinstance(events, list) and events:
        trades = events[0].get("trades") or []
        if isinstance(trades, list) and trades:
            trans = _iso_to_us(trades[0].get("time"))
    return event, trans


def _deribit_trades(msg: dict) -> tuple[int | None, int | None]:
    # Deribit JSON-RPC: timestamp is ms, at envelope.params or per trade.
    params = msg.get("params") or {}
    data = params.get("data") or []
    trans = None
    if isinstance(data, list) and data:
        trans = _ms_to_us(data[0].get("timestamp"))
    return trans, trans  # Deribit has no separate envelope ts; reuse


def _rest_poll(msg: dict) -> tuple[int | None, int | None]:
    # REST snapshots / polls: exchange does not (usually) stamp an event
    # time in the body. We carry only local_ts. `time` or `E` fields, if
    # present, get pulled through for completeness.
    e = _ms_to_us(msg.get("E"))
    if e is None:
        e = _ms_to_us(msg.get("time"))
    return e, None


_EXTRACTORS: dict[tuple[str, str], Any] = {
    ("binance_futures_ws", "depthUpdate"): _binance_envelope,
    ("binance_futures_ws", "aggTrade"): _binance_envelope,
    ("binance_futures_ws", "trade"): _binance_envelope,
    ("binance_futures_ws", "markPriceUpdate"): _binance_mark_price,
    ("binance_futures_ws", "forceOrder"): _binance_force_order,
    ("bybit_v5_public_linear", "publicTrade"): _bybit_envelope_and_first_trade,
    ("okx_v5_public", "trades"): _okx_trades,
    ("bitget_v2_public", "trade"): _bitget_trades,
    ("gateio_v4_futures", "futures.trades"): _gateio_trades,
    ("coinbase_advanced_trade_ws", "market_trades"): _coinbase_market_trades,
    ("deribit_v2_public_ws", "trades"): _deribit_trades,
    ("binance_futures_rest", "depth"): _rest_poll,
    ("binance_futures_rest", "openInterest"): _rest_poll,
    ("binance_futures_rest", "topLongShortAccountRatio"): _rest_poll,
}


def extract_timestamps(msg: dict, *, source_id: str, channel: str) -> Timestamps:
    """Return :class:`Timestamps` for a received message.

    ``source_id`` must match a key in :data:`.version.SOURCE_API_VERSIONS`.
    Unknown (source, channel) pairs fall back to local-only — useful for
    new feeds added ahead of a dedicated extractor.
    """
    local = now_us()
    extractor = _EXTRACTORS.get((source_id, channel))
    if extractor is None:
        return Timestamps(local_ts_us=local, exchange_event_ts_us=None, exchange_trans_ts_us=None)
    event, trans = extractor(msg)
    return Timestamps(local_ts_us=local, exchange_event_ts_us=event, exchange_trans_ts_us=trans)


def stamp_item(
    *,
    local_ts_us: int,
    envelope_event_ts_us: int | None,
    item_trans_ms: Any,
) -> Timestamps:
    """Build per-item timestamps when normalizing a multi-item message.

    Bybit/OKX/Bitget/Gate.io batch multiple trades in one WS frame. The
    envelope carries one event time; each item carries its own transaction
    time. We re-use the single local_ts captured at WS read time (the
    whole batch is "observed" simultaneously).
    """
    return Timestamps(
        local_ts_us=local_ts_us,
        exchange_event_ts_us=envelope_event_ts_us,
        exchange_trans_ts_us=_ms_to_us(item_trans_ms),
    )
