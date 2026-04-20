"""P14 — funding settlement detector.

Binance `markPrice@1s` carries the current ``funding_rate`` (``r``) and the
*next* funding timestamp (``T``). We emit a settlement event whenever we
observe ``T`` advance: the previous ``T`` is the moment just settled, and
the last observed ``r`` before the advance is the realized rate for that
interval.

Every detector instance is private to a single ``(source_id, symbol)``
pair; the recorder serializes calls so no internal locking is needed.

Output rows match :data:`schemas.FUNDING_SETTLEMENT_SCHEMA`.
"""
from __future__ import annotations

from typing import Any

from .timestamps import Timestamps


class FundingSettlementDetector:
    """Detect settlement events from a sequence of markPrice rows.

    ``observe()`` takes the *already-normalized* markPrice row fields
    (``next_funding_time_ms``, ``funding_rate``) rather than the raw WS
    dict — keeps the detector decoupled from upstream payload shape and
    reusable across venues that expose the same two values under
    different names.
    """

    __slots__ = ("_last_next_funding_time_ms", "_last_funding_rate")

    def __init__(self) -> None:
        self._last_next_funding_time_ms: int | None = None
        self._last_funding_rate: float | None = None

    def observe(
        self,
        *,
        next_funding_time_ms: int | None,
        funding_rate: float | None,
        ts: Timestamps,
        source_id: str,
        symbol: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        last_T = self._last_next_funding_time_ms
        if (
            last_T is not None
            and next_funding_time_ms is not None
            and next_funding_time_ms > last_T
        ):
            # A settlement happened at last_T. The realized rate is the
            # last one we saw before the advance; fall back to the current
            # message's rate if the pre-rollover value was missing.
            realized = (
                self._last_funding_rate
                if self._last_funding_rate is not None
                else funding_rate
            )
            rows.append({
                "local_ts_us": ts.local_ts_us,
                "exchange_event_ts_us": ts.exchange_event_ts_us,
                "exchange_trans_ts_us": ts.exchange_trans_ts_us,
                "source_id": source_id,
                "symbol": symbol,
                "realized_funding_rate": float(realized) if realized is not None else 0.0,
                "settled_at_ms": int(last_T),
            })

        if next_funding_time_ms is not None:
            self._last_next_funding_time_ms = int(next_funding_time_ms)
        if funding_rate is not None:
            self._last_funding_rate = float(funding_rate)
        return rows

    def reset(self) -> None:
        self._last_next_funding_time_ms = None
        self._last_funding_rate = None
