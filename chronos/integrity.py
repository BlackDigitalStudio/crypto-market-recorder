"""P3 — integrity validators over the ingest stream.

Each :class:`Validator` is stateful and single-threaded with respect to the
stream it monitors. ``Recorder`` serializes calls per ``StreamKey`` so
implementations do not need their own locking.

The produced :class:`GapRecord` rows are written to a dedicated
:attr:`StreamType.INTEGRITY_GAP` stream per ``(source_id, exchange,
symbol)`` triple, letting consumers filter by ``monitored_stream_type`` /
``monitored_channel`` to reason about any individual data feed.

Re-snapshot triggering is intentionally out of this module: the recorder
surfaces gaps as a data product; the gateway layer (not yet landed in
v2) decides whether to re-pull a REST snapshot in response. Keeps the
archive path free of side-effects.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .timestamps import Timestamps
from .version import StreamType


def _i(x: Any) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class GapRecord:
    """One anomaly observed against an input stream."""

    gap_kind: str
    expected: str | None = None
    observed: str | None = None
    previous_value: int | None = None
    current_value: int | None = None
    skip_count: int | None = None
    note: str | None = None


class Validator(Protocol):
    """Observe one incoming message; return zero or more gap records.

    Implementations are stateful — they typically track the last-seen
    sequence or id. The state is private to the instance.
    """

    def check(self, msg: dict[str, Any], ts: Timestamps) -> list[GapRecord]:
        ...

    def reset(self) -> None:
        """Wipe internal state (after a re-subscribe / gateway reconnect)."""
        ...


# --- Binance futures depth diff sequence ---
#
# Contract:
#   msg.U = first update id in the event
#   msg.u = final update id in the event
#   msg.pu = previous event's final update id
#   Invariant: pu == prev.u (strict). If this breaks the maintained book
#   has lost coherence and must be rebuilt from REST + resync.
#
# Optional secondary check: U == prev.u + 1 — Binance actually allows
# intra-event contiguous windows only, so U should follow prev.u + 1 except
# across the very first event after a snapshot. We record this as a softer
# "skip" signal, since some gateway implementations suppress it by design.


class DepthSequenceValidator:
    """Binance futures depth `pu → u` sequence validator."""

    __slots__ = ("_last_u",)

    def __init__(self) -> None:
        self._last_u: int | None = None

    def check(self, msg: dict[str, Any], ts: Timestamps) -> list[GapRecord]:
        U = _i(msg.get("U"))
        u = _i(msg.get("u"))
        pu = _i(msg.get("pu"))
        if u is None:
            return []
        gaps: list[GapRecord] = []
        last = self._last_u
        if last is not None:
            if pu is not None and pu != last:
                gaps.append(GapRecord(
                    gap_kind="depth_sequence_pu_mismatch",
                    expected=f"pu=={last}",
                    observed=f"pu={pu}",
                    previous_value=last,
                    current_value=pu,
                    skip_count=(pu - last) if pu is not None else None,
                    note="maintained book must be rebuilt",
                ))
            elif pu is None and U is not None and U != last + 1:
                gaps.append(GapRecord(
                    gap_kind="depth_sequence_U_skip",
                    expected=f"U=={last + 1}",
                    observed=f"U={U}",
                    previous_value=last,
                    current_value=U,
                    skip_count=(U - (last + 1)),
                ))
        self._last_u = u
        return gaps

    def reset(self) -> None:
        self._last_u = None


# --- Monotonic-id validators ---
#
# Binance aggTrade `a` is globally monotonic per symbol. A regress or large
# skip is worth surfacing. Small skips are normal because aggTrade collapses
# N raw prints into one — the skip == collapsed-count - 1 is not anomalous,
# but `l - f + 1 == skip` only holds within one event. Across events `a`
# should strictly increase by 1.
#
# We emit two gap kinds:
#   - `aggtrade_id_regress` when current <= previous (hard anomaly)
#   - `aggtrade_id_skip`    when current > previous + 1 (informational;
#                            usually indicates a dropped frame, but can
#                            happen in low-volume symbols if nothing else
#                            traded in the interval — hence informational)


class MonotonicIdValidator:
    """Track a single monotonically increasing integer id field."""

    __slots__ = ("_last", "_id_key", "_gap_prefix", "_skip_threshold")

    def __init__(
        self,
        *,
        id_key: str,
        gap_prefix: str,
        skip_threshold: int = 1,
    ) -> None:
        self._last: int | None = None
        self._id_key = id_key
        self._gap_prefix = gap_prefix
        self._skip_threshold = max(1, int(skip_threshold))

    def check(self, msg: dict[str, Any], ts: Timestamps) -> list[GapRecord]:
        cur = _i(msg.get(self._id_key))
        if cur is None:
            return []
        last = self._last
        gaps: list[GapRecord] = []
        if last is not None:
            if cur <= last:
                gaps.append(GapRecord(
                    gap_kind=f"{self._gap_prefix}_regress",
                    expected=f"{self._id_key}>{last}",
                    observed=f"{self._id_key}={cur}",
                    previous_value=last,
                    current_value=cur,
                    skip_count=(cur - last),
                ))
            elif cur > last + self._skip_threshold:
                gaps.append(GapRecord(
                    gap_kind=f"{self._gap_prefix}_skip",
                    expected=f"{self._id_key}=={last + 1}",
                    observed=f"{self._id_key}={cur}",
                    previous_value=last,
                    current_value=cur,
                    skip_count=(cur - (last + 1)),
                ))
        # Always advance — even on a regress we keep the larger observed
        # id as the new baseline (regress itself is the recorded signal).
        if last is None or cur > last:
            self._last = cur
        return gaps

    def reset(self) -> None:
        self._last = None


# --- Default wiring ---
#
# Returns a fresh set of validators suited to a given (source_id, channel).
# Callers attach the result to `Recorder.register(..., validators=...)`.


def default_validators_for(source_id: str, channel: str) -> list[Validator]:
    if source_id == "binance_futures_ws":
        if channel == "depthUpdate":
            return [DepthSequenceValidator()]
        if channel == "aggTrade":
            return [MonotonicIdValidator(id_key="a", gap_prefix="aggtrade_id")]
    # Other venues get defaults as their gap semantics get modeled (lands
    # alongside P15 per-exchange quirks).
    return []


# --- Row builder ---


def gap_row(
    *,
    gap: GapRecord,
    ts: Timestamps,
    source_id: str,
    symbol: str,
    monitored_stream_type: StreamType,
    monitored_channel: str,
) -> dict[str, Any]:
    return {
        "local_ts_us": ts.local_ts_us,
        "exchange_event_ts_us": ts.exchange_event_ts_us,
        "exchange_trans_ts_us": ts.exchange_trans_ts_us,
        "source_id": source_id,
        "symbol": symbol,
        "monitored_stream_type": monitored_stream_type.value,
        "monitored_channel": monitored_channel,
        "gap_kind": gap.gap_kind,
        "expected": gap.expected,
        "observed": gap.observed,
        "previous_value": gap.previous_value,
        "current_value": gap.current_value,
        "skip_count": gap.skip_count,
        "note": gap.note,
    }
