"""P14: funding settlement detector."""
from __future__ import annotations

from chronos.funding import FundingSettlementDetector
from chronos.timestamps import Timestamps


def _ts() -> Timestamps:
    return Timestamps(local_ts_us=1, exchange_event_ts_us=2, exchange_trans_ts_us=None)


def test_first_observation_does_not_emit():
    d = FundingSettlementDetector()
    rows = d.observe(
        next_funding_time_ms=1_700_000_900_000,
        funding_rate=0.0001,
        ts=_ts(), source_id="binance_futures_ws", symbol="BTCUSDT",
    )
    assert rows == []


def test_same_T_does_not_emit():
    d = FundingSettlementDetector()
    for _ in range(5):
        rows = d.observe(
            next_funding_time_ms=1_700_000_900_000,
            funding_rate=0.0001,
            ts=_ts(), source_id="s", symbol="SYM",
        )
    assert rows == []


def test_T_advance_emits_settlement_with_last_rate():
    d = FundingSettlementDetector()
    d.observe(next_funding_time_ms=1_700_000_900_000, funding_rate=0.0001,
              ts=_ts(), source_id="s", symbol="SYM")
    d.observe(next_funding_time_ms=1_700_000_900_000, funding_rate=0.00015,
              ts=_ts(), source_id="s", symbol="SYM")
    rows = d.observe(
        next_funding_time_ms=1_700_029_700_000,  # 8h later
        funding_rate=0.0002,  # next-interval rate, already visible
        ts=_ts(), source_id="s", symbol="SYM",
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["realized_funding_rate"] == 0.00015
    assert r["settled_at_ms"] == 1_700_000_900_000
    assert r["source_id"] == "s" and r["symbol"] == "SYM"


def test_T_regress_is_ignored():
    """Clock anomaly or out-of-order frame — don't fire a false settlement."""
    d = FundingSettlementDetector()
    d.observe(next_funding_time_ms=1_700_000_900_000, funding_rate=0.0001,
              ts=_ts(), source_id="s", symbol="SYM")
    rows = d.observe(
        next_funding_time_ms=1_699_000_000_000,
        funding_rate=0.0001,
        ts=_ts(), source_id="s", symbol="SYM",
    )
    # Per current semantics regress simply advances state; no emission.
    assert rows == []


def test_reset_clears_state():
    d = FundingSettlementDetector()
    d.observe(next_funding_time_ms=1_700_000_900_000, funding_rate=0.0001,
              ts=_ts(), source_id="s", symbol="SYM")
    d.reset()
    rows = d.observe(
        next_funding_time_ms=1_700_029_700_000, funding_rate=0.0002,
        ts=_ts(), source_id="s", symbol="SYM",
    )
    # After reset, no prior T → no emission.
    assert rows == []


def test_missing_last_rate_falls_back_to_current():
    d = FundingSettlementDetector()
    d.observe(next_funding_time_ms=1_700_000_900_000, funding_rate=None,
              ts=_ts(), source_id="s", symbol="SYM")
    rows = d.observe(
        next_funding_time_ms=1_700_029_700_000, funding_rate=0.0002,
        ts=_ts(), source_id="s", symbol="SYM",
    )
    assert len(rows) == 1
    assert rows[0]["realized_funding_rate"] == 0.0002
