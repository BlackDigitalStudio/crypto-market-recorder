"""P3: integrity validators."""
from __future__ import annotations

from chronos.integrity import (
    DepthSequenceValidator,
    MonotonicIdValidator,
    default_validators_for,
    gap_row,
)
from chronos.timestamps import Timestamps
from chronos.version import StreamType


def _ts() -> Timestamps:
    return Timestamps(local_ts_us=1, exchange_event_ts_us=2, exchange_trans_ts_us=3)


def test_depth_sequence_validator_accepts_first_message():
    v = DepthSequenceValidator()
    gaps = v.check({"U": 100, "u": 105, "pu": 99, "b": [], "a": []}, _ts())
    assert gaps == []


def test_depth_sequence_validator_accepts_contiguous():
    v = DepthSequenceValidator()
    v.check({"U": 100, "u": 105, "pu": 99, "b": [], "a": []}, _ts())
    gaps = v.check({"U": 106, "u": 110, "pu": 105, "b": [], "a": []}, _ts())
    assert gaps == []


def test_depth_sequence_validator_detects_pu_mismatch():
    v = DepthSequenceValidator()
    v.check({"U": 100, "u": 105, "pu": 99, "b": [], "a": []}, _ts())
    gaps = v.check({"U": 200, "u": 210, "pu": 190, "b": [], "a": []}, _ts())
    assert len(gaps) == 1
    g = gaps[0]
    assert g.gap_kind == "depth_sequence_pu_mismatch"
    assert g.previous_value == 105 and g.current_value == 190
    assert g.skip_count == 190 - 105


def test_depth_sequence_validator_detects_U_skip_without_pu():
    v = DepthSequenceValidator()
    v.check({"U": 100, "u": 105, "b": [], "a": []}, _ts())
    gaps = v.check({"U": 200, "u": 210, "b": [], "a": []}, _ts())
    assert len(gaps) == 1
    assert gaps[0].gap_kind == "depth_sequence_U_skip"


def test_depth_sequence_validator_ignores_missing_u():
    v = DepthSequenceValidator()
    gaps = v.check({}, _ts())
    assert gaps == []


def test_depth_validator_reset_clears_baseline():
    v = DepthSequenceValidator()
    v.check({"U": 1, "u": 2, "pu": 0, "b": [], "a": []}, _ts())
    v.reset()
    gaps = v.check({"U": 1000, "u": 1001, "pu": 500, "b": [], "a": []}, _ts())
    assert gaps == []


def test_monotonic_id_validator_detects_regress():
    v = MonotonicIdValidator(id_key="a", gap_prefix="aggtrade_id")
    v.check({"a": 100}, _ts())
    gaps = v.check({"a": 99}, _ts())
    assert len(gaps) == 1
    assert gaps[0].gap_kind == "aggtrade_id_regress"


def test_monotonic_id_validator_detects_skip():
    v = MonotonicIdValidator(id_key="a", gap_prefix="aggtrade_id")
    v.check({"a": 100}, _ts())
    gaps = v.check({"a": 105}, _ts())  # skip=4
    assert len(gaps) == 1
    assert gaps[0].gap_kind == "aggtrade_id_skip"
    assert gaps[0].skip_count == 4


def test_monotonic_id_validator_contiguous_no_gap():
    v = MonotonicIdValidator(id_key="a", gap_prefix="aggtrade_id")
    v.check({"a": 100}, _ts())
    gaps = v.check({"a": 101}, _ts())
    assert gaps == []


def test_monotonic_id_validator_regress_does_not_advance_baseline():
    v = MonotonicIdValidator(id_key="a", gap_prefix="aggtrade_id")
    v.check({"a": 100}, _ts())
    v.check({"a": 50}, _ts())  # regress
    # Baseline still 100; a normal subsequent 101 is contiguous
    gaps = v.check({"a": 101}, _ts())
    assert gaps == []


def test_default_validators_wires_binance():
    vs = default_validators_for("binance_futures_ws", "depthUpdate")
    assert len(vs) == 1 and isinstance(vs[0], DepthSequenceValidator)
    vs = default_validators_for("binance_futures_ws", "aggTrade")
    assert len(vs) == 1 and isinstance(vs[0], MonotonicIdValidator)
    # Unknown source → empty
    assert default_validators_for("unknown", "xxx") == []


def test_gap_row_shape():
    from chronos.integrity import GapRecord
    row = gap_row(
        gap=GapRecord(gap_kind="k", expected="e", observed="o",
                      previous_value=1, current_value=2, skip_count=1, note="n"),
        ts=_ts(), source_id="s", symbol="SYM",
        monitored_stream_type=StreamType.DEPTH_DIFF,
        monitored_channel="depthUpdate",
    )
    assert row["gap_kind"] == "k"
    assert row["monitored_stream_type"] == "depth_diff"
    assert row["monitored_channel"] == "depthUpdate"
    assert row["source_id"] == "s"
    assert row["symbol"] == "SYM"
