"""Regression: reconcile with large-snapshot book + small max_price_levels.

Reproduces the 2026-04-20 live-run false-positive: local book seeded from
a 1000-level REST snapshot, reconciled against the same snapshot with
``max_price_levels=100`` previously returned 1800 "missing_rest" findings
because the local side was iterated in full while REST was truncated.

Fix prunes both sides symmetrically → identical snapshots must yield 0
findings regardless of ``max_price_levels`` choice.
"""
from __future__ import annotations

from chronos.order_book import OrderBookV2


def _dense_snapshot(depth: int = 1000) -> dict:
    return {
        "lastUpdateId": 42,
        "bids": [[str(50000 - i * 0.5), "1.0"] for i in range(depth)],
        "asks": [[str(50001 + i * 0.5), "1.0"] for i in range(depth)],
    }


def test_identical_snapshot_no_findings_with_max_levels_small():
    b = OrderBookV2("BTCUSDT")
    snap = _dense_snapshot(1000)
    b.restore_from_snapshot(snap)
    for max_levels in (50, 100, 500, 1000, None):
        findings = b.compare_to_rest(snap, max_price_levels=max_levels)
        assert findings == [], f"max_price_levels={max_levels}: {len(findings)} false positives"


def test_real_drift_still_detected_after_fix():
    b = OrderBookV2("BTCUSDT")
    snap = _dense_snapshot(1000)
    b.restore_from_snapshot(snap)
    drifted = _dense_snapshot(1000)
    # Tweak one bid level within top-100 on the REST side.
    drifted["bids"][5] = ["49997.5", "99.0"]
    findings = b.compare_to_rest(drifted, max_price_levels=100)
    assert len(findings) >= 1
    kinds = {f.kind for f in findings}
    assert "qty_mismatch" in kinds


def test_drift_outside_window_ignored():
    b = OrderBookV2("BTCUSDT")
    snap = _dense_snapshot(1000)
    b.restore_from_snapshot(snap)
    drifted = _dense_snapshot(1000)
    # Flip a level at depth 500 (beyond max_price_levels=100 window).
    drifted["bids"][500] = [str(50000 - 500 * 0.5), "99.0"]
    findings = b.compare_to_rest(drifted, max_price_levels=100)
    # Should not flag anything — that level is outside the contract window.
    assert findings == [], f"leaked deep-level drift: {findings[:3]}"
