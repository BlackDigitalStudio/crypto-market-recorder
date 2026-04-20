"""P6/P7/P11: OrderBookV2 unit tests."""
from __future__ import annotations

from chronos.order_book import BookChange, OrderBookV2
from chronos.timestamps import Timestamps


def _ts() -> Timestamps:
    return Timestamps(local_ts_us=1, exchange_event_ts_us=2, exchange_trans_ts_us=3)


def _snapshot(last_id: int = 100) -> dict:
    return {
        "lastUpdateId": last_id,
        "bids": [["50000", "1.0"], ["49999", "2.0"], ["49998", "3.0"]],
        "asks": [["50001", "1.5"], ["50002", "2.5"], ["50003", "3.5"]],
    }


def test_unsynced_book_drops_diffs():
    b = OrderBookV2("BTCUSDT")
    assert not b.synced
    changes = b.apply_diff({"U": 1, "u": 2, "pu": 0, "b": [["1", "1"]], "a": []})
    assert changes == []
    assert not b.synced


def test_restore_loads_levels():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    assert b.synced
    assert b.last_update_id == 100
    assert len(b) == 6
    assert b.best_bid() == (50000.0, 1.0)
    assert b.best_ask() == (50001.0, 1.5)


def test_apply_diff_insert_update_remove():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    changes = b.apply_diff({
        "U": 101, "u": 105, "pu": 100,
        "b": [["50000", "0"], ["49997", "4.0"], ["49998", "3.5"]],
        "a": [],
    })
    kinds = [(c.side, c.price, c.update_type) for c in changes]
    assert ("bid", 50000.0, "remove") in kinds
    assert ("bid", 49997.0, "insert") in kinds
    assert ("bid", 49998.0, "update") in kinds
    assert b.last_update_id == 105


def test_dup_qty_is_not_emitted():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    changes = b.apply_diff({
        "U": 101, "u": 102, "pu": 100,
        "b": [["50000", "1.0"]],  # same qty as snapshot
        "a": [],
    })
    assert changes == []


def test_stale_diff_dropped():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    changes = b.apply_diff({"U": 50, "u": 99, "pu": 49, "b": [["50000", "9"]], "a": []})
    assert changes == []
    assert b.last_update_id == 100


def test_remove_nonexistent_is_noop():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    changes = b.apply_diff({"U": 101, "u": 102, "pu": 100, "b": [["12345", "0"]], "a": []})
    assert changes == []


def test_top_n_zero_pads_when_shallow():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    row = b.snapshot_row(levels=5, ts=_ts(), source_id="x")
    # Snapshot had 3 bids and 3 asks; levels=5 should zero-pad.
    assert row["bid_prices"][0] == 50000.0
    assert row["bid_prices"][3] == 0.0 and row["bid_qtys"][3] == 0.0
    assert row["ask_prices"][0] == 50001.0
    assert row["ask_prices"][3] == 0.0


def test_checkpoint_rows_include_every_level():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    rows = b.checkpoint_rows(ts=_ts(), source_id="x")
    assert len(rows) == 6
    dump_ids = {r["dump_id"] for r in rows}
    assert len(dump_ids) == 1
    assert all(r["last_update_id"] == 100 for r in rows)
    sides = [(r["side"], r["price"]) for r in rows]
    assert ("bid", 50000.0) in sides
    assert ("ask", 50003.0) in sides


def test_compare_to_rest_no_drift_when_identical():
    b = OrderBookV2("BTCUSDT")
    snap = _snapshot(100)
    b.restore_from_snapshot(snap)
    assert b.compare_to_rest(snap) == []


def test_compare_to_rest_detects_qty_mismatch():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    drifted = _snapshot(100)
    drifted["bids"][0] = ["50000", "9.99"]
    findings = b.compare_to_rest(drifted)
    kinds = {(f.kind, f.side, f.price) for f in findings}
    assert ("qty_mismatch", "bid", 50000.0) in kinds


def test_compare_to_rest_detects_missing_levels():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    # REST lost one of our bid levels, and has an extra ask level
    rest = {
        "lastUpdateId": 100,
        "bids": [["50000", "1.0"], ["49999", "2.0"]],  # 49998 dropped
        "asks": [["50001", "1.5"], ["50002", "2.5"], ["50003", "3.5"], ["50004", "4.5"]],
    }
    findings = b.compare_to_rest(rest)
    kinds = {(f.kind, f.side, f.price) for f in findings}
    assert ("missing_rest", "bid", 49998.0) in kinds
    assert ("missing_local", "ask", 50004.0) in kinds


def test_compare_max_levels_ignores_deeper_rest():
    b = OrderBookV2("BTCUSDT")
    b.restore_from_snapshot(_snapshot(100))
    # REST has many more levels than our maintained book but the top-3 of
    # each side match exactly. max_price_levels=3 caps the compare window.
    local_top = _snapshot(100)
    extra_bids = [[str(49990 - i), "9.0"] for i in range(50)]
    extra_asks = [[str(50010 + i), "9.0"] for i in range(50)]
    rest = {
        "lastUpdateId": 100,
        "bids": local_top["bids"] + extra_bids,
        "asks": local_top["asks"] + extra_asks,
    }
    findings = b.compare_to_rest(rest, max_price_levels=3)
    assert findings == []
