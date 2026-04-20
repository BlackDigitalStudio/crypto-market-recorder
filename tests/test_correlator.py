"""P12: trade↔depth correlator smoke test over a hand-built archive."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from chronos import Recorder, StreamKey, StreamType


# Import the script module (scripts/correlate_trade_depth_v2.py).
import importlib.util as _iu
import sys as _sys
_script_path = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "correlate_trade_depth.py"
)
_spec = _iu.spec_from_file_location("_correlator", str(_script_path))
assert _spec is not None and _spec.loader is not None
_correlator = _iu.module_from_spec(_spec)
_sys.modules.setdefault("_correlator", _correlator)
_spec.loader.exec_module(_correlator)
correlate = _correlator.correlate


def _run(coro):
    return asyncio.run(coro)


def _populate(tmp_path: Path) -> None:
    diff_key = StreamKey(
        "binance_futures_ws", "binance_futures", "BTCUSDT",
        StreamType.DEPTH_DIFF, "depthUpdate",
    )
    trade_key = StreamKey(
        "binance_futures_ws", "binance_futures", "BTCUSDT",
        StreamType.TRADE, "aggTrade",
    )

    async def _body() -> None:
        rec = Recorder(tmp_path, flush_interval_sec=0.05)
        # Book-attached depth so depth_diff rows carry real classified changes.
        rec.register_maintained_book(
            source_id="binance_futures_ws", exchange="binance_futures",
            symbol="BTCUSDT", snapshot_levels=5, checkpoint_interval_sec=3600,
        )
        rec.register(trade_key)
        rec.ingest_rest_snapshot(diff_key, {
            "lastUpdateId": 100,
            "bids": [["50000", "1.0"]],
            "asks": [["50001", "1.0"]],
        })
        await rec.start()
        # Depth change at 50000 around T=2000ms local-time (trans).
        rec.record(diff_key, {
            "U": 101, "u": 105, "pu": 100,
            "E": 2_000, "T": 2_000,
            "b": [["50000", "0.5"]], "a": [],
        })
        # Trade at 50000 at T=2050ms — within 100ms window → matched.
        rec.record(trade_key, {
            "a": 1, "p": "50000", "q": "0.5", "m": False,
            "E": 2_050, "T": 2_050, "f": 1, "l": 1,
        })
        # Trade at 49999 at T=2050ms — no depth change at that price → unmatched.
        rec.record(trade_key, {
            "a": 2, "p": "49999", "q": "0.1", "m": False,
            "E": 2_050, "T": 2_050, "f": 2, "l": 2,
        })
        await asyncio.sleep(0.2)
        await rec.stop()

    _run(_body())


def test_correlate_counts_matched_and_unmatched(tmp_path: Path):
    _populate(tmp_path)
    summary = correlate(
        tmp_path, "binance_futures", "BTCUSDT",
        start=None, end=None, window_us=100_000,  # 100 ms
    )
    assert summary["trades"] == 2
    assert summary["matched"] == 1
    assert summary["unmatched"] == 1
    assert 0.0 <= summary["match_rate"] <= 1.0
    assert summary["unmatched_samples"]
