"""P12 — trade ↔ depth correlation post-processor over the v2 archive.

For each aggregated trade observed at ``(exchange, symbol)`` the
companion maintained book (depth_diff stream) SHOULD show a qty change
at the traded price within a narrow window around ``exchange_trans_ts``.
Absence of a corresponding depth change is a cross-stream integrity
signal — usually a dropped depth diff, occasionally a logic bug in the
recorder or the exchange.

This runs offline against the archive (no live traffic, no infra) and
emits a report and/or ``INTEGRITY_GAP``-style row dump.

Usage::

    python scripts/correlate_trade_depth_v2.py \
        --root /home/scalper/scalper-bot/data_v2 \
        --exchange binance_futures --symbol BTCUSDT \
        --hour-start 2026041910 --hour-end 2026041912 \
        --window-ms 150

The window is symmetric around the trade's ``exchange_trans_ts``.
Default 100ms matches Binance futures depth cadence.

Output summary: total trades, matched, unmatched, mismatch rate.
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pyarrow as pa

# Allow running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chronos.reader import (  # noqa: E402
    StreamLocation, discover_streams, read_stream,
)
from chronos.version import StreamType  # noqa: E402

logger = logging.getLogger("correlate_trade_depth_v2")


def _find(
    root: Path, exchange: str, symbol: str, stream_type: StreamType,
) -> StreamLocation | None:
    for loc in discover_streams(root):
        if loc.exchange == exchange and loc.symbol == symbol and loc.stream_type is stream_type:
            return loc
    return None


def _hour_from_arg(v: str | None) -> datetime | None:
    if v is None:
        return None
    # YYYYMMDDHH
    if len(v) != 10:
        raise SystemExit(f"--hour-start / --hour-end must be YYYYMMDDHH: {v!r}")
    return datetime.strptime(v, "%Y%m%d%H")


def _build_depth_price_index(
    depth_table: pa.Table,
) -> dict[float, list[tuple[int, float]]]:
    """price → sorted list of (trans_ts_us, qty) events.

    We only index by price; finding matches is then a binary search by
    time within a per-price list. Memory usage is O(rows) — fine for
    per-hour batches (≈ 100k rows).
    """
    cols = depth_table.to_pylist()
    idx: dict[float, list[tuple[int, float]]] = defaultdict(list)
    for r in cols:
        trans = r.get("exchange_trans_ts_us") or r.get("local_ts_us")
        if trans is None:
            continue
        price = r.get("price")
        qty = r.get("qty")
        if price is None or qty is None:
            continue
        idx[float(price)].append((int(trans), float(qty)))
    for price in idx:
        idx[price].sort()
    return idx


def _binary_search_range(
    events: list[tuple[int, float]], lo: int, hi: int,
) -> list[tuple[int, float]]:
    import bisect
    left = bisect.bisect_left(events, (lo,))
    right = bisect.bisect_right(events, (hi, float("inf")))
    return events[left:right]


def correlate(
    root: Path,
    exchange: str,
    symbol: str,
    *,
    start: datetime | None,
    end: datetime | None,
    window_us: int,
) -> dict:
    trade_loc = _find(root, exchange, symbol, StreamType.TRADE)
    depth_loc = _find(root, exchange, symbol, StreamType.DEPTH_DIFF)
    if trade_loc is None:
        raise SystemExit(f"no TRADE stream for {exchange}/{symbol}")
    if depth_loc is None:
        raise SystemExit(f"no DEPTH_DIFF stream for {exchange}/{symbol}")

    trades = read_stream(trade_loc, start=start, end=end)
    depth = read_stream(depth_loc, start=start, end=end)
    logger.info(
        "loaded trades=%d depth_changes=%d", trades.num_rows, depth.num_rows,
    )
    if trades.num_rows == 0:
        return {"trades": 0, "matched": 0, "unmatched": 0, "match_rate": 1.0}

    depth_idx = _build_depth_price_index(depth)

    matched = 0
    unmatched_samples: list[dict] = []
    for r in trades.to_pylist():
        price = r.get("price")
        trans = r.get("exchange_trans_ts_us") or r.get("local_ts_us")
        if price is None or trans is None:
            continue
        events = depth_idx.get(float(price))
        if not events:
            unmatched_samples.append({"price": price, "trans_us": trans, "reason": "no_price"})
            continue
        window = _binary_search_range(events, trans - window_us, trans + window_us)
        if window:
            matched += 1
        else:
            if len(unmatched_samples) < 20:
                unmatched_samples.append({
                    "price": price, "trans_us": trans, "reason": "no_event_in_window",
                })
    total = trades.num_rows
    unmatched = total - matched
    return {
        "trades": total,
        "matched": matched,
        "unmatched": unmatched,
        "match_rate": matched / total if total else 1.0,
        "unmatched_samples": unmatched_samples,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, type=Path)
    ap.add_argument("--exchange", default="binance_futures")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--hour-start", default=None)
    ap.add_argument("--hour-end", default=None)
    ap.add_argument("--window-ms", type=int, default=100)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    summary = correlate(
        args.root, args.exchange, args.symbol,
        start=_hour_from_arg(args.hour_start),
        end=_hour_from_arg(args.hour_end),
        window_us=args.window_ms * 1000,
    )
    logger.info("=== trade↔depth correlation ===")
    logger.info(
        "trades=%d matched=%d unmatched=%d rate=%.4f",
        summary["trades"], summary["matched"], summary["unmatched"],
        summary["match_rate"],
    )
    if summary.get("unmatched_samples"):
        logger.info("first unmatched samples (up to 20):")
        for s in summary["unmatched_samples"][:20]:
            logger.info("  %s", s)


if __name__ == "__main__":
    main()
