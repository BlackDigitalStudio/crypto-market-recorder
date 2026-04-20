"""Version constants embedded in every parquet file we write (P5).

Bumping rules:
- `RECORDER_VERSION`: SemVer for the v2 codebase as a whole.
- `SCHEMA_VERSIONS[stream_type]`: bump when an Arrow schema for that stream
  type gains/loses/renames a column, or the semantic meaning of a column
  changes. Consumers read this from parquet file metadata and dispatch.
- `SOURCE_API_VERSIONS[source_id]`: human-readable snapshot of the upstream
  WS contract we parsed. Binance, Bybit, OKX etc. don't publish versioned
  WS APIs; this is a capture-date marker so downstream can reason about
  historical silent changes. Update when the exchange ships a breaking
  change you adapted to.
"""
from __future__ import annotations

import os
import socket
from enum import Enum

RECORDER_VERSION = "2.0.0-alpha1"


class StreamType(str, Enum):
    """Stream kinds the recorder knows how to store.

    The value is the directory name under `data_v2/<exchange>/<symbol>/`.
    """

    # Top-of-book L1..L_N snapshot per tick (flat FixedSizeList layout).
    DEPTH_SNAPSHOT = "depth_snapshot"
    # Incremental depth diff — one row per (side, price, new_qty).
    DEPTH_DIFF = "depth_diff"
    # Aggregated / public trade prints.
    TRADE = "trade"
    # Mark / funding / index snapshot (Binance markPrice@1s and siblings).
    MARK_PRICE = "mark_price"
    # Funding settlement events (detected from markPrice `T` rollover).
    FUNDING_SETTLEMENT = "funding_settlement"
    # Liquidation / forced-order prints (`@forceOrder`).
    LIQUIDATION = "liquidation"
    # Periodic derivatives REST poll: open_interest, long/short ratio.
    DERIVATIVES_POLL = "derivatives_poll"
    # Order-book full-state checkpoints (5-min periodic dump of maintained
    # book, used by replay tools to skip reconstruction from start of day).
    BOOK_CHECKPOINT = "book_checkpoint"
    # Per-stream integrity markers: sequence gaps, id regressions, clock
    # anomalies. One stream per (source_id, exchange, symbol) aggregates
    # gap events across every monitored sibling stream.
    INTEGRITY_GAP = "integrity_gap"


SCHEMA_VERSIONS: dict[StreamType, str] = {
    StreamType.DEPTH_SNAPSHOT: "v1",
    StreamType.DEPTH_DIFF: "v1",
    StreamType.TRADE: "v1",
    StreamType.MARK_PRICE: "v1",
    StreamType.FUNDING_SETTLEMENT: "v1",
    StreamType.LIQUIDATION: "v1",
    StreamType.DERIVATIVES_POLL: "v1",
    StreamType.BOOK_CHECKPOINT: "v1",
    StreamType.INTEGRITY_GAP: "v1",
}

# Capture-date markers. Update when the upstream contract visibly changes.
SOURCE_API_VERSIONS: dict[str, str] = {
    "binance_futures_ws": "2026-04-19",
    "binance_futures_rest": "2026-04-19",
    "bybit_v5_public_linear": "2026-04-19",
    "okx_v5_public": "2026-04-19",
    "bitget_v2_public": "2026-04-19",
    "gateio_v4_futures": "2026-04-19",
    "coinbase_advanced_trade_ws": "2026-04-19",
    "deribit_v2_public_ws": "2026-04-19",
}


def _detect_host_id() -> str:
    override = os.environ.get("RECORDER_HOST_ID")
    if override:
        return override
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


HOST_ID = _detect_host_id()
