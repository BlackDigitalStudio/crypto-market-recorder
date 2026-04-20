"""Arrow schemas for every stream type.

All schemas share a mandatory triple-timestamp prefix (P1). Schema version
strings live in :data:`.version.SCHEMA_VERSIONS` and are embedded in parquet
file metadata (P5).

The schemas are deliberately **source-agnostic** — a `trade` row looks the
same whether it came from Binance, Bybit or OKX. The originating source is
carried in parquet file metadata plus a per-row `source_id` column so
consumers can merge across venues without ambiguity.
"""
from __future__ import annotations

import pyarrow as pa

from .version import StreamType

# Shared preamble: three timestamps + where the event came from.
#
# Kept as a helper so schema additions down the line (e.g. adding a gap-id
# in P3) stay uniform across streams.
def _preamble() -> list[pa.Field]:
    return [
        pa.field("local_ts_us", pa.int64(), nullable=False),
        pa.field("exchange_event_ts_us", pa.int64(), nullable=True),
        pa.field("exchange_trans_ts_us", pa.int64(), nullable=True),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
    ]


# Depth snapshot with fixed-size L levels. For universality the recorder can
# be configured to any N; the default layouts expose L20 (legacy parity) and
# L100 (full Tardis-parity, land in P6). The schema is a *template* —
# build_depth_snapshot_schema(levels) returns the concrete schema.
def build_depth_snapshot_schema(levels: int) -> pa.Schema:
    fsl = lambda: pa.list_(pa.float64(), levels)
    return pa.schema([
        *_preamble(),
        pa.field("depth_levels", pa.int32(), nullable=False),
        pa.field("first_update_id", pa.int64(), nullable=True),
        pa.field("final_update_id", pa.int64(), nullable=True),
        pa.field("prev_final_update_id", pa.int64(), nullable=True),
        pa.field("bid_prices", fsl(), nullable=False),
        pa.field("bid_qtys", fsl(), nullable=False),
        pa.field("ask_prices", fsl(), nullable=False),
        pa.field("ask_qtys", fsl(), nullable=False),
    ])


DEPTH_DIFF_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("first_update_id", pa.int64(), nullable=True),
    pa.field("final_update_id", pa.int64(), nullable=True),
    pa.field("prev_final_update_id", pa.int64(), nullable=True),
    # "bid" | "ask"
    pa.field("side", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    # qty = 0 means "remove level". Binance signals removal with qty=0; we
    # keep it explicit as update_type to stay consistent with Tardis.
    pa.field("qty", pa.float64(), nullable=False),
    # "insert" | "update" | "remove"
    pa.field("update_type", pa.string(), nullable=False),
])


TRADE_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("trade_id", pa.string(), nullable=True),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
    # True when buy-side was passive (maker). Aligns with Binance `m`.
    pa.field("is_buyer_maker", pa.bool_(), nullable=False),
    # For exchanges that expose an aggregate id window (Binance aggTrade
    # `f`/`l`), carry it. Null on venues that don't.
    pa.field("first_trade_id", pa.int64(), nullable=True),
    pa.field("last_trade_id", pa.int64(), nullable=True),
])


MARK_PRICE_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("mark_price", pa.float64(), nullable=False),
    pa.field("index_price", pa.float64(), nullable=True),
    pa.field("estimated_settle_price", pa.float64(), nullable=True),
    pa.field("funding_rate", pa.float64(), nullable=True),
    pa.field("next_funding_time_ms", pa.int64(), nullable=True),
])


FUNDING_SETTLEMENT_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("realized_funding_rate", pa.float64(), nullable=False),
    pa.field("settled_at_ms", pa.int64(), nullable=False),
])


LIQUIDATION_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("side", pa.string(), nullable=False),  # "BUY" | "SELL"
    pa.field("order_type", pa.string(), nullable=True),
    pa.field("time_in_force", pa.string(), nullable=True),
    pa.field("original_qty", pa.float64(), nullable=True),
    pa.field("price", pa.float64(), nullable=True),
    pa.field("avg_price", pa.float64(), nullable=True),
    pa.field("order_status", pa.string(), nullable=True),
    pa.field("last_filled_qty", pa.float64(), nullable=True),
    pa.field("filled_accumulated_qty", pa.float64(), nullable=True),
])


DERIVATIVES_POLL_SCHEMA = pa.schema([
    *_preamble(),
    pa.field("open_interest", pa.float64(), nullable=True),
    pa.field("long_short_ratio", pa.float64(), nullable=True),
    pa.field("long_account", pa.float64(), nullable=True),
    pa.field("short_account", pa.float64(), nullable=True),
    pa.field("period", pa.string(), nullable=True),
])


BOOK_CHECKPOINT_SCHEMA = pa.schema([
    *_preamble(),
    # All rows from one dump share this id (typically ``local_ts_us`` at
    # dump start) so consumers can re-assemble a full state snapshot.
    pa.field("dump_id", pa.int64(), nullable=False),
    pa.field("last_update_id", pa.int64(), nullable=True),
    pa.field("side", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
])


INTEGRITY_GAP_SCHEMA = pa.schema([
    *_preamble(),
    # Which sibling stream produced this gap event.
    pa.field("monitored_stream_type", pa.string(), nullable=False),
    pa.field("monitored_channel", pa.string(), nullable=False),
    # e.g. "depth_sequence_pu_mismatch", "aggtrade_id_regress",
    # "aggtrade_id_skip", "trade_id_regress", "clock_regress".
    pa.field("gap_kind", pa.string(), nullable=False),
    pa.field("expected", pa.string(), nullable=True),
    pa.field("observed", pa.string(), nullable=True),
    pa.field("previous_value", pa.int64(), nullable=True),
    pa.field("current_value", pa.int64(), nullable=True),
    # Number of items skipped / magnitude of the anomaly when meaningful.
    pa.field("skip_count", pa.int64(), nullable=True),
    pa.field("note", pa.string(), nullable=True),
])


def schema_for(stream_type: StreamType, *, depth_levels: int | None = None) -> pa.Schema:
    if stream_type is StreamType.DEPTH_SNAPSHOT:
        if depth_levels is None:
            raise ValueError("depth_snapshot requires depth_levels")
        return build_depth_snapshot_schema(depth_levels)
    if stream_type is StreamType.DEPTH_DIFF:
        return DEPTH_DIFF_SCHEMA
    if stream_type is StreamType.TRADE:
        return TRADE_SCHEMA
    if stream_type is StreamType.MARK_PRICE:
        return MARK_PRICE_SCHEMA
    if stream_type is StreamType.FUNDING_SETTLEMENT:
        return FUNDING_SETTLEMENT_SCHEMA
    if stream_type is StreamType.LIQUIDATION:
        return LIQUIDATION_SCHEMA
    if stream_type is StreamType.DERIVATIVES_POLL:
        return DERIVATIVES_POLL_SCHEMA
    if stream_type is StreamType.INTEGRITY_GAP:
        return INTEGRITY_GAP_SCHEMA
    if stream_type is StreamType.BOOK_CHECKPOINT:
        return BOOK_CHECKPOINT_SCHEMA
    raise ValueError(f"unknown stream type: {stream_type!r}")
