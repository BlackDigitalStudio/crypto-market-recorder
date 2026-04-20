"""Chronos — institutional-grade crypto market-data recorder.

Public surface re-exports the full ingestion + consumer API. See
``docs/SPEC.md`` for the 16-point Tardis-parity spec and
``docs/PROGRESS.md`` for implementation status per point.

Primary entry points:

- :class:`Recorder` — in-process orchestrator; registers streams, buffers,
  writes parquet parts + raw archive, compacts hourly.
- :class:`Gateway` — WS/REST subscription manager; feeds a
  :class:`Recorder` across 7 venues (Binance futures, Bybit, OKX,
  Bitget, Gate.io, Coinbase, Deribit).
- :func:`discover_streams` / :func:`unified_trades` — consumer-side
  reader over an archive root.
"""
from __future__ import annotations

from .version import (
    RECORDER_VERSION,
    SCHEMA_VERSIONS,
    SOURCE_API_VERSIONS,
    HOST_ID,
    StreamType,
)
from .timestamps import Timestamps, extract_timestamps
from .raw_archive import RawArchiveWriter
from .parquet_writer import ParquetWriter
from .recorder import (
    Recorder,
    StreamKey,
    integrity_key_for,
    funding_settlement_key_for,
    depth_snapshot_key_for,
    book_checkpoint_key_for,
)
from .integrity import (
    Validator,
    GapRecord,
    DepthSequenceValidator,
    MonotonicIdValidator,
    default_validators_for,
)
from .funding import FundingSettlementDetector
from .order_book import OrderBookV2, BookChange, DriftFinding
from .gateway import DeribitCredentials, Gateway

__all__ = [
    "RECORDER_VERSION",
    "SCHEMA_VERSIONS",
    "SOURCE_API_VERSIONS",
    "HOST_ID",
    "StreamType",
    "Timestamps",
    "extract_timestamps",
    "RawArchiveWriter",
    "ParquetWriter",
    "Recorder",
    "StreamKey",
    "integrity_key_for",
    "funding_settlement_key_for",
    "Validator",
    "GapRecord",
    "DepthSequenceValidator",
    "MonotonicIdValidator",
    "default_validators_for",
    "FundingSettlementDetector",
    "depth_snapshot_key_for",
    "book_checkpoint_key_for",
    "OrderBookV2",
    "BookChange",
    "DriftFinding",
    "DeribitCredentials",
    "Gateway",
]
