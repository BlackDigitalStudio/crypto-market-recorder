"""P5: parquet metadata round-trip + compaction."""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from chronos.parquet_writer import (
    ParquetWriter,
    REQUIRED_METADATA_KEYS,
    parse_metadata,
    read_file_metadata,
    validate_metadata,
)
from chronos.schemas import schema_for
from chronos.version import (
    HOST_ID,
    RECORDER_VERSION,
    SCHEMA_VERSIONS,
    SOURCE_API_VERSIONS,
    StreamType,
)


def _make_trade_table(n: int = 3) -> pa.Table:
    schema = schema_for(StreamType.TRADE)
    cols = {
        "local_ts_us": pa.array([1_700_000_000_000_000 + i for i in range(n)], type=pa.int64()),
        "exchange_event_ts_us": pa.array([1_700_000_000_000_000 + i for i in range(n)], type=pa.int64()),
        "exchange_trans_ts_us": pa.array([1_700_000_000_000_000 + i for i in range(n)], type=pa.int64()),
        "source_id": pa.array(["binance_futures_ws"] * n, type=pa.string()),
        "symbol": pa.array(["BTCUSDT"] * n, type=pa.string()),
        "trade_id": pa.array([str(i) for i in range(n)], type=pa.string()),
        "price": pa.array([50000.0 + i for i in range(n)], type=pa.float64()),
        "qty": pa.array([0.1] * n, type=pa.float64()),
        "is_buyer_maker": pa.array([False] * n, type=pa.bool_()),
        "first_trade_id": pa.array([None] * n, type=pa.int64()),
        "last_trade_id": pa.array([None] * n, type=pa.int64()),
    }
    return pa.table(cols, schema=schema)


def test_write_part_stamps_all_required_metadata_keys(tmp_path: Path):
    w = ParquetWriter(tmp_path)
    path = w.write_part(
        table=_make_trade_table(),
        stream_rel_dir=Path("binance_futures/BTCUSDT/trade"),
        hour_key="20260419_10",
        stream_type=StreamType.TRADE,
        source_id="binance_futures_ws",
        exchange="binance_futures",
        symbol="BTCUSDT",
    )
    assert path.exists()

    meta = read_file_metadata(path)
    missing = validate_metadata(meta)
    assert missing == [], f"missing metadata keys: {missing}"

    # Spot-check values
    assert meta["recorder_version"] == RECORDER_VERSION
    assert meta["schema_version"] == SCHEMA_VERSIONS[StreamType.TRADE]
    assert meta["schema_id"] == f"trade:{SCHEMA_VERSIONS[StreamType.TRADE]}"
    assert meta["source_id"] == "binance_futures_ws"
    assert meta["source_api_version"] == SOURCE_API_VERSIONS["binance_futures_ws"]
    assert meta["exchange"] == "binance_futures"
    assert meta["symbol"] == "BTCUSDT"
    assert meta["stream_type"] == "trade"
    assert meta["hour_key"] == "20260419_10"
    assert meta["host_id"] == HOST_ID
    assert meta["producer"] == "chronos"


def test_compact_hour_merges_parts_and_preserves_metadata(tmp_path: Path):
    w = ParquetWriter(tmp_path)
    rel = Path("binance_futures/BTCUSDT/trade")
    hour = "20260419_11"
    w.write_part(
        table=_make_trade_table(2),
        stream_rel_dir=rel, hour_key=hour,
        stream_type=StreamType.TRADE, source_id="binance_futures_ws",
        exchange="binance_futures", symbol="BTCUSDT",
    )
    w.write_part(
        table=_make_trade_table(3),
        stream_rel_dir=rel, hour_key=hour,
        stream_type=StreamType.TRADE, source_id="binance_futures_ws",
        exchange="binance_futures", symbol="BTCUSDT",
    )

    canonical = w.compact_hour(
        stream_rel_dir=rel, hour_key=hour,
        stream_type=StreamType.TRADE, source_id="binance_futures_ws",
        exchange="binance_futures", symbol="BTCUSDT",
    )
    assert canonical is not None and canonical.exists()
    # Parts gone
    parts = list((tmp_path / rel / ".parts").glob(f"{hour}_*.parquet"))
    assert parts == []
    # Canonical has 5 rows and full metadata
    table = pq.read_table(canonical)
    assert table.num_rows == 5
    meta = parse_metadata(table.schema.metadata)
    assert meta["hour_key"] == hour
    assert meta["compacted_parts"] == "2"
    assert all(k in meta for k in REQUIRED_METADATA_KEYS)


def test_validate_metadata_detects_missing():
    partial = {"recorder_version": "x", "schema_version": "y"}
    missing = validate_metadata(partial)
    assert "source_id" in missing and "host_id" in missing


def test_all_stream_types_have_schema_version():
    for st in StreamType:
        assert st in SCHEMA_VERSIONS, f"no schema version for {st}"
