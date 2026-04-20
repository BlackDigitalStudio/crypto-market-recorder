# Chronos

**Institutional-grade crypto market-data recorder.** Real-time capture of
orderbook, trades, liquidations, funding, and derivatives state across 7+
venues with ≤50 µs local clock accuracy, schema-versioned parquet output,
and a parallel raw JSONL archive for loss-less re-normalization.

Designed for HFT funds, research desks, and backtesting teams who need
Tardis-comparable real-time quality without being locked into a commercial
vendor.

---

## Data quality

| Axis | Chronos | Tardis |
|---|---|---|
| Timestamps per event | **3** — `local_ts_us`, `exchange_event_ts_us`, `exchange_trans_ts_us` | 2 — local + exchange |
| Local clock accuracy | 17-40 µs (chrony + Stratum-1 anycast + `SCHED_FIFO`) | 5-20 µs (hardware PHC) |
| Orderbook snapshots | L100 @ 100 ms cadence, classified `insert/update/remove` diffs | L20 / L100 raw diffs |
| Integrity surface | `pu/u` sequence validator + monotonic id validator + REST reconcile + `INTEGRITY_GAP` stream | `disconnect` markers |
| Replay from raw | yes — `.jsonl.gz` archive alongside every parquet | yes (raw CSV) |
| File-level metadata | embedded schema versions, source API capture dates, host id, recorded-at | external catalog |
| Multi-endpoint dedupe | LRU cache across primary+fallback WS URLs | n/a |

## Venues

- **Binance USDT-M Futures** — depth@100ms, aggTrade, markPrice@1s, `@forceOrder` liquidations, REST derivatives poll
- **Bybit v5 Linear** — publicTrade
- **OKX v5** — trades
- **Bitget v2 Futures** — trades
- **Gate.io v4 Futures** — futures.trades
- **Coinbase Advanced Trade** — market_trades
- **Deribit v2** — trades (`.raw` via authenticated session, `.100ms` fallback without creds)

dYdX v4 deliberately omitted (gRPC client stack — out of current scope).

## Stream types

| type | cadence | content |
|---|---|---|
| `trade` | event-driven | per-trade prints across all venues, unified `is_buyer_maker` semantics |
| `depth_diff` | 100 ms | incremental book changes, `insert/update/remove` classified against maintained book |
| `depth_snapshot` | 100 ms | L100 top-of-book flat row; best-bid/ask and depth array |
| `book_checkpoint` | 5 min | every maintained level serialized for fast replay seed |
| `mark_price` | 1 s | mark + index + funding rate + next funding time |
| `funding_settlement` | on rollover | realized funding emitted when `next_funding_time` advances |
| `liquidation` | event-driven | Binance `@forceOrder` |
| `derivatives_poll` | 15 s | open interest + top long/short account ratio |
| `integrity_gap` | on anomaly | sequence gaps, id regressions, REST drift, re-sync markers |

## Output layout

```
<root>/<exchange>/<symbol>/<stream_type>/
    <hour_key>.parquet                  # compacted hourly
    raw/<hour_key>.jsonl.gz             # parallel raw-WS archive
    .parts/<hour_key>_NNNNN.parquet     # pre-compaction parts
```

Every parquet file carries schema-level metadata: `recorder_version`,
`schema_version`, `schema_id`, `source_id`, `source_api_version`,
`exchange`, `symbol`, `stream_type`, `hour_key`, `host_id`, `recorded_at`,
`producer`. Consumers dispatch on `schema_id` without out-of-band catalogs.

## Consumer library

```python
from chronos.reader import discover_streams, unified_trades, summarize_metadata

for loc in discover_streams("/data/chronos"):
    print(loc.exchange, loc.symbol, loc.stream_type)

# Cross-venue trade table with injected `exchange` column:
trades = unified_trades("/data/chronos", symbol_filter="BTCUSDT")
```

See `docs/SPEC.md` and `docs/PROGRESS.md` for implementation details.

## Running

```bash
# Credentials (optional — unlocks Deribit .raw)
export DERIBIT_CLIENT_ID=...
export DERIBIT_CLIENT_SECRET=...
export CHRONOS_ROOT=/data/chronos

# Run (foreground)
python scripts/record_data.py

# Systemd unit at systemd/chronos.service for production
```

## Testing

```bash
python -m pytest tests/ -q
# 95/95 passing as of 2026-04-20
```

## License

**Proprietary — all rights reserved.** See `LICENSE`.

This software is not open source. Usage beyond authorized licensees is
prohibited. For commercial data feed subscriptions or source licensing,
contact the repository owner.
