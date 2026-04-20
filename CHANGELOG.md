# Changelog

## 0.1.0 — 2026-04-20

Initial Chronos release. Forked from internal scalper-bot branch
`recorder-v2-enterprise` (commit `66dadd9`) into standalone product repo.

### Shipped (14 of 16 Tardis-parity spec points)

- **P1** triple timestamps: `local_ts_us`, `exchange_event_ts_us`,
  `exchange_trans_ts_us` (microseconds, per-source extractors).
- **P2** raw WS JSONL.gz archive, hourly-rotated, crash-safe via
  concatenated gzip members + `Z_SYNC_FLUSH` on drain.
- **P3** integrity validators: Binance depth `pu/u` sequence +
  `aggTrade.a` monotonic id. Gaps emitted as first-class
  `INTEGRITY_GAP` stream with `gap_kind/expected/observed/skip_count`.
- **P4** host clock tuning (operational): chrony + SCHED_FIFO + 3×Google
  anycast + Cloudflare NTS + NIST, typical 40 µs offset, last-poll
  sub-50 µs, skew 2 ppm.
- **P5** parquet schema metadata: 12 embedded fields per file
  (`recorder_version`, `schema_version`, `schema_id`, `source_id`,
  `source_api_version`, `exchange`, `symbol`, `stream_type`, `hour_key`,
  `host_id`, `recorded_at`, `producer`).
- **P6** L100 maintained orderbook with `depth_snapshot` stream emitted
  at every diff (100 ms cadence matching Binance).
- **P7** classified `depth_diff` (`insert` / `update` / `remove`)
  derived from book state, replacing stateless qty=0 collapse.
- **P9** LRU dedupe cache for multi-endpoint WS subscriptions (used by
  Binance depth primary+fallback).
- **P10** 5-min `book_checkpoint` — every maintained level serialized
  for fast replay origin.
- **P11** REST reconciliation: periodic `/fapi/v1/depth?limit=1000`
  comparison against maintained book; drift → `INTEGRITY_GAP` rows +
  automatic reseed.
- **P12** offline trade↔depth correlator (`scripts/correlate_trade_depth.py`)
  matching trade prints to corresponding depth qty changes in a
  configurable µs window.
- **P13** Binance `@forceOrder` liquidation stream.
- **P14** funding settlement detector — emits realized-rate rows when
  mark-price `T` rolls over.
- **P15** Coinbase Advanced Trade (ISO-8601 timestamps) + Deribit v2
  with authenticated `public/auth` → `.raw` subscriptions; unauth
  fallback to `.100ms`.
- **P16** consumer reader — `discover_streams`, `read_stream`,
  `unified_trades`, `summarize_metadata`, `iter_raw_jsonl`.

### Out of scope

- **P8** redundancy with second host (requires additional infrastructure).
- **P15 dYdX** (gRPC client stack; WS-only path being evaluated).

### Tests

95/95 passing. Coverage:
- timestamps (9 cases across 8 sources + malformed input)
- raw archive round-trip + hour rollover + concatenated gzip recovery
- parquet metadata contract + compaction
- normalization per venue (is_buyer_maker uniformity, classifier correctness)
- integrity validators (state transitions, reset, boundary)
- funding detector (T rollover, regress ignored, missing last rate)
- book state (apply, classify, top-N, checkpoint, REST reconcile)
- end-to-end: depth+snapshot+diff+checkpoint+integrity+funding through
  `Recorder`
- dedupe (LRU + recency)
- Coinbase + Deribit normalizers
- Deribit auth credentials redaction + interval resolution
- reader helpers (discovery, filters, metadata validation)
- correlator (matched / unmatched sample classification)

### Live verification

Two 60-75 s production runs against live WS + REST on Tokyo host:
- All 7 venues subscribed; zero errors on second run after two-bug fix.
- Binance BTCUSDT: 42,380 classified depth_diff + 724 L100 snapshots +
  2,832 checkpoint rows in 75 s.
- Deribit `.raw` via authenticated session: 29 trades in 60 s,
  credential material never written to parquet / archive / logs.
