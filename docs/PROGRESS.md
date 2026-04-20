# Recorder v2 — progress ledger

Branch: `recorder-v2-enterprise` (local only, not pushed).

Production recorder (`scripts/record_data.py` + `src/ws_client.py` +
`src/recorder.py`) is **untouched** and keeps running as the systemd unit
target. All v2 code is under `recorder/` + `tests/chronos/`.

## 2026-04-19 — P1 + P2 + P5 landed

### What shipped

**P1 — triple timestamps** (`recorder/timestamps.py`)
- `Timestamps(local_ts_us, exchange_event_ts_us, exchange_trans_ts_us)` in
  microseconds since epoch.
- Per-source extractors for Binance (depthUpdate, aggTrade, markPriceUpdate,
  forceOrder), Bybit v5 publicTrade, OKX trades, Bitget trade, Gate.io
  futures.trades, Binance REST polls.
- `markPrice` explicitly stamps `trans=None` because upstream `T` there is
  next-funding-time, not a transaction time.

**P2 — raw WS JSON archive** (`recorder/raw_archive.py`)
- Hourly-rotated `.jsonl.gz` at
  `<root>/<exchange>/<symbol>/<stream_type>/raw/<hour_key>.jsonl.gz`.
- One JSON line per frame with the full triple-timestamp and
  `source_id/channel` stamped alongside the verbatim message.
- Append-mode gzip with `Z_SYNC_FLUSH` on drain → crash-safe; concatenated
  gzip members decompress as one continuous stream.
- Thread-safe across stream keys.

**P5 — parquet schema metadata** (`recorder/parquet_writer.py`)
- Every parts file and every compacted canonical file carries the full
  preamble in parquet schema metadata:
  `recorder_version / schema_version / schema_id / source_id /
  source_api_version / exchange / symbol / stream_type / hour_key /
  host_id / recorded_at / producer`.
- `validate_metadata()` + `REQUIRED_METADATA_KEYS` for consumer-side
  contract checks.
- `zstd` compression (smaller than legacy snappy at same CPU).

### Supporting modules

- `version.py` — `StreamType` enum + `SCHEMA_VERSIONS` + `SOURCE_API_VERSIONS`
  (capture-date markers per exchange).
- `schemas.py` — Arrow schemas for every stream type, all share the 5-field
  preamble (`local_ts_us, exchange_event_ts_us, exchange_trans_ts_us,
  source_id, symbol`).
- `normalize.py` — per-source row normalizers, all producing canonical
  `is_buyer_maker` semantics cross-venue.
- `recorder.py` — `Recorder` orchestrator: register streams, record raw
  frames, periodic flush, hourly compaction, crash-safe part recovery.

### Tests (28 / 28 passing)

- `tests/chronos/test_timestamps.py` — 9 cases across all sources,
  including malformed input & unknown source fallback.
- `tests/chronos/test_raw_archive.py` — 4 cases: round-trip,
  hour rollover, concatenated gzip members, pre-close flush.
- `tests/chronos/test_parquet_writer.py` — 4 cases: required metadata,
  compaction, validation, schema-version coverage.
- `tests/chronos/test_normalize.py` — 8 cases: per-level depth diff,
  is_buyer_maker semantics per venue, Gate.io signed-size, force-order
  nested `o`, round-trip against the Arrow schema.
- `tests/chronos/test_recorder_end_to_end.py` — 3 cases: single-stream
  parquet+raw, multi-row depth diff, multi-venue coexistence.

Run: `venv/bin/python -m pytest tests/chronos/ -q`.

## 2026-04-19 — P3 + P14 landed

### What shipped

**P3 — integrity validators** (`recorder/integrity.py` +
`StreamType.INTEGRITY_GAP`)
- `DepthSequenceValidator` checks Binance futures `pu == prev.u` (hard
  invariant). Also surfaces soft `U == prev.u + 1` skips when `pu` is
  absent.
- `MonotonicIdValidator` tracks a monotonically increasing integer id
  (Binance `aggTrade.a`, reusable for any strictly-incrementing field).
  Emits `_regress` on id<=prev and `_skip` on id>prev+threshold.
- `default_validators_for(source_id, channel)` auto-picks the right set;
  currently covers Binance `depthUpdate` and `aggTrade`. Other venues
  land alongside P15 per-exchange quirks.
- Gaps are written to a per-`(source, exchange, symbol)` integrity
  stream (`integrity_gap` directory). Rows carry `monitored_stream_type`
  + `monitored_channel` so a single integrity feed covers all data
  streams for that venue.
- Re-snapshot triggering is deliberately out of the archive path —
  gateway layer decides to pull REST in response; the archive only
  records the event.

**P14 — funding settlement detector** (`recorder/funding.py` +
`StreamType.FUNDING_SETTLEMENT`)
- `FundingSettlementDetector` observes normalized mark-price rows.
  When `next_funding_time_ms` advances, emits one settlement row with
  the pre-rollover rate as `realized_funding_rate` and the prior `T`
  as `settled_at_ms`.
- Auto-attached when a `MARK_PRICE` stream is registered (toggle via
  `enable_funding_settlement=False`).

### Tests (51 / 51 passing)

- `tests/chronos/test_integrity.py` — 12 cases on validator state
  transitions (contiguous, mismatch, skip, regress, reset).
- `tests/chronos/test_funding_detector.py` — 6 cases on settlement
  emission (first-sample no-op, same-T no-op, `T` advance, regress
  ignored, reset, missing prior rate fallback).
- `tests/chronos/test_recorder_p3_p14.py` — 5 end-to-end cases
  wiring both into `Recorder` + verifying parquet metadata.

## 2026-04-19 — P6 + P7 + P10 + P11 landed (maintained-book cluster)

### What shipped

**`OrderBookV2`** (`recorder/order_book.py`) — pure state container:
- `restore_from_snapshot(rest_body)` — init from Binance-style REST body.
- `apply_diff(msg)` — returns `list[BookChange]` with classified
  `insert` / `update` / `remove` per level. Stale diffs (`u <= last`)
  silently dropped; duplicate qty emits no change.
- `top_n(n)` / `snapshot_row(levels=..)` — build a DEPTH_SNAPSHOT row.
- `checkpoint_rows(...)` — one row per maintained level for a P10 dump.
- `compare_to_rest(rest, qty_tolerance, max_price_levels)` — returns
  `list[DriftFinding]` for P11 reconcile.

**P6 depth_snapshot** — `Recorder.register_maintained_book(...)` auto-
registers a sibling `DEPTH_SNAPSHOT` stream (L=`snapshot_levels`,
default 100). A row is emitted every `snapshot_every_n_diffs` applied
diff (default 1 → matches Binance `@100ms` cadence). An initial row is
also emitted on `ingest_rest_snapshot` so consumers see the seed state
before the first diff.

**P7 classified `depth_diff`** — when a book is attached, the stateless
depth-diff normalizer is bypassed and rows carry true
`insert` / `update` / `remove` classification derived from book state
(previously every non-zero qty collapsed to `"update"`). Unsynced-book
diffs are dropped from parquet but the raw `.jsonl.gz` archive retains
them verbatim.

**P10 book_checkpoint** — a periodic (default 5 min) full-state dump of
every maintained level, one row per level, grouped by `dump_id`. Emitted
from the flush loop; interval=0 forces emission every tick (useful for
tests).

**P11 REST reconciliation** — `Recorder.reconcile_with_rest(key,
snapshot, *, qty_tolerance, max_price_levels)` compares the maintained
book against a freshly pulled REST snapshot and writes one
`INTEGRITY_GAP` row per drift finding (`gap_kind =
rest_depth_drift_qty_mismatch | _missing_local | _missing_rest`).
`max_price_levels` caps the compare window so deeper REST levels that
the maintained book doesn't track don't produce false positives.

### Integration surface

- `register_maintained_book(*, source_id, exchange, symbol,
  depth_diff_channel="depthUpdate", snapshot_levels=100,
  checkpoint_interval_sec=300.0, snapshot_every_n_diffs=1)` returns the
  book handle. Caller feeds REST via `ingest_rest_snapshot(diff_key,
  body)` and calls `reconcile_with_rest(diff_key, body)` on a schedule.
- Default P3 `DepthSequenceValidator` is auto-attached to the diff key.
- REST fetch itself is deliberately gateway-side (no HTTP in the
  archive path); any caller — live gateway or offline replayer — feeds
  the dict.

### Tests (67 / 67 passing, +16)

- `tests/chronos/test_order_book.py` — 12 cases: unsynced drops
  diffs, restore loads, insert/update/remove classification, dup
  filtering, stale-diff drop, remove-nonexistent no-op, top-n
  zero-padding, checkpoint shape, REST reconcile across identical /
  qty drift / level drift / max_price_levels capping.
- `tests/chronos/test_recorder_maintained_book.py` — 4 end-to-end
  cases: depth_snapshot + classified depth_diff, periodic checkpoint,
  REST reconcile drift gaps, unsynced-book drop + raw archive
  preservation.

## 2026-04-19 — P9 + P12 + P13 + P15 + P16 + gateway + entrypoint landed

### What shipped

**P9 — LRU dedupe cache** (`recorder/dedupe.py`)
- ``DedupeCache(maxsize)`` with ``check_and_mark(key) -> bool``.
  Thread-safe, bounded, LRU-evicting. Used by the gateway to dedupe
  Binance depth diffs across ``fstream`` and the fallback endpoint
  (key = ``(stream_key, u)``) and aggTrade across duplicates
  (key = ``(stream_key, a)``).

**P13 — liquidation feed**
- Gateway subscribes to ``<symbol>@forceOrder``; schema and normalizer
  were already in place. Liquidation rows land in
  ``<exchange>/<symbol>/liquidation/``.

**P15 — Coinbase + Deribit** (two additions; dYdX deferred — needs its
native gRPC client stack, out of scope here)
- Coinbase Advanced Trade ``market_trades`` with ISO-8601 timestamp
  parsing (both envelope ``timestamp`` and per-item ``time``).
- Deribit v2 JSON-RPC ``trades.<instrument>.raw`` including heartbeat
  protocol (``public/set_heartbeat`` + test-request response).
- Both venues expose the canonical TRADE schema with unified
  ``is_buyer_maker`` semantics.

**P16 — consumer reader** (`recorder/reader.py`)
- ``discover_streams(root)`` — list every recorded triple.
- ``read_stream(loc, start, end, validate_metadata)`` — read a single
  stream with hour-window filter + metadata contract check.
- ``unified_trades(root, symbol_filter, exchanges)`` — cross-venue
  trade table merge with an injected ``exchange`` column.
- ``iter_raw_jsonl(loc)`` — replay the raw archive for re-normalization.
- ``summarize_metadata(root)`` — archive-wide summary listing
  ``schema_id`` / ``source_id`` / file count per stream.

**REST client** (`recorder/rest_client.py`)
- ``BinanceFuturesREST`` narrow aiohttp-based client for
  ``/fapi/v1/depth``, ``/fapi/v1/openInterest`` and
  ``topLongShortAccountRatio``. No signing — archive path is
  public-data only.

**WS gateway** (`recorder/gateway.py`)
- Single class ``Gateway`` orchestrates all WS subscriptions + REST
  polling + maintained-book seeding and reconciliation.
- Auto-registers every stream with the recorder in ``add_*``; launches
  tasks in ``start()`` so the whole thing is declarative.
- Binance futures: depth@100ms (primary + fallback endpoint with P9
  dedupe), aggTrade (with id-dedupe), markPrice@1s, @forceOrder,
  REST derivatives poll, periodic REST reconcile (P11) → automatic
  reseed on drift.
- Bybit / OKX / Bitget / Gate.io trade handlers (reuses the venue
  quirks from the legacy recorder: Bitget plain-text ping, idle
  watchdogs, exponential reconnect backoff).
- Coinbase multi-product subscription, Deribit JSON-RPC heartbeat.

**P12 correlator** (`scripts/correlate_trade_depth_v2.py`)
- Offline post-processor. For each aggregated trade, searches the
  maintained-book depth_diff stream for a qty change at the same price
  within a ``±window_ms`` band around ``exchange_trans_ts``. Reports
  total / matched / unmatched / match rate + sample unmatched rows.
- Runs against any archive root — independent from the recorder
  process.

**Entrypoint** (`scripts/record_data_v2.py`)
- Full production-grade config out of the box: Binance BTCUSDT +
  ETHUSDT futures with maintained books, reconcile, dedupe; Bybit,
  OKX, Bitget, Gate.io trade feeds; Coinbase BTC-USD; Deribit
  BTC-PERPETUAL.
- Writes to ``$CHRONOS_ROOT`` (default
  ``/home/scalper/scalper-bot/data_v2``). Runs side-by-side with the
  production recorder — separate directory tree, separate health
  file, independent process.

### Tests (85 / 85 passing, +18)

- ``test_dedupe.py`` — 6 cases: first sight, second sight, distinct
  keys, LRU eviction, recency touch, clear.
- ``test_coinbase_deribit.py`` — 5 cases: ISO-8601 parsing, Coinbase
  trade rows, Deribit ms→us, Deribit rows, unknown-channel fallback.
- ``test_reader.py`` — 6 cases: discovery, single-stream read,
  cross-venue merge, exchange filter, metadata summary, empty root.
- ``test_correlator.py`` — end-to-end: 2 trades (one matched one
  unmatched) against a book-derived depth_diff stream.

### Out of scope

- **P4** chrony/PTP — operational.
- **P8** second recorder on a different host — infrastructure
  (requires a second VPS + merge tooling).
- **P15 dYdX** — needs a native gRPC client (not a drop-in handler).
- ``scripts/record_data_v2.py`` has not been run against live WS on
  the Contabo host yet; first live bring-up is gated on user approval
  (keeps production recorder unaffected).
