# Phase 2 — Kernel-level timestamping, target σ ≤ 40 μs

**Status**: spec, not implemented. Assigned to next agent for full build-out.

**Goal**: Produce `estimated_true_event_ts_us` in every Chronos parquet row
with standard deviation ≤ 40 μs over a 1-hour live window, measured
empirically against Binance futures WebSocket streams from the Contabo
Tokyo host.

This is a production quality target for institutional market-data
consumers. It replaces the current Python-event-loop timestamp (σ
~1-200 ms) with a kernel-level recv timestamp corrected for measured
one-way network delay and our local clock offset.

No "simpler staging" path — this document assumes the full
implementation is the deliverable. The simpler aiohttp-only version
stays as the shipped fallback behind a feature flag so live traffic
never stops while Phase 2 comes online.

---

## 1. Current deployed state (you are starting from here)

**Repo**: `BlackDigitalStudio/crypto-market-recorder`  
**Branch**: `main`  
**Latest commit at start of Phase 2**: `22a3aa4`
(fix(recorder): semaphore-limit concurrent compactions)

**Running process** on Contabo host `root@84.247.154.229` (IP
84.247.154.229, hostname `vmi3230287`, Tokyo/Ebara, AS141995 Contabo
Asia):

- pid 880540 — `/home/scalper/scalper-bot/venv/bin/python
  /home/scalper/chronos/scripts/record_data.py`
- data root: `/home/scalper/chronos-data/`
- log: `/home/scalper/chronos/logs/live_run.log`
- config: `/home/scalper/chronos/config.env` (gitignored — holds
  `DERIBIT_CLIENT_ID` + `DERIBIT_CLIENT_SECRET`; do **not** echo or
  log these values)

**Tests**: 98/98 passing
(`PYTHONPATH=/home/scalper/chronos
/home/scalper/scalper-bot/venv/bin/python -m pytest tests/ -q`).

**Chrony**: configured per `/etc/chrony/sources.d/
institutional-stratum1.sources` with `SCHED_FIFO priority 50`,
reference `time1.google.com` Stratum 1. `chronyc tracking`
consistently reports `RMS offset ~200 μs`, `last offset 15-75 μs`,
`system time` within ±40 μs of NTP reference. This 30-40 μs floor is
our clock-sync accuracy — it is the single hardest thing to improve
without PTP hardware, so everything else must stay below it.

**Machine**: 16 vCPU AMD EPYC, 62 GB RAM, 581 GB SSD, Linux kernel
5.15+ (verify with `uname -r` on first action — `SO_TIMESTAMPNS_NEW`
requires ≥ 5.1). Python 3.12.

**Production recorder (different product)**: `scripts/record_data.py`
also exists in `/home/scalper/scalper-bot/`. This is **not** Chronos
— it is the scalper-bot internal recorder. Do not touch it. Its pid is
`515532` under user `scalper`. Our Chronos runs as root from
`/home/scalper/chronos/`.

---

## 2. Why this target

`estimated_true_event_ts_us` is the authoritative per-event time column
consumers of Chronos data will use. Today we publish three raw
timestamps per row (`local_ts_us`, `exchange_event_ts_us`,
`exchange_trans_ts_us`). None of these is what an institutional
backtest / research / compliance consumer actually wants:

- `local_ts_us` is polluted by Python event-loop jitter (σ 1-200 ms in
  steady state, 2 s at hour rollover).
- `exchange_event_ts_us` is Binance's own matching-engine timestamp
  with ms quantization and unknown absolute clock precision.
- `exchange_trans_ts_us` likewise.

The correction we apply:

```
estimated_true_event_ts_us
    = kernel_recv_ts_us             # when our kernel actually received the TCP segment
    − measured_one_way_delay_us     # running median of (RTT / 2) to Binance
    − binance_clock_offset_us       # long-running estimator of their clock bias vs ours
```

The last two terms are *estimators*, and their standard deviations
compose with the kernel timestamp precision under error propagation:

```
σ(estimated_true_event_ts_us) = √(σ_kernel² + σ_chrony² + σ_delay² + σ_binance_bias²)
```

Our target σ ≤ 40 μs is achievable because:
- σ_chrony ≈ 30 μs (already tuned — do not regress)
- σ_kernel ≈ 10-15 μs (from `SO_TIMESTAMPNS_NEW`)
- σ_delay ≈ 5 μs (from running median of ≥ 100 RTT samples)
- σ_binance_bias ≈ 10-15 μs (from long-window mean of
  `local_ts − E − one_way` on every event)

`√(30² + 15² + 5² + 15²) ≈ 36 μs`.

No software-only trick gets below ~30 μs without replacing chrony with
PHC (hardware PTP on NIC) or GPS PPS, which Contabo's guest VM does
not expose.

---

## 3. Architectural decisions (fixed — do not reopen without cause)

### 3.1 Scope — which streams get kernel timestamping

**All Binance futures streams** (depth@100ms, aggTrade, markPrice@1s,
@forceOrder). These are the highest-value premium feeds Chronos sells.

Other venues (Bybit, OKX, Bitget, Gate.io, Coinbase, Deribit) keep the
aiohttp path in this phase. Their physical network delay to Tokyo is
longer (10-250 ms) and their `exchange_event_ts_us` is either absent
(OKX, Gate.io) or ms-quantized (all), so a 30 μs `local_ts` stamping
infra delivers proportionally less edge there. Future phases can
extend to those venues once the Binance path is proven.

### 3.2 WebSocket + TLS stack

**Decision**: use `asyncio.open_connection(host, port, ssl=ctx)` to
obtain a `StreamReader` / `StreamWriter` pair with asyncio-integrated
TLS, then access the raw socket via `writer.get_extra_info('socket')`
for the `SO_TIMESTAMPNS_NEW` setsockopt and per-message
`sock.recvmsg()` calls driven from the reader's underlying transport.

**Rationale**:
- aiohttp cannot expose the raw socket's ancillary data.
- picows may expose `recv()` on a lower level but does not today
  surface kernel timestamps via its public API.
- Rewriting TLS in Python is a non-starter; asyncio's default SSL
  transport is battle-tested and gives us the same event-loop
  integration we rely on elsewhere.
- `get_extra_info('socket')` gives us the underlying `socket.socket`,
  on which `setsockopt` and `recvmsg` are valid.

### 3.3 WebSocket frame parser

Write a minimal RFC 6455 frame parser (opcodes 1=text, 2=binary,
8=close, 9=ping, 10=pong; masking bit; length 7 / 16 / 64; defragmentation
of FIN=0 continuations). ~150 LOC of table-driven struct unpacking.
Do not depend on an external library — we want full visibility and
zero extra buffering layers between `recvmsg` and the frame handler.

### 3.4 How kernel_recv_ts flows through the system

1. `recvmsg(sock, buf, ancillary_buf)` returns `(nbytes, ancdata,
   flags, addr)`.
2. For each `ancdata` entry with
   `cmsg_level=SOL_SOCKET, cmsg_type=SO_TIMESTAMPNS_NEW`, parse the
   payload as two `int64` (tv_sec, tv_nsec). Convert to
   `kernel_recv_ts_us = tv_sec * 1_000_000 + tv_nsec // 1000`.
3. The bytes in `buf` are the TCP stream. Feed them to the WS parser.
4. When a complete WS frame (or a defragmented sequence thereof)
   emerges, tag the frame with the *first packet's* kernel timestamp.
   Multi-packet frames happen; use the earliest packet's ts to be
   conservative (the frame is only meaningful once all bytes arrive,
   but the clock should reflect when the exchange started sending).
5. Pass `(ws_message_text, kernel_recv_ts_us)` into the existing
   dispatcher in `chronos/gateway.py`.

### 3.5 Delay estimator

Separate asyncio task per Binance stream (because RTT to
`fstream.binance.com` can differ from a control plane endpoint). It
issues a WebSocket ping frame every 1 second (opcode 9, 4-byte random
nonce), expects a pong (opcode 10) back with the same payload. Both
send and recv are kernel-timestamped (use `SO_TIMESTAMPING` with
`SOF_TIMESTAMPING_TX_SOFTWARE` for the TX side, then drain the error
queue via `recvmsg(MSG_ERRQUEUE)`; on recv side same
`SO_TIMESTAMPNS_NEW` path).

RTT = `kernel_recv_ts(pong) − kernel_send_ts(ping)`. Half-RTT is our
one-way estimate (assumes symmetric routing; Tokyo-Tokyo peering is
symmetric within ±5 μs per independent measurements — cite to be
added once we run them).

State: ring buffer of last 600 samples (10 minutes). Publish two
scalars visible to recorder:
- `measured_one_way_delay_us` = running median
- `delay_estimator_confidence_us` = MAD (median absolute deviation) ×
  1.4826 (robust σ estimate)

Consumers of parquet read both columns and can reject rows where
confidence > 100 μs as stale.

### 3.6 Binance clock-offset estimator

Separate long-window estimator, feeds from every received WS message
(not just ping). On each event compute:

```
bias_sample = local_ts_us(Python) − E_us − measured_one_way_delay_us
```

where `E_us = E * 1000` (Binance emits in ms). Running median over a
3600-sample window is `binance_clock_offset_us`. Remains near-constant
on the minute scale (Binance runs NTP); drifts at most 1-2 μs per
hour.

`estimated_true_event_ts_us` can use either:

**Formulation A (kernel-recv based, our preferred)**:
```
estimated_true_event_ts_us =
    kernel_recv_ts_us − measured_one_way_delay_us
```

This is the cleanest. No dependency on Binance's internal clock. We
measure "when the WS frame left Binance's server", not "when
matching engine stamped E". For derivative consumers (backtest, TWAP,
microstructure research) this is the authoritative time anyway.

**Formulation B (Binance-E based, alternative)**:
```
estimated_true_event_ts_us =
    E_us + binance_clock_offset_us
```

Useful as a cross-check. If A and B diverge by > their joint σ, emit
an `INTEGRITY_GAP` row with kind `clock_bias_anomaly`.

**Ship both values** as separate parquet columns so consumers can
pick and so we can post-hoc correlate them.

### 3.7 Schema versioning

Bump all trade / depth_diff / depth_snapshot / mark_price /
liquidation / book_checkpoint / derivatives_poll / funding_settlement
schemas from `v1` to `v2`. Add columns:

- `kernel_recv_ts_us` (int64, nullable — null for non-kernel-stamped
  streams)
- `estimated_true_event_ts_us` (int64, nullable)
- `estimated_true_event_ts_binance_us` (int64, nullable, from
  formulation B)
- `measured_one_way_delay_us` (int64, nullable)
- `delay_estimator_confidence_us` (int64, nullable)

The existing `local_ts_us` stays — it is the audit trail and the only
source of truth for "when our Python process observed the frame",
which we still want recorded.

Update `chronos/version.py SCHEMA_VERSIONS` atomically with the
column additions. Old v1 files on disk stay readable by Chronos's
reader helpers; new writes go as v2.

Add a `v2` round-trip test for every stream type in `tests/
test_schema_v2.py`.

### 3.8 Feature flag

`CHRONOS_INGEST_MODE` env var, read at entrypoint:

- `aiohttp` — current path (default until Phase 2 is proven in live).
- `kernel` — new path.

The flag routes Binance stream creation in `gateway.py` between two
implementations. Allows immediate rollback if Phase 2 misbehaves in
production.

---

## 4. New modules to create

Under `chronos/`:

### `chronos/kernel_socket.py`
Low-level helpers, no networking logic:
- `enable_kernel_timestamps(sock: socket.socket) -> None` — setsockopt
  for `SO_TIMESTAMPNS_NEW` (+ `SO_TIMESTAMPING` for TX side).
- `parse_scm_timestampns(ancdata) -> int | None` — pull microseconds
  since epoch from cmsg.
- Constants: `SCM_TIMESTAMPNS_NEW = 35`, `SO_TIMESTAMPNS_NEW = 64`
  etc. — check `<asm-generic/socket.h>` for your kernel version.

### `chronos/ws_parser.py`
Pure RFC 6455 frame parser:
- `class FrameParser` with `feed(bytes)` → yields `(opcode, payload,
  first_byte_ts_us)` tuples.
- Handles defrag (opcode 0 continuation, FIN bit).
- Handles control frames (ping / pong / close) inline.
- Client-side: remember to mask outgoing frames.

Unit tests against RFC 6455 canonical vectors in
`tests/test_ws_parser.py`.

### `chronos/kernel_ws.py`
High-level async client:
- `class KernelTimestampedWSClient`
- `async connect(url, ssl_ctx)` — uses `asyncio.open_connection`, does
  HTTP Upgrade handshake (parse Sec-WebSocket-Accept), enables kernel
  timestamps on underlying socket, returns ready state.
- `async messages()` — async generator yielding `(text, kernel_ts_us)`.
- `async send_ping(payload: bytes)` — sends, returns a future for
  the pong; kernel-timestamps both.
- Handles reconnect with exponential backoff (same semantics as the
  current aiohttp path in `gateway.py`).

### `chronos/delay_estimator.py`
- `class DelayEstimator` — ring buffer of RTTs, exposes
  `one_way_delay_us` and `confidence_us` properties (read by
  recorder).
- `async run(client: KernelTimestampedWSClient)` — task that pings
  every 1 s, consumes pong, updates estimate.
- `class BinanceClockOffsetEstimator` — running median on `(local_ts
  − E − one_way)` samples supplied by recorder on every event.

### Modifications to existing modules

- `chronos/version.py`: bump all `SCHEMA_VERSIONS` entries to `v2`.
- `chronos/schemas.py`: add the 5 new column fields to every preamble
  helper that builds a schema.
- `chronos/recorder.py`:
  - Extend `Recorder.record` and `Recorder.record_preformed_rows`
    signatures to accept `kernel_recv_ts_us` and
    `estimator_snapshot`.
  - Write the 5 new columns into every emitted row.
  - Keep backwards compatibility: callers that don't pass the extras
    get `None` for those fields.
- `chronos/gateway.py`:
  - Read `CHRONOS_INGEST_MODE` env.
  - When `kernel`: instantiate `KernelTimestampedWSClient` +
    `DelayEstimator` per Binance stream; pass `kernel_ts` +
    estimator snapshot to `self._feed(key, msg, ...)`.
  - When `aiohttp`: current path, unchanged.
- `scripts/record_data.py`: surface `CHRONOS_INGEST_MODE` in the
  boot log so it's visible whether Phase 2 is active.

---

## 5. Tests

Every module above gets a `tests/test_<module>.py` with the same
rigor as existing Chronos tests (see `tests/test_order_book.py` for
style). Full suite **must stay at 100%** — `PYTHONPATH=.
/home/scalper/scalper-bot/venv/bin/python -m pytest tests/ -q`.

Specific test assets:

- `tests/test_ws_parser.py` — RFC 6455 canonical frames (masked
  client→server, unmasked server→client, multi-frame defrag,
  oversized 16-bit and 64-bit payload lengths, control-frame
  interleaving).
- `tests/test_kernel_socket.py` — fake ancillary buffer construction
  and parsing (`SOL_SOCKET/SCM_TIMESTAMPNS_NEW` cmsg payload of two
  int64). Use `socket.CMSG_SPACE` for alignment.
- `tests/test_delay_estimator.py` — feed synthetic RTT samples with
  known distribution, verify running median + MAD converge; verify
  outlier rejection (Hampel filter).
- `tests/test_kernel_ws_integration.py` — spawn a local TLS echo
  server (`asyncio.start_server` + self-signed cert), run
  `KernelTimestampedWSClient` against it, verify round-trip, verify
  kernel_ts is populated, verify ping/pong.
- `tests/test_schema_v2.py` — round-trip of every stream type with
  the new columns, verify metadata version = `v2`, verify reader
  helpers dispatch correctly.
- `tests/test_recorder_phase2_e2e.py` — run the whole Recorder +
  fake KernelTimestampedWSClient end-to-end, confirm all 5 new
  columns present in parquet output.

---

## 6. Acceptance criteria

The build is considered **complete** when all of the following hold
on the Contabo host:

1. `pytest tests/ -q` reports 100% passing (existing 98 + new tests
   added in Phase 2, expected 140+).

2. Live run on Contabo with
   `CHRONOS_INGEST_MODE=kernel` for ≥ 1 hour produces:
   - All existing streams writing as before (no regression in
     `find /home/scalper/chronos-data -name '*.parquet' | wc -l`).
   - Binance trade/depth_diff parquets contain populated
     `kernel_recv_ts_us`, `estimated_true_event_ts_us`,
     `measured_one_way_delay_us`, `delay_estimator_confidence_us`.
   - **σ of `estimated_true_event_ts_us − exchange_event_ts_us` (per
     event, across the hour excluding the first 10 min of estimator
     warm-up) ≤ 40 μs.**
   - `delay_estimator_confidence_us` reported by the recorder
     stabilizes < 20 μs after 10 minutes of runtime.
   - Schema metadata in every written parquet carries
     `schema_version = v2` and
     `schema_id = <stream_type>:v2`.
   - No regression in existing metrics: 0 ERROR/CRITICAL,
     monotonically increasing `final_update_id` in depth_diff,
     reconcile steady state ≤ 10 findings per 15 min, memory RSS
     stable ≤ 2 GB.

3. Rollback works: setting `CHRONOS_INGEST_MODE=aiohttp` and
   restarting returns immediately to v1-compatible behaviour with
   the new columns written as null for older callers.

4. Commits pushed to `BlackDigitalStudio/crypto-market-recorder` with
   clear commit messages. README.md and CHANGELOG.md updated to
   reflect Phase 2.

5. `docs/PROGRESS.md` updated with a Phase 2 section summarising the
   measured σ and the architectural decisions actually taken.

---

## 7. Constraints and non-destructive rules

- **Do not regress chrony**. Its 30 μs floor is the entire budget we
  depend on. If you see `chronyc tracking` reporting worse numbers,
  stop and investigate — a regressed chrony makes the whole phase
  meaningless.
- **Do not disable existing tests** to make new ones pass. If an
  existing test conflicts with Phase 2 semantics, fix the test's
  expectations with a clear commit message, never delete.
- **Do not touch the production scalper-bot recorder** (pid 515532,
  `/home/scalper/scalper-bot/`).
- **Credentials** (`DERIBIT_CLIENT_ID`, `DERIBIT_CLIENT_SECRET`)
  must never appear in logs, parquet metadata, commit messages,
  stdout, or any other persisted surface. `DeribitCredentials.__repr__`
  redacts — keep it that way; review new code for accidental leaks.
- **Schema v1 compatibility**: every Chronos reader helper
  (`chronos/reader.py`) must continue to read v1 files after the v2
  bump. Add a dispatch branch on `schema_version`.
- **Rollback**: `CHRONOS_INGEST_MODE=aiohttp` must work at any moment
  during Phase 2 development. Never break the aiohttp path while
  iterating on the kernel path.
- **Measurement honesty**: report σ from actual empirical live data,
  not from unit-test synthetic numbers. If the live σ comes in above
  40 μs, do not declare done — diagnose.

---

## 8. Risks requiring early investigation (do these first)

Before writing any ingest code, verify in under an hour each of:

1. `uname -r` on Contabo confirms kernel ≥ 5.1. If not, escalate —
   `SO_TIMESTAMPNS_NEW` unavailable, fall back to `SO_TIMESTAMPNS`
   (ms precision — breaks the target).

2. `getsockopt(sock, SOL_SOCKET, SO_TIMESTAMPNS_NEW)` round-trip
   works from a CPython 3.12 subprocess. Write a 30-line smoke script
   that connects TCP to `fstream.binance.com:443`, enables the
   option, reads a byte, confirms ancdata is returned. If Contabo's
   kernel or the virtio NIC doesn't populate software timestamps,
   we pivot to user-space stamping immediately-after-recv
   (σ ~ 50 μs instead of 10 μs — still under budget but tighter).

3. `asyncio.open_connection(..., ssl=ctx)` actually exposes
   `writer.get_extra_info('socket')` as a real SSL-wrapped socket
   where `recvmsg` is callable. If asyncio wraps it too opaquely,
   we may need `ssl.wrap_socket` on a raw socket and drive the IO
   manually. Write a 50-line prototype and confirm.

4. Binance's WS server responds to ping frames (opcode 9) with pongs
   (opcode 10). The docs imply yes; confirm experimentally because
   some WS servers require the ping payload to be "valid" or
   intermittently drop pings. If unsupported, fall back to
   REST-level RTT probing against `/fapi/v1/time` (every 1 s is a
   trivial fraction of Binance's 1200-req/min rate limit).

5. TX-side kernel timestamping (`SO_TIMESTAMPING +
   SOF_TIMESTAMPING_TX_SOFTWARE`) on a TCP socket with TLS — tricky
   because TLS encrypts before write. The send timestamp reflects
   when the encrypted bytes hit the wire, which is what we want. But
   the MSG_ERRQUEUE drain is fiddly — prototype this before fully
   integrating into the client.

If any of these fail, fix-or-pivot immediately — do not write
dependent code first and discover the failure later.

---

## 9. Suggested commit cadence

The work naturally splits into reviewable units. Suggested series:

1. `feat(kernel): SO_TIMESTAMPNS helpers + unit tests`
2. `feat(ws): RFC 6455 frame parser + unit tests`
3. `feat(kernel-ws): client with asyncio TLS + kernel ts extraction`
4. `feat(delay): DelayEstimator and BinanceClockOffsetEstimator`
5. `feat(schemas): v2 schema bump with 5 new timestamp columns`
6. `feat(recorder): accept kernel_recv_ts in record path`
7. `feat(gateway): CHRONOS_INGEST_MODE dispatch, kernel path for Binance`
8. `chore(reader): dispatch between schema v1 and v2`
9. Tests + live validation report in `docs/PHASE2_RESULTS.md`

Each commit passes `pytest tests/ -q` on its own. Do not squash into
a single giant commit — the review trail needs to be inspectable.

---

## 10. Useful references

- Linux kernel `<linux/net_tstamp.h>` defines SOF_TIMESTAMPING_*.
- `man 7 socket` covers `SO_TIMESTAMPNS_NEW` and `SO_TIMESTAMPING`.
- `man recvmsg` covers ancillary data layout.
- RFC 6455 §5.2 covers WebSocket frame format.
- Binance Futures WS docs: https://binance-docs.github.io/apidocs/futures/en/
- Tardis normalized-ts theory: https://docs.tardis.dev/faq#how-do-you-handle-timestamps
  (their approach is essentially what we're building).

---

## 10a. Development vs validation environment split

This Phase is implemented under a two-environment workflow. The agent
that picks up this spec runs on **GitHub Codespaces (or equivalent
CI)**. Live acceptance validation is performed in a **separate short
session on the Contabo Tokyo host** (which is also the production
host for Chronos v1 and other unrelated workloads — do not hold that
machine for dev iteration).

### What the GitHub agent does

- All 9 commits on a branch `phase2-kernel-ts`.
- Full unit + integration test suite (`pytest tests/ -q`) passes on
  every commit.
- Integration tests use a local TLS echo server (spawn via
  `asyncio.start_server` with self-signed cert) to exercise the
  full `KernelTimestampedWSClient` round-trip.
- Smoke tests may reach `fstream.binance.com:443` to verify TLS
  handshake + `kernel_recv_ts_us` populates, but must **not** rely on
  σ measurements from that connection (GitHub datacenter ↔ Binance
  Tokyo has 150-300 ms RTT, which destroys the statistic this Phase
  is measured by).
- Optional but encouraged: add a GitHub Actions workflow at
  `.github/workflows/test.yml` that runs the suite on every push and
  PR.
- Open a PR `phase2-kernel-ts → main`; do not merge. Title should
  contain `READY FOR LIVE VALIDATION ON CONTABO` once green.

### What the Contabo agent does (separate short session)

- `git fetch origin; git checkout phase2-kernel-ts`.
- Gracefully stop the running Chronos v1 process
  (`pkill -INT -f 'chronos/scripts/record_data.py'`, wait ≥ 10 s for
  final drain + compact).
- Launch the Phase 2 build with
  `CHRONOS_INGEST_MODE=kernel`, write to
  `/home/scalper/chronos-data-phase2/` so the two archives stay
  separable for comparison analysis.
- Run for at least 1 hour continuous (use `ScheduleWakeup` to come
  back).
- Read the Phase 2 parquets and compute empirical σ of
  `estimated_true_event_ts_us − exchange_event_ts_us` after the
  first 10 min of estimator warm-up.
- If σ ≤ 40 μs: write `docs/PHASE2_RESULTS.md` with the measured
  numbers, commit to `phase2-kernel-ts`, update the PR description
  with `VALIDATION PASSED σ=<value> μs`.
- If σ > 40 μs: write the same file with the observed numbers and
  note `VALIDATION FAILED` — hand back to the GitHub agent for
  iteration without merging.
- Switch back to Chronos v1 (`git checkout main` +
  `CHRONOS_INGEST_MODE=aiohttp` + restart) so the production archive
  continues on the proven path while the PR waits to be merged.

The Contabo session is deliberately **short** (≤ 2 hours per round):
all dev work happens on GitHub, and the Contabo machine is held
only for the empirical statistic, never for iteration.

---

## 11. What "done" looks like

Live run on Contabo for 24 hours with
`CHRONOS_INGEST_MODE=kernel`, producing a parquet archive where every
Binance futures row has:

- `kernel_recv_ts_us` populated, within 15 μs of true TCP recv time
- `estimated_true_event_ts_us` with empirical σ ≤ 40 μs
- `delay_estimator_confidence_us` stable between 5 and 25 μs
- All existing v1 integrity properties preserved (monotonic final_update_id,
  no gap explosions, no reseed storms)
- Schema metadata advertises `v2`

Commits pushed. CHANGELOG updated. Handoff-back doc
`docs/PHASE2_RESULTS.md` committed with the observed numbers and any
deviations from this plan, with rationale.

That is the definition of complete for Phase 2.
