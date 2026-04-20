"""Recorder — orchestration wiring P1 + P2 + P3 + P5 + P6 + P7 + P10 +
P11 + P14.

Per recorded raw message we:

1. Stamp :class:`Timestamps` (P1).
2. Append the raw frame to the per-stream ``.jsonl.gz`` archive (P2).
3. Run attached :class:`Validator` instances (P3). Any produced
   :class:`GapRecord` is written to the per-(source, exchange, symbol)
   :attr:`StreamType.INTEGRITY_GAP` stream.
4. If the stream is a ``depthUpdate`` with an attached maintained book
   (P6/P7/P10): apply the diff to the book, emit classified
   ``depth_diff`` rows (insert/update/remove) and one ``depth_snapshot``
   row; otherwise normalize via the stateless dispatcher.
5. For mark-price rows, feed a :class:`FundingSettlementDetector` (P14).
6. On each flush tick, write buffered rows to a parts parquet file whose
   schema metadata carries the full v2 version preamble (P5). Maintained
   books also periodically emit ``book_checkpoint`` rows (P10).
7. On hour rollover, compact the prior hour's parts.

P11 REST reconciliation is exposed via :meth:`Recorder.reconcile_with_rest`
and emits :class:`StreamType.INTEGRITY_GAP` rows on drift.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa

from . import normalize as norm
from .funding import FundingSettlementDetector
from .integrity import (
    GapRecord,
    Validator,
    default_validators_for,
    gap_row,
)
from .order_book import BookChange, DriftFinding, OrderBookV2
from .parquet_writer import ParquetWriter
from .raw_archive import RawArchiveWriter
from .schemas import schema_for
from .timestamps import Timestamps, extract_timestamps, now_us
from .version import StreamType

logger = logging.getLogger(__name__)

DEFAULT_FLUSH_INTERVAL_SEC = 15.0
HEALTH_FILE_DEFAULT = "/tmp/chronos_health"


# Normalizer dispatch: (source_id, channel) → callable
_NORMALIZERS: dict[tuple[str, str], Callable[..., list[dict]]] = {
    ("binance_futures_ws", "depthUpdate"): norm.normalize_binance_depth_update,
    ("binance_futures_ws", "aggTrade"): norm.normalize_binance_agg_trade,
    ("binance_futures_ws", "markPriceUpdate"): norm.normalize_binance_mark_price,
    ("binance_futures_ws", "forceOrder"): norm.normalize_binance_force_order,
    ("bybit_v5_public_linear", "publicTrade"): norm.normalize_bybit_public_trade,
    ("okx_v5_public", "trades"): norm.normalize_okx_trades,
    ("bitget_v2_public", "trade"): norm.normalize_bitget_trade,
    ("gateio_v4_futures", "futures.trades"): norm.normalize_gateio_futures_trades,
    ("coinbase_advanced_trade_ws", "market_trades"): norm.normalize_coinbase_market_trades,
    ("deribit_v2_public_ws", "trades"): norm.normalize_deribit_trades,
}


@dataclass(frozen=True)
class StreamKey:
    """A recordable stream.

    - ``source_id`` feeds version.SOURCE_API_VERSIONS and normalizer dispatch.
    - ``exchange`` is the short tag used in the storage path
      (``data_v2/<exchange>/<symbol>/<stream_type>/``).
    - ``channel`` is the upstream event name (e.g. ``"aggTrade"``,
      ``"depthUpdate"``, ``"publicTrade"``).
    - ``depth_levels`` is required when ``stream_type`` is
      :attr:`StreamType.DEPTH_SNAPSHOT` and ignored otherwise.
    """

    source_id: str
    exchange: str
    symbol: str
    stream_type: StreamType
    channel: str
    depth_levels: int | None = None

    @property
    def rel_dir(self) -> Path:
        return Path(self.exchange) / self.symbol / self.stream_type.value

    @property
    def dispatch_key(self) -> tuple[str, str]:
        return (self.source_id, self.channel)


def integrity_key_for(key: StreamKey) -> StreamKey:
    """Sibling integrity stream for a data stream."""
    return StreamKey(
        source_id=key.source_id,
        exchange=key.exchange,
        symbol=key.symbol,
        stream_type=StreamType.INTEGRITY_GAP,
        channel="integrity",
    )


def funding_settlement_key_for(key: StreamKey) -> StreamKey:
    """Sibling funding-settlement stream for a mark-price stream."""
    return StreamKey(
        source_id=key.source_id,
        exchange=key.exchange,
        symbol=key.symbol,
        stream_type=StreamType.FUNDING_SETTLEMENT,
        channel="funding_settlement",
    )


def depth_snapshot_key_for(depth_diff_key: StreamKey, *, levels: int) -> StreamKey:
    """Sibling L-N depth_snapshot stream derived from a maintained book."""
    return StreamKey(
        source_id=depth_diff_key.source_id,
        exchange=depth_diff_key.exchange,
        symbol=depth_diff_key.symbol,
        stream_type=StreamType.DEPTH_SNAPSHOT,
        channel=f"depth_snapshot_L{levels}",
        depth_levels=levels,
    )


def book_checkpoint_key_for(depth_diff_key: StreamKey) -> StreamKey:
    """Sibling full-state book_checkpoint stream."""
    return StreamKey(
        source_id=depth_diff_key.source_id,
        exchange=depth_diff_key.exchange,
        symbol=depth_diff_key.symbol,
        stream_type=StreamType.BOOK_CHECKPOINT,
        channel="book_checkpoint",
    )


@dataclass
class _StreamState:
    key: StreamKey
    schema: pa.Schema
    buffer: list[dict] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_hour_key: str | None = None
    # P3 — per-stream ordered chain of stateful validators.
    validators: list[Validator] = field(default_factory=list)
    # P14 — only set on mark-price streams.
    funding_detector: FundingSettlementDetector | None = None


@dataclass
class _BookBinding:
    """Binds a maintained :class:`OrderBookV2` to a ``depthUpdate`` stream.

    All three output streams (depth_diff, depth_snapshot, book_checkpoint)
    live alongside the source ``depthUpdate`` in the same exchange/symbol
    directory tree.
    """

    book: OrderBookV2
    depth_diff_key: StreamKey
    depth_snapshot_key: StreamKey
    checkpoint_key: StreamKey
    snapshot_levels: int
    snapshot_every_n_diffs: int
    checkpoint_interval_sec: float
    diffs_since_snapshot: int = 0
    last_checkpoint_monotonic: float | None = None


def _to_int(x: Any) -> int | None:
    try:
        return int(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _current_hour_key(now: datetime | None = None) -> str:
    n = now or datetime.now(timezone.utc)
    return n.strftime("%Y%m%d_%H")


def _rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Build an Arrow table directly from row dicts (no pandas round-trip).

    pandas-based conversion held the GIL inside the flush thread pool —
    even offloaded via ``asyncio.to_thread`` it starved the event loop
    and produced ~1.2 GB/hour RSS growth from allocator fragmentation on
    short-lived DataFrames. ``pa.array`` / ``pa.table`` build columns
    in C and release the GIL for the bulk of the conversion work.
    """
    # Transpose list-of-dicts → dict-of-lists, matching schema order and
    # filling missing columns with None (preserves nullable semantics).
    cols: dict[str, list] = {f.name: [None] * len(rows) for f in schema}
    for i, row in enumerate(rows):
        for name in cols:
            v = row.get(name)
            if v is not None:
                cols[name][i] = v
    return pa.table(cols, schema=schema)


class Recorder:
    """Universal recorder — any exchange / symbol / stream type."""

    def __init__(
        self,
        root: Path,
        *,
        flush_interval_sec: float = DEFAULT_FLUSH_INTERVAL_SEC,
        health_file: str | Path = HEALTH_FILE_DEFAULT,
    ) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._flush_interval = float(flush_interval_sec)
        self._health_file = Path(health_file)
        self._parquet = ParquetWriter(self._root)
        self._archive = RawArchiveWriter(self._root)
        self._streams: dict[StreamKey, _StreamState] = {}
        self._books: dict[StreamKey, _BookBinding] = {}
        self._streams_lock = threading.Lock()
        self._flush_task: asyncio.Task | None = None
        # Background hour-rollover compactions run fire-and-forget; we
        # hold references so GC doesn't kill them mid-flight and so
        # `stop()` can drain them before the process exits.
        self._background_tasks: set[asyncio.Task] = set()
        self._running = False

    # --- lifecycle ---

    def register(
        self,
        key: StreamKey,
        *,
        validators: list[Validator] | None = None,
        attach_default_validators: bool = True,
        enable_funding_settlement: bool = True,
    ) -> None:
        """Register ``key`` and optionally attach validators / side-effects.

        ``validators`` is appended to any defaults picked by
        :func:`integrity.default_validators_for` when
        ``attach_default_validators=True``. If the resulting list is
        non-empty, a sibling :attr:`StreamType.INTEGRITY_GAP` stream is
        auto-registered.

        ``enable_funding_settlement`` causes a mark-price stream to
        auto-register a sibling :attr:`StreamType.FUNDING_SETTLEMENT`
        stream and attach a :class:`FundingSettlementDetector`.
        """
        self._register_raw(key)
        state = self._streams[key]

        # P3 wiring
        chosen: list[Validator] = []
        if attach_default_validators:
            chosen.extend(default_validators_for(key.source_id, key.channel))
        if validators:
            chosen.extend(validators)
        if chosen:
            state.validators = chosen
            self._register_raw(integrity_key_for(key))

        # P14 wiring
        if enable_funding_settlement and key.stream_type is StreamType.MARK_PRICE:
            state.funding_detector = FundingSettlementDetector()
            self._register_raw(funding_settlement_key_for(key))

    def register_maintained_book(
        self,
        *,
        source_id: str,
        exchange: str,
        symbol: str,
        depth_diff_channel: str = "depthUpdate",
        snapshot_levels: int = 100,
        checkpoint_interval_sec: float = 300.0,
        snapshot_every_n_diffs: int = 1,
    ) -> OrderBookV2:
        """Attach a maintained :class:`OrderBookV2` to a depth-diff stream.

        Registers three streams under
        ``<exchange>/<symbol>/{depth_diff, depth_snapshot, book_checkpoint}/``,
        plus the default P3 depth-sequence validator (which writes gap
        rows to the sibling ``integrity_gap`` stream).

        Returns the book object. Caller is responsible for seeding it via
        :meth:`ingest_rest_snapshot` before / shortly after subscribing to
        the WS depth stream.
        """
        diff_key = StreamKey(
            source_id=source_id, exchange=exchange, symbol=symbol,
            stream_type=StreamType.DEPTH_DIFF, channel=depth_diff_channel,
        )
        snap_key = depth_snapshot_key_for(diff_key, levels=snapshot_levels)
        cp_key = book_checkpoint_key_for(diff_key)

        # Register diff with default validators (P3 DepthSequenceValidator).
        self.register(diff_key)
        self._register_raw(snap_key)
        self._register_raw(cp_key)

        book = OrderBookV2(symbol=symbol)
        binding = _BookBinding(
            book=book,
            depth_diff_key=diff_key,
            depth_snapshot_key=snap_key,
            checkpoint_key=cp_key,
            snapshot_levels=snapshot_levels,
            snapshot_every_n_diffs=max(1, int(snapshot_every_n_diffs)),
            checkpoint_interval_sec=max(1.0, float(checkpoint_interval_sec)),
        )
        with self._streams_lock:
            self._books[diff_key] = binding
        logger.info(
            "maintained book attached — %s/%s L%d checkpoint=%.0fs",
            exchange, symbol, snapshot_levels, checkpoint_interval_sec,
        )
        return book

    def ingest_rest_snapshot(
        self,
        depth_diff_key: StreamKey,
        snapshot: dict[str, Any],
    ) -> None:
        """Seed / resync the book for ``depth_diff_key`` from a REST snapshot."""
        binding = self._books.get(depth_diff_key)
        if binding is None:
            raise KeyError(f"no maintained book for {depth_diff_key}")
        binding.book.restore_from_snapshot(snapshot)
        logger.info(
            "book seeded — %s/%s levels=%d last_update_id=%s",
            depth_diff_key.exchange, depth_diff_key.symbol,
            len(binding.book), binding.book.last_update_id,
        )
        # Emit an initial depth_snapshot row so consumers see the seed
        # state without having to wait for the first diff to land.
        ts = Timestamps(local_ts_us=now_us(), exchange_event_ts_us=None, exchange_trans_ts_us=None)
        snap_row = binding.book.snapshot_row(
            levels=binding.snapshot_levels,
            ts=ts,
            source_id=depth_diff_key.source_id,
        )
        snap_state = self._streams.get(binding.depth_snapshot_key)
        if snap_state is not None:
            with snap_state.lock:
                snap_state.buffer.append(snap_row)

    def reconcile_with_rest(
        self,
        depth_diff_key: StreamKey,
        snapshot: dict[str, Any],
        *,
        qty_tolerance: float = 1e-12,
        max_price_levels: int | None = None,
    ) -> int:
        """Compare maintained state to a REST snapshot; emit integrity gaps.

        Returns the number of drift findings. A return of ``0`` means the
        maintained book matches within tolerance.
        """
        binding = self._books.get(depth_diff_key)
        if binding is None:
            raise KeyError(f"no maintained book for {depth_diff_key}")
        findings = binding.book.compare_to_rest(
            snapshot,
            qty_tolerance=qty_tolerance,
            max_price_levels=max_price_levels,
        )
        if not findings:
            return 0
        ikey = integrity_key_for(binding.depth_diff_key)
        self._register_raw(ikey)  # may not be registered yet
        ts = Timestamps(local_ts_us=now_us(), exchange_event_ts_us=None, exchange_trans_ts_us=None)
        rows: list[dict] = []
        for f in findings:
            rows.append({
                "local_ts_us": ts.local_ts_us,
                "exchange_event_ts_us": ts.exchange_event_ts_us,
                "exchange_trans_ts_us": ts.exchange_trans_ts_us,
                "source_id": binding.depth_diff_key.source_id,
                "symbol": binding.depth_diff_key.symbol,
                "monitored_stream_type": StreamType.DEPTH_DIFF.value,
                "monitored_channel": binding.depth_diff_key.channel,
                "gap_kind": f"rest_depth_drift_{f.kind}",
                "expected": f"rest_qty={f.rest_qty}",
                "observed": f"local_qty={f.local_qty}",
                "previous_value": None,
                "current_value": None,
                "skip_count": None,
                "note": f"side={f.side} price={f.price}",
            })
        istate = self._streams.get(ikey)
        if istate is not None:
            with istate.lock:
                istate.buffer.extend(rows)
        logger.warning(
            "rest reconcile drift — %s/%s findings=%d",
            binding.depth_diff_key.exchange, binding.depth_diff_key.symbol,
            len(findings),
        )
        return len(findings)

    def _register_raw(self, key: StreamKey) -> None:
        """Unconditional register, no validator/detector wiring."""
        with self._streams_lock:
            if key in self._streams:
                return
            schema = schema_for(key.stream_type, depth_levels=key.depth_levels)
            state = _StreamState(key=key, schema=schema)
            self._streams[key] = state
        (self._root / key.rel_dir).mkdir(parents=True, exist_ok=True)
        self._parquet.recover_orphan_parts(
            stream_rel_dir=key.rel_dir,
            stream_type=key.stream_type,
            source_id=key.source_id,
            exchange=key.exchange,
            symbol=key.symbol,
        )

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info(
            "Recorder started — root=%s streams=%d flush=%.1fs",
            self._root, len(self._streams), self._flush_interval,
        )

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._drain_all(final=True)
        except Exception as e:
            logger.error("final drain failed: %r", e)
        try:
            self._archive.close()
        except Exception as e:
            logger.error("raw archive close failed: %r", e)

    # --- ingestion ---

    def record(self, key: StreamKey, msg: dict[str, Any]) -> None:
        """Record one raw WS frame for ``key``."""
        state = self._streams.get(key)
        if state is None:
            raise KeyError(f"stream not registered: {key}")

        ts = extract_timestamps(msg, source_id=key.source_id, channel=key.channel)
        hour_key = _current_hour_key()

        # P2 — raw archive
        try:
            self._archive.append(
                stream_rel_dir=key.rel_dir,
                hour_key=hour_key,
                local_ts_us=ts.local_ts_us,
                exchange_event_ts_us=ts.exchange_event_ts_us,
                exchange_trans_ts_us=ts.exchange_trans_ts_us,
                source_id=key.source_id,
                channel=key.channel,
                msg=msg,
            )
        except Exception as e:
            logger.error("raw archive append failed for %s: %r", key.rel_dir, e)

        # P3 — integrity validators
        if state.validators:
            self._run_validators(key, state, msg, ts)

        # P6/P7 — book-aware path for depth_diff streams with attached book
        binding = self._books.get(key)
        if binding is not None:
            self._apply_to_book(binding, msg, ts)
            return

        # Stateless normalize → rows
        normalizer = _NORMALIZERS.get(key.dispatch_key)
        if normalizer is None:
            return
        try:
            rows = normalizer(msg, ts=ts, source_id=key.source_id, symbol=key.symbol)
        except Exception as e:
            logger.error("normalize failed for %s/%s: %r", key.source_id, key.channel, e)
            return
        if not rows:
            return
        with state.lock:
            state.buffer.extend(rows)

        # P14 — funding settlement derived from markPrice rows
        if state.funding_detector is not None:
            self._run_funding_detector(key, state, rows, ts)

    def record_preformed_rows(self, key: StreamKey, rows: list[dict]) -> None:
        """Inject pre-formed rows (e.g. depth_snapshot from a maintained book)."""
        state = self._streams.get(key)
        if state is None:
            raise KeyError(f"stream not registered: {key}")
        if not rows:
            return
        with state.lock:
            state.buffer.extend(rows)

    # --- book-aware ingestion (P6/P7/P10) ---

    def _apply_to_book(
        self,
        binding: _BookBinding,
        msg: dict[str, Any],
        ts: Timestamps,
    ) -> None:
        book = binding.book
        if not book.synced:
            # Awaiting initial REST snapshot; drop the diff. The P3
            # validator's first-message state is already populated by the
            # validator chain, so resync is cleanly observable.
            return

        first_uid = msg.get("U")
        prev_last_uid = book.last_update_id
        try:
            changes = book.apply_diff(msg)
        except Exception as e:
            logger.error("book apply_diff failed for %s: %r", binding.depth_diff_key, e)
            return

        if changes:
            # Emit one classified depth_diff row per change.
            final_uid = book.last_update_id
            diff_rows = [
                book.change_to_depth_diff_row(
                    c, ts=ts,
                    source_id=binding.depth_diff_key.source_id,
                    first_update_id=_to_int(first_uid),
                    final_update_id=final_uid,
                    prev_final_update_id=prev_last_uid,
                )
                for c in changes
            ]
            diff_state = self._streams.get(binding.depth_diff_key)
            if diff_state is not None:
                with diff_state.lock:
                    diff_state.buffer.extend(diff_rows)

        # Emit a depth_snapshot row at the configured cadence.
        binding.diffs_since_snapshot += 1
        if binding.diffs_since_snapshot >= binding.snapshot_every_n_diffs:
            binding.diffs_since_snapshot = 0
            snap_row = book.snapshot_row(
                levels=binding.snapshot_levels, ts=ts,
                source_id=binding.depth_diff_key.source_id,
                first_update_id=_to_int(first_uid),
                prev_final_update_id=prev_last_uid,
            )
            snap_state = self._streams.get(binding.depth_snapshot_key)
            if snap_state is not None:
                with snap_state.lock:
                    snap_state.buffer.append(snap_row)

    def _maybe_emit_checkpoints(self) -> None:
        """Called from the flush loop; dumps each maintained book if due."""
        now = _time.monotonic()
        with self._streams_lock:
            bindings = list(self._books.values())
        for b in bindings:
            if not b.book.synced:
                continue
            last = b.last_checkpoint_monotonic
            if last is not None and (now - last) < b.checkpoint_interval_sec:
                continue
            ts = Timestamps(local_ts_us=now_us(), exchange_event_ts_us=None, exchange_trans_ts_us=None)
            rows = b.book.checkpoint_rows(ts=ts, source_id=b.depth_diff_key.source_id)
            if not rows:
                b.last_checkpoint_monotonic = now
                continue
            cp_state = self._streams.get(b.checkpoint_key)
            if cp_state is None:
                continue
            with cp_state.lock:
                cp_state.buffer.extend(rows)
            b.last_checkpoint_monotonic = now
            logger.info(
                "book checkpoint — %s/%s rows=%d last_update_id=%s",
                b.depth_diff_key.exchange, b.depth_diff_key.symbol,
                len(rows), b.book.last_update_id,
            )

    # --- validator / detector execution ---

    def _run_validators(
        self,
        key: StreamKey,
        state: _StreamState,
        msg: dict[str, Any],
        ts: Timestamps,
    ) -> None:
        try:
            all_gaps: list[GapRecord] = []
            for v in state.validators:
                try:
                    all_gaps.extend(v.check(msg, ts))
                except Exception as e:
                    logger.error(
                        "validator %s failed on %s/%s: %r",
                        type(v).__name__, key.source_id, key.channel, e,
                    )
            if not all_gaps:
                return
            rows = [
                gap_row(
                    gap=g, ts=ts,
                    source_id=key.source_id, symbol=key.symbol,
                    monitored_stream_type=key.stream_type,
                    monitored_channel=key.channel,
                )
                for g in all_gaps
            ]
            ikey = integrity_key_for(key)
            istate = self._streams.get(ikey)
            if istate is None:
                logger.warning(
                    "integrity stream not registered for %s; dropping %d gaps",
                    key, len(rows),
                )
                return
            with istate.lock:
                istate.buffer.extend(rows)
            for g in all_gaps:
                logger.warning(
                    "integrity gap — %s on %s/%s: expected=%s observed=%s skip=%s",
                    g.gap_kind, key.exchange, key.channel,
                    g.expected, g.observed, g.skip_count,
                )
        except Exception as e:
            logger.error("_run_validators outer failure for %s: %r", key, e)

    def _run_funding_detector(
        self,
        key: StreamKey,
        state: _StreamState,
        mark_rows: list[dict],
        ts: Timestamps,
    ) -> None:
        detector = state.funding_detector
        if detector is None:
            return
        # A single markPrice message produces exactly one normalized row;
        # iterate defensively.
        settle_rows: list[dict] = []
        for row in mark_rows:
            try:
                settle_rows.extend(detector.observe(
                    next_funding_time_ms=row.get("next_funding_time_ms"),
                    funding_rate=row.get("funding_rate"),
                    ts=ts,
                    source_id=key.source_id,
                    symbol=key.symbol,
                ))
            except Exception as e:
                logger.error("funding detector failed for %s: %r", key, e)
        if not settle_rows:
            return
        skey = funding_settlement_key_for(key)
        sstate = self._streams.get(skey)
        if sstate is None:
            logger.warning("settlement stream not registered for %s", key)
            return
        with sstate.lock:
            sstate.buffer.extend(settle_rows)
        for row in settle_rows:
            logger.info(
                "funding settlement — %s/%s r=%s settled_at=%s",
                key.exchange, key.symbol,
                row["realized_funding_rate"], row["settled_at_ms"],
            )

    # --- flush / compaction ---

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._flush_interval)
            except asyncio.CancelledError:
                break
            try:
                await self._drain_all(final=False)
                self._touch_health()
            except Exception as e:
                logger.error("flush loop error: %r", e)

    async def _drain_all(self, *, final: bool) -> None:
        """Drain buffers + compact due hours, offloading disk I/O to threads.

        All pyarrow writes and parquet compactions are dispatched via
        :func:`asyncio.to_thread` so the event loop never blocks on
        disk — otherwise a multi-second hour-rollover compaction would
        stall WS ingestion and the corresponding messages would queue
        up in the kernel TCP buffer, arriving with a 10-40 s ``local_ts``
        delay relative to ``exchange_event_ts`` (observed 2026-04-20 at
        39 s max latency on BTCUSDT aggTrade during hour 01→02 rollover).
        """
        hour_key = _current_hour_key()
        try:
            self._maybe_emit_checkpoints()
        except Exception as e:
            logger.error("checkpoint emission failed: %r", e)

        with self._streams_lock:
            states = list(self._streams.values())

        # Snapshot buffers quickly under lock, then write off-loop.
        part_jobs: list[tuple[_StreamState, list[dict]]] = []
        for state in states:
            with state.lock:
                rows = state.buffer
                state.buffer = []
            if rows:
                part_jobs.append((state, rows))

        # Fan part writes out to threads in parallel.
        if part_jobs:
            await asyncio.gather(
                *(asyncio.to_thread(self._write_part_sync, s, r, hour_key) for s, r in part_jobs),
                return_exceptions=True,
            )

        # Hour rollover: spawn compactions in the background and return
        # immediately. Waiting on gather() would block the next flush
        # tick by the full compaction duration (~6 s on 24 streams with
        # hour-worth parts), which in turn stalls WS ingestion and
        # produces 5-10 s `local_ts_us` delays around the UTC boundary.
        # Fire-and-forget is safe because (a) compactions are
        # idempotent, (b) typical hour-compact finishes in a few
        # seconds — well before the next hour rollover.
        compact_targets = [s for s in states if s.last_hour_key is not None and s.last_hour_key != hour_key]
        for s in compact_targets:
            prior = s.last_hour_key
            self._background_tasks.add(
                asyncio.create_task(
                    asyncio.to_thread(self._compact_one_sync, s, prior),
                    name=f"compact-{s.key.rel_dir}-{prior}",
                )
            )
        # Reap completed background tasks so the set doesn't grow.
        done = {t for t in self._background_tasks if t.done()}
        self._background_tasks.difference_update(done)

        for state in states:
            state.last_hour_key = hour_key

        # Raw-archive flush is a cheap Z_SYNC_FLUSH per open file; stay off-loop too.
        try:
            await asyncio.to_thread(self._archive.flush)
        except Exception as e:
            logger.error("archive flush failed: %r", e)

        if final:
            # On shutdown we DO wait — we want a fully compacted archive
            # before the process exits.
            await asyncio.gather(
                *(asyncio.to_thread(self._compact_one_sync, s, hour_key) for s in states),
                return_exceptions=True,
            )
            # Drain any still-running background compactions too.
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
                self._background_tasks.clear()

    def _compact_one_sync(self, state: _StreamState, hour_key: str) -> None:
        try:
            self._parquet.compact_hour(
                stream_rel_dir=state.key.rel_dir,
                hour_key=hour_key,
                stream_type=state.key.stream_type,
                source_id=state.key.source_id,
                exchange=state.key.exchange,
                symbol=state.key.symbol,
            )
        except Exception as e:
            logger.error("compact %s/%s failed: %r", state.key.rel_dir, hour_key, e)

    def _write_part_sync(
        self, state: _StreamState, rows: list[dict], hour_key: str,
    ) -> None:
        """Thread-pool-safe part writer. Called via asyncio.to_thread."""
        try:
            table = _rows_to_table(rows, state.schema)
        except Exception as e:
            logger.error(
                "rows→table failed for %s (rows=%d): %r",
                state.key.rel_dir, len(rows), e,
            )
            return
        try:
            self._parquet.write_part(
                table=table,
                stream_rel_dir=state.key.rel_dir,
                hour_key=hour_key,
                stream_type=state.key.stream_type,
                source_id=state.key.source_id,
                exchange=state.key.exchange,
                symbol=state.key.symbol,
            )
        except Exception as e:
            logger.error("write_part failed for %s: %r", state.key.rel_dir, e)

    def _touch_health(self) -> None:
        try:
            self._health_file.write_text(str(datetime.now(timezone.utc).timestamp()))
        except Exception:
            pass

    # --- introspection ---

    @property
    def root(self) -> Path:
        return self._root

    def registered_streams(self) -> list[StreamKey]:
        with self._streams_lock:
            return list(self._streams.keys())
