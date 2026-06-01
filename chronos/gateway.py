"""WS + REST gateway feeding :class:`Recorder`.

Responsibilities:

- Hold WS subscriptions per venue (Binance futures / Bybit / OKX / Bitget /
  Gate.io / Coinbase Advanced Trade / Deribit).
- P9 dedupe across multiple endpoints for the same venue stream.
- P13 ``@forceOrder`` liquidation subscription for Binance futures.
- Seed maintained books from REST on startup and resync on demand.
- Periodic REST reconcile (P11) + derivatives polling.

The gateway is intentionally independent from the production
``src/ws_client.py``. Keeping them separate means we can iterate v2
without destabilising live recorder traffic.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import orjson

from .dedupe import DedupeCache
from .funding import FundingSettlementDetector  # re-exported for convenience
from .recorder import (
    Recorder,
    StreamKey,
    integrity_key_for,
)
from .rest_client import BinanceFuturesREST
from .version import StreamType

logger = logging.getLogger(__name__)


# --- URLs ---

BINANCE_FUTURES_WS_PRIMARY = "wss://fstream.binance.com"
BINANCE_FUTURES_WS_FALLBACK = "wss://fstream3.binance.com"  # geo-rotated replica
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
BITGET_WS = "wss://ws.bitget.com/v2/ws/public"
GATEIO_FUTURES_WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"
COINBASE_ADVANCED_WS = "wss://advanced-trade-ws.coinbase.com"
DERIBIT_WS = "wss://www.deribit.com/ws/api/v2"


@dataclass
class _BinanceFuturesConfig:
    symbol: str
    exchange_tag: str = "binance_futures"
    source_id: str = "binance_futures_ws"
    snapshot_levels: int = 100
    maintain_book: bool = True
    # Cap maintained book depth per side (None = keep full book). REST seed /
    # reconcile fetch depth (valid Binance values: 5/10/20/50/100/500/1000).
    book_max_levels: int | None = None
    seed_limit: int = 1000
    subscribe_depth: bool = True
    subscribe_agg_trade: bool = True
    subscribe_mark_price: bool = True
    subscribe_force_order: bool = True
    subscribe_secondary_endpoint: bool = True
    reconcile_interval_sec: float = 900.0
    derivatives_poll_interval_sec: float = 15.0
    depth_diff_key: StreamKey | None = None
    mark_price_key: StreamKey | None = None
    agg_trade_key: StreamKey | None = None
    force_order_key: StreamKey | None = None
    derivatives_poll_key: StreamKey | None = None


@dataclass
class _VenueTradeConfig:
    source_id: str
    exchange_tag: str
    symbol: str
    subscribe_payload: dict[str, Any] | None
    ws_url: str
    trade_key: StreamKey | None = None


@dataclass(frozen=True)
class DeribitCredentials:
    """Deribit API key pair for WS `public/auth`.

    Unlocks `.raw` subscriptions (one frame per trade, <5 ms delivery).
    Without credentials we fall back to `.100ms` (per-trade granularity
    preserved, frames batched per 100 ms).

    Never printed in full — ``__repr__`` redacts both fields so the
    object is safe to include in logs / task metadata.
    """

    client_id: str
    client_secret: str

    def __repr__(self) -> str:
        cid = (self.client_id[:2] + "***") if self.client_id else "***"
        return f"DeribitCredentials(client_id={cid})"


class Gateway:
    """High-level orchestrator. Call ``add_*``, then ``start()``."""

    def __init__(
        self,
        recorder: Recorder,
        *,
        session: aiohttp.ClientSession | None = None,
        dedupe_size: int = 200_000,
        reconnect_initial_backoff_sec: float = 1.0,
        reconnect_max_backoff_sec: float = 30.0,
        ws_idle_max_sec: float = 60.0,
    ) -> None:
        self._rec = recorder
        self._own_session = session is None
        self._session = session
        self._dedupe = DedupeCache(dedupe_size)
        self._binance_rest: BinanceFuturesREST | None = None
        self._binance_configs: list[_BinanceFuturesConfig] = []
        self._simple_venues: list[_VenueTradeConfig] = []
        self._coinbase_products: list[str] = []
        self._deribit_instruments: list[str] = []
        self._deribit_credentials: DeribitCredentials | None = None
        # "auto" flips to "raw" if credentials accepted, else "100ms".
        self._deribit_interval: str = "auto"
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._backoff_initial = float(reconnect_initial_backoff_sec)
        self._backoff_max = float(reconnect_max_backoff_sec)
        self._idle_max = float(ws_idle_max_sec)

    # --- configuration -------------------------------------------------

    def add_binance_futures(
        self,
        symbol: str,
        *,
        snapshot_levels: int = 100,
        maintain_book: bool = True,
        subscribe_force_order: bool = True,
        subscribe_secondary_endpoint: bool = True,
        reconcile_interval_sec: float = 900.0,
        derivatives_poll_interval_sec: float = 15.0,
        book_max_levels: int | None = None,
        seed_limit: int = 1000,
    ) -> _BinanceFuturesConfig:
        cfg = _BinanceFuturesConfig(
            symbol=symbol.upper(),
            snapshot_levels=snapshot_levels,
            maintain_book=maintain_book,
            book_max_levels=book_max_levels,
            seed_limit=seed_limit,
            subscribe_force_order=subscribe_force_order,
            subscribe_secondary_endpoint=subscribe_secondary_endpoint,
            reconcile_interval_sec=reconcile_interval_sec,
            derivatives_poll_interval_sec=derivatives_poll_interval_sec,
        )
        # Register streams ahead of start() so recorder mkdirs etc. happen.
        sym = cfg.symbol
        src = cfg.source_id
        if cfg.maintain_book:
            self._rec.register_maintained_book(
                source_id=src, exchange=cfg.exchange_tag, symbol=sym,
                snapshot_levels=snapshot_levels,
                book_max_levels=book_max_levels,
            )
            cfg.depth_diff_key = StreamKey(src, cfg.exchange_tag, sym,
                                           StreamType.DEPTH_DIFF, "depthUpdate")
        if cfg.subscribe_agg_trade:
            cfg.agg_trade_key = StreamKey(src, cfg.exchange_tag, sym,
                                          StreamType.TRADE, "aggTrade")
            self._rec.register(cfg.agg_trade_key)
        if cfg.subscribe_mark_price:
            cfg.mark_price_key = StreamKey(src, cfg.exchange_tag, sym,
                                           StreamType.MARK_PRICE, "markPriceUpdate")
            self._rec.register(cfg.mark_price_key)
        if cfg.subscribe_force_order:
            cfg.force_order_key = StreamKey(src, cfg.exchange_tag, sym,
                                            StreamType.LIQUIDATION, "forceOrder")
            self._rec.register(cfg.force_order_key)
        cfg.derivatives_poll_key = StreamKey(
            "binance_futures_rest", cfg.exchange_tag, sym,
            StreamType.DERIVATIVES_POLL, "derivatives",
        )
        self._rec._register_raw(cfg.derivatives_poll_key)
        self._binance_configs.append(cfg)
        return cfg

    def add_bybit_trades(self, symbol: str) -> StreamKey:
        sym = symbol.upper()
        cfg = _VenueTradeConfig(
            source_id="bybit_v5_public_linear", exchange_tag="bybit", symbol=sym,
            subscribe_payload={"op": "subscribe", "args": [f"publicTrade.{sym}"]},
            ws_url=BYBIT_WS,
        )
        cfg.trade_key = StreamKey(cfg.source_id, cfg.exchange_tag, sym,
                                  StreamType.TRADE, "publicTrade")
        self._rec.register(cfg.trade_key)
        self._simple_venues.append(cfg)
        return cfg.trade_key

    def add_okx_trades(self, inst_id: str) -> StreamKey:
        cfg = _VenueTradeConfig(
            source_id="okx_v5_public", exchange_tag="okx", symbol=inst_id,
            subscribe_payload={"op": "subscribe", "args": [
                {"channel": "trades", "instId": inst_id},
            ]},
            ws_url=OKX_WS,
        )
        cfg.trade_key = StreamKey(cfg.source_id, cfg.exchange_tag, inst_id,
                                  StreamType.TRADE, "trades")
        self._rec.register(cfg.trade_key)
        self._simple_venues.append(cfg)
        return cfg.trade_key

    def add_bitget_trades(self, symbol: str) -> StreamKey:
        sym = symbol.upper()
        cfg = _VenueTradeConfig(
            source_id="bitget_v2_public", exchange_tag="bitget", symbol=sym,
            subscribe_payload={"op": "subscribe", "args": [
                {"instType": "USDT-FUTURES", "channel": "trade", "instId": sym},
            ]},
            ws_url=BITGET_WS,
        )
        cfg.trade_key = StreamKey(cfg.source_id, cfg.exchange_tag, sym,
                                  StreamType.TRADE, "trade")
        self._rec.register(cfg.trade_key)
        self._simple_venues.append(cfg)
        return cfg.trade_key

    def add_gateio_trades(self, contract: str) -> StreamKey:
        cfg = _VenueTradeConfig(
            source_id="gateio_v4_futures", exchange_tag="gateio", symbol=contract,
            subscribe_payload={
                "channel": "futures.trades", "event": "subscribe",
                "payload": [contract], "time": int(time.time()),
            },
            ws_url=GATEIO_FUTURES_WS,
        )
        cfg.trade_key = StreamKey(cfg.source_id, cfg.exchange_tag, contract,
                                  StreamType.TRADE, "futures.trades")
        self._rec.register(cfg.trade_key)
        self._simple_venues.append(cfg)
        return cfg.trade_key

    def add_coinbase_trades(self, product_id: str) -> StreamKey:
        self._coinbase_products.append(product_id)
        key = StreamKey("coinbase_advanced_trade_ws", "coinbase", product_id,
                        StreamType.TRADE, "market_trades")
        self._rec.register(key)
        return key

    def add_deribit_trades(
        self,
        instrument_name: str,
        *,
        credentials: DeribitCredentials | None = None,
        interval: str = "auto",
    ) -> StreamKey:
        """Register a Deribit trade stream.

        ``interval``:
            - ``"auto"`` — ``raw`` if credentials provided, else ``100ms``.
            - ``"raw"`` — force raw (requires credentials; auth failure
              falls back to ``100ms`` at run time).
            - ``"100ms"`` — 100ms-batched frames, no auth needed.
            - ``"agg2"`` — 2-trade aggregated, no auth needed.

        ``credentials`` is stored at the gateway level (one auth session
        serves every Deribit instrument). Passing different credentials
        on subsequent calls is silently ignored — document explicitly
        rather than fail on a subtle multi-account scenario.
        """
        if interval not in ("auto", "raw", "100ms", "agg2"):
            raise ValueError(f"deribit interval must be auto/raw/100ms/agg2, got {interval!r}")
        self._deribit_instruments.append(instrument_name)
        if credentials is not None and self._deribit_credentials is None:
            self._deribit_credentials = credentials
        self._deribit_interval = interval
        key = StreamKey("deribit_v2_public_ws", "deribit", instrument_name,
                        StreamType.TRADE, "trades")
        self._rec.register(key)
        return key

    # --- feed helpers --------------------------------------------------

    def _feed(self, key: StreamKey, msg: dict[str, Any], *, dedupe_id: Any = None) -> None:
        """Pass one normalized message to the recorder, dedupe-first."""
        if dedupe_id is not None:
            if self._dedupe.check_and_mark((key, dedupe_id)):
                return
        try:
            self._rec.record(key, msg)
        except KeyError:
            logger.error("unregistered stream fed from gateway: %s", key)
        except Exception as e:
            logger.error("recorder.record(%s) failed: %r", key, e)

    # --- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        if self._session is None:
            # Each Binance stream is its own persistent WS connection, all to
            # fstream.binance.com. A multi-symbol fleet needs many: N symbols x
            # {depth, aggTrade, markPrice, forceOrder} = 4N to one host. The old
            # limit_per_host=8 starved everything past the first ~8 (depth won
            # the race; trade/mark/liq timed out forever at 16 symbols). Lift the
            # per-host cap and raise the total pool accordingly.
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=256, limit_per_host=0),
            )
            self._own_session = True
        self._binance_rest = BinanceFuturesREST(self._session)

        await self._rec.start()

        # Seed maintained books + kick off Binance tasks.
        for cfg in self._binance_configs:
            if cfg.maintain_book and cfg.depth_diff_key is not None:
                await self._seed_binance_book(cfg)
            self._tasks.extend(self._launch_binance_tasks(cfg))

        # Simple venues.
        for venue in self._simple_venues:
            self._tasks.append(asyncio.create_task(self._run_simple_venue(venue)))

        # Coinbase + Deribit need multi-instrument subscription frames.
        if self._coinbase_products:
            self._tasks.append(asyncio.create_task(self._run_coinbase()))
        if self._deribit_instruments:
            self._tasks.append(asyncio.create_task(self._run_deribit()))

        logger.info(
            "Gateway started — binance=%d simple=%d coinbase=%d deribit=%d",
            len(self._binance_configs), len(self._simple_venues),
            len(self._coinbase_products), len(self._deribit_instruments),
        )

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        await self._rec.stop()
        if self._own_session and self._session is not None:
            await self._session.close()
            self._session = None
        logger.info("Gateway stopped")

    async def _seed_binance_book(self, cfg: _BinanceFuturesConfig) -> None:
        assert self._binance_rest is not None and cfg.depth_diff_key is not None
        for attempt in range(3):
            try:
                body = await self._binance_rest.depth_snapshot(cfg.symbol, limit=cfg.seed_limit)
                self._rec.ingest_rest_snapshot(cfg.depth_diff_key, body)
                return
            except Exception as e:
                logger.error(
                    "REST snapshot for %s failed (attempt %d): %r",
                    cfg.symbol, attempt + 1, e,
                )
                await asyncio.sleep(2 * (attempt + 1))
        logger.critical("REST snapshot for %s failed after retries", cfg.symbol)

    # --- Binance futures runners --------------------------------------

    def _launch_binance_tasks(self, cfg: _BinanceFuturesConfig) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        sym_lower = cfg.symbol.lower()

        if cfg.subscribe_depth and cfg.depth_diff_key is not None:
            primary_url = f"{BINANCE_FUTURES_WS_PRIMARY}/ws/{sym_lower}@depth@100ms"
            tasks.append(asyncio.create_task(
                self._run_binance_stream(
                    url=primary_url, key=cfg.depth_diff_key,
                    dedupe_id_field="u", name=f"{cfg.symbol}-depth-primary",
                )
            ))
            if cfg.subscribe_secondary_endpoint:
                secondary_url = f"{BINANCE_FUTURES_WS_FALLBACK}/ws/{sym_lower}@depth@100ms"
                tasks.append(asyncio.create_task(
                    self._run_binance_stream(
                        url=secondary_url, key=cfg.depth_diff_key,
                        dedupe_id_field="u", name=f"{cfg.symbol}-depth-fallback",
                    )
                ))

        if cfg.subscribe_agg_trade and cfg.agg_trade_key is not None:
            url = f"{BINANCE_FUTURES_WS_PRIMARY}/ws/{sym_lower}@aggTrade"
            tasks.append(asyncio.create_task(
                self._run_binance_stream(
                    url=url, key=cfg.agg_trade_key,
                    dedupe_id_field="a", name=f"{cfg.symbol}-aggTrade",
                )
            ))

        if cfg.subscribe_mark_price and cfg.mark_price_key is not None:
            url = f"{BINANCE_FUTURES_WS_PRIMARY}/ws/{sym_lower}@markPrice@1s"
            tasks.append(asyncio.create_task(
                self._run_binance_stream(
                    url=url, key=cfg.mark_price_key,
                    dedupe_id_field=None, name=f"{cfg.symbol}-markPrice",
                )
            ))

        if cfg.subscribe_force_order and cfg.force_order_key is not None:
            url = f"{BINANCE_FUTURES_WS_PRIMARY}/ws/{sym_lower}@forceOrder"
            tasks.append(asyncio.create_task(
                self._run_binance_stream(
                    url=url, key=cfg.force_order_key,
                    dedupe_id_field=None, name=f"{cfg.symbol}-forceOrder",
                )
            ))

        if cfg.maintain_book and cfg.depth_diff_key is not None:
            tasks.append(asyncio.create_task(self._run_binance_reconcile(cfg)))
        if cfg.derivatives_poll_key is not None:
            tasks.append(asyncio.create_task(self._run_binance_derivatives_poll(cfg)))

        return tasks

    async def _run_binance_stream(
        self,
        *,
        url: str,
        key: StreamKey,
        dedupe_id_field: str | None,
        name: str,
    ) -> None:
        backoff = self._backoff_initial
        while self._running:
            try:
                assert self._session is not None
                async with self._session.ws_connect(url, heartbeat=20) as ws:
                    logger.info("WS %s connected", name)
                    backoff = self._backoff_initial
                    last_data = time.monotonic()
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = orjson.loads(msg.data)
                            last_data = time.monotonic()
                            dedupe_id = data.get(dedupe_id_field) if dedupe_id_field else None
                            self._feed(key, data, dedupe_id=dedupe_id)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                        if time.monotonic() - last_data > self._idle_max:
                            logger.warning("WS %s idle → reconnect", name)
                            break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("WS %s error: %r", name, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    async def _run_binance_reconcile(self, cfg: _BinanceFuturesConfig) -> None:
        """Periodically compare maintained book to a fresh REST snapshot.

        Findings are always recorded as INTEGRITY_GAP rows. A full book
        reseed is triggered only when findings exceed
        ``reseed_threshold_pct`` of the comparison window — low single-
        digit drift is normal (race between our last WS diff and REST's
        snapshot-of-the-moment) and does not warrant throwing away the
        maintained state. The threshold defaults to 5% of the compared
        level count.
        """
        assert self._binance_rest is not None and cfg.depth_diff_key is not None
        # Compared level count = 2 sides × snapshot_levels.
        reseed_threshold = max(1, int(cfg.snapshot_levels * 2 * 0.05))
        await asyncio.sleep(cfg.reconcile_interval_sec)
        while self._running:
            try:
                body = await self._binance_rest.depth_snapshot(cfg.symbol, limit=cfg.seed_limit)
                n = self._rec.reconcile_with_rest(
                    cfg.depth_diff_key, body, max_price_levels=cfg.snapshot_levels,
                )
                if n == 0:
                    logger.debug("reconcile %s clean", cfg.symbol)
                elif n < reseed_threshold:
                    logger.info(
                        "reconcile minor drift %s — findings=%d (< reseed threshold=%d)",
                        cfg.symbol, n, reseed_threshold,
                    )
                else:
                    logger.warning(
                        "reconcile major drift %s — findings=%d (>= threshold=%d); reseeding book",
                        cfg.symbol, n, reseed_threshold,
                    )
                    self._rec.ingest_rest_snapshot(cfg.depth_diff_key, body)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("reconcile %s failed: %r", cfg.symbol, e)
            await asyncio.sleep(cfg.reconcile_interval_sec)

    async def _run_binance_derivatives_poll(self, cfg: _BinanceFuturesConfig) -> None:
        from .normalize import normalize_derivatives_poll
        from .timestamps import now_us
        from .timestamps import Timestamps
        assert self._binance_rest is not None and cfg.derivatives_poll_key is not None
        await asyncio.sleep(5)
        while self._running:
            try:
                oi = await self._binance_rest.open_interest(cfg.symbol)
                ls = await self._binance_rest.top_long_short_account_ratio(cfg.symbol)
                ratio = None
                long_account = None
                short_account = None
                if ls is not None:
                    try:
                        ratio = float(ls.get("longShortRatio", 0) or 0) or None
                        long_account = float(ls.get("longAccount", 0) or 0) or None
                        short_account = float(ls.get("shortAccount", 0) or 0) or None
                    except (TypeError, ValueError):
                        pass
                ts = Timestamps(local_ts_us=now_us(), exchange_event_ts_us=None, exchange_trans_ts_us=None)
                rows = normalize_derivatives_poll(
                    ts=ts, source_id="binance_futures_rest", symbol=cfg.symbol,
                    open_interest=oi, long_short_ratio=ratio,
                    long_account=long_account, short_account=short_account,
                    period="5m",
                )
                self._rec.record_preformed_rows(cfg.derivatives_poll_key, rows)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("derivatives poll %s: %r", cfg.symbol, e)
            await asyncio.sleep(cfg.derivatives_poll_interval_sec)

    # --- simple-venue runner (Bybit/OKX/Bitget/Gate.io) ---------------

    async def _run_simple_venue(self, cfg: _VenueTradeConfig) -> None:
        backoff = self._backoff_initial
        name = f"{cfg.exchange_tag}-{cfg.symbol}"
        while self._running:
            bitget_ping_task: asyncio.Task | None = None
            try:
                assert self._session is not None and cfg.trade_key is not None
                async with self._session.ws_connect(cfg.ws_url, heartbeat=20) as ws:
                    if cfg.subscribe_payload is not None:
                        await ws.send_json(cfg.subscribe_payload)
                    logger.info("WS %s connected", name)
                    backoff = self._backoff_initial
                    last_data = time.monotonic()
                    if cfg.exchange_tag == "bitget":
                        # Bitget requires a plain "ping" text every 20-25s.
                        async def _ping() -> None:
                            try:
                                while not ws.closed:
                                    await asyncio.sleep(20)
                                    if not ws.closed:
                                        await ws.send_str("ping")
                            except Exception:
                                pass
                        bitget_ping_task = asyncio.create_task(_ping())
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == "pong":
                                continue
                            data = orjson.loads(msg.data)
                            last_data = time.monotonic()
                            self._feed(cfg.trade_key, data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                        if time.monotonic() - last_data > self._idle_max:
                            logger.warning("WS %s idle → reconnect", name)
                            break
            except asyncio.CancelledError:
                if bitget_ping_task:
                    bitget_ping_task.cancel()
                return
            except Exception as e:
                logger.error("WS %s error: %r", name, e)
            finally:
                if bitget_ping_task:
                    bitget_ping_task.cancel()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    # --- Coinbase --------------------------------------------------------

    async def _run_coinbase(self) -> None:
        backoff = self._backoff_initial
        while self._running:
            try:
                assert self._session is not None
                async with self._session.ws_connect(COINBASE_ADVANCED_WS, heartbeat=20) as ws:
                    await ws.send_json({
                        "type": "subscribe",
                        "product_ids": list(self._coinbase_products),
                        "channel": "market_trades",
                    })
                    logger.info("WS coinbase connected products=%s", self._coinbase_products)
                    backoff = self._backoff_initial
                    last_data = time.monotonic()
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = orjson.loads(msg.data)
                            last_data = time.monotonic()
                            if data.get("channel") != "market_trades":
                                continue
                            # Coinbase delivers one frame that can span multiple
                            # product_ids; split + feed per-product.
                            by_product: dict[str, dict] = {}
                            for ev in data.get("events") or []:
                                for t in ev.get("trades") or []:
                                    pid = t.get("product_id")
                                    if not pid:
                                        continue
                                    bucket = by_product.setdefault(pid, {
                                        "channel": "market_trades",
                                        "timestamp": data.get("timestamp"),
                                        "events": [{"type": ev.get("type", "update"), "trades": []}],
                                    })
                                    bucket["events"][0]["trades"].append(t)
                            for pid, sub in by_product.items():
                                key = StreamKey("coinbase_advanced_trade_ws", "coinbase",
                                                pid, StreamType.TRADE, "market_trades")
                                self._feed(key, sub)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                        if time.monotonic() - last_data > self._idle_max:
                            break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("WS coinbase error: %r", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    # --- Deribit ---------------------------------------------------------

    async def _deribit_try_auth(self, ws: aiohttp.ClientWebSocketResponse) -> bool:
        """Send public/auth and await the reply.

        Returns True on success. NEVER logs the access_token or the
        ``client_secret``; logs only scope and expires_in so audit trail
        captures the session identity without leaking the token.
        """
        creds = self._deribit_credentials
        if creds is None:
            return False
        await ws.send_json({
            "jsonrpc": "2.0", "id": 1, "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
            },
        })
        try:
            # Short deadline — auth reply should arrive within a few hundred ms.
            deadline = 5.0
            while True:
                msg = await asyncio.wait_for(ws.receive(), timeout=deadline)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                d = orjson.loads(msg.data)
                if d.get("id") != 1:
                    continue
                if "error" in d:
                    logger.warning(
                        "deribit auth error code=%s message=%s",
                        d["error"].get("code"), d["error"].get("message"),
                    )
                    return False
                result = d.get("result") or {}
                if not result.get("access_token"):
                    return False
                # SCOPE audit — no token material.
                logger.info(
                    "deribit auth ok — scope=%s expires_in=%s",
                    result.get("scope"), result.get("expires_in"),
                )
                return True
        except asyncio.TimeoutError:
            logger.warning("deribit auth timed out")
            return False
        except Exception as e:
            logger.error("deribit auth exception: %r", e)
            return False

    def _deribit_resolve_interval(self, authed: bool) -> str:
        if self._deribit_interval == "auto":
            return "raw" if authed else "100ms"
        if self._deribit_interval == "raw" and not authed:
            logger.warning("deribit interval=raw but not authed, falling back to .100ms")
            return "100ms"
        return self._deribit_interval

    async def _run_deribit(self) -> None:
        backoff = self._backoff_initial
        while self._running:
            try:
                assert self._session is not None
                async with self._session.ws_connect(DERIBIT_WS, heartbeat=20) as ws:
                    # Auth must happen BEFORE subscribe so raw subscriptions
                    # are accepted. Re-auth every reconnect — tokens don't
                    # survive disconnect.
                    authed = False
                    if self._deribit_credentials is not None:
                        authed = await self._deribit_try_auth(ws)

                    interval = self._deribit_resolve_interval(authed)
                    channels = [f"trades.{inst}.{interval}" for inst in self._deribit_instruments]
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 2, "method": "public/subscribe",
                        "params": {"channels": channels},
                    })
                    # Server-initiated heartbeat; we respond to test_request below.
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 3, "method": "public/set_heartbeat",
                        "params": {"interval": 30},
                    })
                    logger.info(
                        "WS deribit connected channels=%d interval=%s authed=%s",
                        len(channels), interval, authed,
                    )
                    backoff = self._backoff_initial
                    last_data = time.monotonic()
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = orjson.loads(msg.data)
                            last_data = time.monotonic()
                            if data.get("method") == "heartbeat":
                                params = data.get("params") or {}
                                if params.get("type") == "test_request":
                                    await ws.send_json({
                                        "jsonrpc": "2.0", "id": 99,
                                        "method": "public/test", "params": {},
                                    })
                                continue
                            params = data.get("params") or {}
                            channel = params.get("channel") or ""
                            if not channel.startswith("trades."):
                                continue
                            # channel: "trades.<instrument>.<interval>"
                            parts = channel.split(".")
                            if len(parts) < 3:
                                continue
                            instrument = parts[1]
                            key = StreamKey("deribit_v2_public_ws", "deribit",
                                            instrument, StreamType.TRADE, "trades")
                            self._feed(key, data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                        if time.monotonic() - last_data > self._idle_max:
                            break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("WS deribit error: %r", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)
