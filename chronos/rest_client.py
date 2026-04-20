"""Minimal REST client for v2 gateway.

Covers the small handful of calls the archive path actually needs:

- Binance futures L1000 depth snapshot (seeds + reconciles
  :class:`OrderBookV2`).
- Binance futures open interest and top-trader long/short ratio
  (fills :attr:`StreamType.DERIVATIVES_POLL`).

Everything is narrow on purpose — full REST SDKs belong in the live
trading bot, not the archive. Uses aiohttp for shared-loop semantics.
"""
from __future__ import annotations

import logging
from typing import Any

import aiohttp
import orjson

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=3, sock_read=5)


class BinanceFuturesREST:
    """Narrow client for public futures endpoints (no signing required)."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        base_url: str = "https://fapi.binance.com",
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> None:
        self._session = session
        self._base = base_url.rstrip("/")
        self._timeout = timeout or _DEFAULT_TIMEOUT

    async def depth_snapshot(self, symbol: str, *, limit: int = 1000) -> dict[str, Any]:
        """``GET /fapi/v1/depth`` — returns ``{lastUpdateId, bids, asks}``."""
        url = f"{self._base}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": limit}
        async with self._session.get(url, params=params, timeout=self._timeout) as r:
            data = await r.json(loads=orjson.loads)
            if r.status != 200:
                logger.error("depth snapshot %s → %d: %s", symbol, r.status, data)
            return data

    async def open_interest(self, symbol: str) -> float | None:
        url = f"{self._base}/fapi/v1/openInterest"
        params = {"symbol": symbol}
        async with self._session.get(url, params=params, timeout=self._timeout) as r:
            if r.status != 200:
                return None
            data = await r.json(loads=orjson.loads)
        try:
            return float(data.get("openInterest", 0))
        except (TypeError, ValueError):
            return None

    async def top_long_short_account_ratio(
        self,
        symbol: str,
        *,
        period: str = "5m",
    ) -> dict[str, Any] | None:
        url = f"{self._base}/futures/data/topLongShortAccountRatio"
        params = {"symbol": symbol, "period": period, "limit": 1}
        async with self._session.get(url, params=params, timeout=self._timeout) as r:
            if r.status != 200:
                return None
            data = await r.json(loads=orjson.loads)
        if isinstance(data, list) and data:
            return data[0]
        return None
