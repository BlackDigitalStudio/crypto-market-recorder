"""Recorder v2 entrypoint — production-grade, multi-venue WS + REST.

Usage::

    python scripts/record_data_v2.py                    # default config
    CHRONOS_ROOT=/path/data_v2 python scripts/record_data_v2.py
    CHRONOS_CONFIG=/path/streams.yaml python scripts/record_data_v2.py

Default config (no YAML needed):

- Binance futures BTCUSDT + ETHUSDT: depth@100ms (L100 maintained book +
  reconcile + secondary endpoint dedupe), aggTrade, markPrice@1s,
  @forceOrder (P13), derivatives poll.
- Bybit / OKX / Bitget / Gate.io BTC trades (same as legacy recorder).
- Coinbase BTC-USD + Deribit BTC-PERPETUAL (new in v2).

The production recorder (`scripts/record_data.py`) is independent — both
may run side by side during v2 bring-up. v2 writes under
``<DATA_DIR>/data_v2/`` by default to stay isolated on disk.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

# Allow running as `python scripts/record_data_v2.py` from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chronos import Recorder  # noqa: E402
from chronos.gateway import DeribitCredentials, Gateway  # noqa: E402

logger = logging.getLogger("chronos")


def _default_root() -> Path:
    override = os.environ.get("CHRONOS_ROOT")
    if override:
        return Path(override)
    default_home = Path("/home/scalper/scalper-bot/data_v2")
    return default_home


def _load_config_env() -> None:
    """Load key=value pairs from ./config.env into os.environ (if not set).

    Same contract as the production recorder — supports credentials that
    must never be committed. ``config.env`` is already in .gitignore.
    """
    cfg = Path("/home/scalper/scalper-bot/config.env")
    if not cfg.exists():
        return
    for line in cfg.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip()
        if k and v and k not in os.environ:
            os.environ[k] = v


def _deribit_credentials_from_env() -> DeribitCredentials | None:
    cid = os.environ.get("DERIBIT_CLIENT_ID", "").strip()
    sec = os.environ.get("DERIBIT_CLIENT_SECRET", "").strip()
    if cid and sec:
        return DeribitCredentials(client_id=cid, client_secret=sec)
    return None


async def _main() -> None:
    log_fmt = "%(asctime)s.%(msecs)03d %(levelname)-5s [%(name)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    _load_config_env()
    root = _default_root()
    root.mkdir(parents=True, exist_ok=True)
    logger.info("chronos root=%s", root)
    deribit_creds = _deribit_credentials_from_env()
    if deribit_creds is not None:
        logger.info("deribit credentials loaded — will attempt .raw")
    else:
        logger.info("deribit credentials not set — will use .100ms")

    recorder = Recorder(root)
    gateway = Gateway(recorder)

    # --- Binance futures -------------------------------------------------
    for symbol in ("BTCUSDT", "ETHUSDT"):
        gateway.add_binance_futures(
            symbol,
            snapshot_levels=100,
            maintain_book=True,
            subscribe_force_order=True,
            # fstream3 replica returns a WAF 302 from Tokyo IPs; leave the
            # secondary endpoint off by default until we pick a working
            # replica (or land P8 with a real second host).
            subscribe_secondary_endpoint=False,
            reconcile_interval_sec=900.0,
            derivatives_poll_interval_sec=15.0,
        )

    # --- Cross-venue trades ---------------------------------------------
    gateway.add_bybit_trades("BTCUSDT")
    gateway.add_okx_trades("BTC-USDT-SWAP")
    gateway.add_bitget_trades("BTCUSDT")
    gateway.add_gateio_trades("BTC_USDT")
    gateway.add_coinbase_trades("BTC-USD")
    gateway.add_deribit_trades("BTC-PERPETUAL", credentials=deribit_creds)

    await gateway.start()

    shutdown = asyncio.Event()

    def _stop() -> None:
        logger.info("shutdown signal received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    logger.info("chronos running — Ctrl+C to stop")
    await shutdown.wait()

    logger.info("stopping gateway + recorder")
    await gateway.stop()
    logger.info("done")


if __name__ == "__main__":
    asyncio.run(_main())
