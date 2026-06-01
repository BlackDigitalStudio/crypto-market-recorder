"""Recorder v2 entrypoint — production-grade, multi-venue WS + REST.

Usage::

    python scripts/record_data_v2.py                    # default config
    CHRONOS_ROOT=/path/data_v2 python scripts/record_data_v2.py
    CHRONOS_CONFIG=/path/streams.yaml python scripts/record_data_v2.py

    # Override the Binance symbol set + book depth via env (defaults keep
    # the original BTC/ETH @ L100 full-book behaviour):
    CHRONOS_BINANCE_SYMBOLS="BTCUSDT,BTCUSDC,ETHUSDT,ETHUSDC" \
    CHRONOS_DEPTH_LEVELS=20 CHRONOS_BOOK_MAX_LEVELS=100 CHRONOS_SEED_LIMIT=100 \
        python scripts/record_data_v2.py

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


_REPO_ROOT = Path(__file__).resolve().parent.parent


def _default_root() -> Path:
    override = os.environ.get("CHRONOS_ROOT")
    if override:
        return Path(override)
    return _REPO_ROOT / "data"


def _load_config_env() -> None:
    """Load key=value pairs from repo-root ``config.env`` into os.environ.

    Credentials never belong in the tracked source tree — ``config.env``
    is in ``.gitignore``. We look relative to this script so Chronos can
    ship as a self-contained deployment bundle.
    """
    cfg = _REPO_ROOT / "config.env"
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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    raw = raw.strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("ignoring invalid %s=%r (expected int)", name, raw)
        return default


def _binance_symbols() -> list[str]:
    """Binance USDⓈ-M symbols to record. Override via ``CHRONOS_BINANCE_SYMBOLS``
    (comma-separated, e.g. ``BTCUSDT,BTCUSDC,ETHUSDT``). Default: BTC/ETH USDT."""
    raw = os.environ.get("CHRONOS_BINANCE_SYMBOLS", "")
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return syms or ["BTCUSDT", "ETHUSDT"]


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
    # Symbol set + book depth are env-overridable (defaults preserve the
    # original BTC/ETH @ L100 full-book behaviour):
    #   CHRONOS_BINANCE_SYMBOLS  comma-separated, e.g. "BTCUSDT,BTCUSDC,ETHUSDT"
    #   CHRONOS_DEPTH_LEVELS     emitted depth_snapshot levels   (default 100)
    #   CHRONOS_BOOK_MAX_LEVELS  cap maintained book per side    (default 0 = no cap)
    #   CHRONOS_SEED_LIMIT       REST snapshot/reconcile depth   (default 1000)
    symbols = _binance_symbols()
    snapshot_levels = _env_int("CHRONOS_DEPTH_LEVELS", 100)
    book_max_levels = _env_int("CHRONOS_BOOK_MAX_LEVELS", 0) or None
    seed_limit = _env_int("CHRONOS_SEED_LIMIT", 1000)
    logger.info(
        "binance: symbols=%s depth=L%d book_max_levels=%s seed_limit=%d",
        symbols, snapshot_levels, book_max_levels, seed_limit,
    )
    for symbol in symbols:
        gateway.add_binance_futures(
            symbol,
            snapshot_levels=snapshot_levels,
            maintain_book=True,
            subscribe_force_order=True,
            # fstream3 replica returns a WAF 302 from Tokyo IPs; leave the
            # secondary endpoint off by default until we pick a working
            # replica (or land P8 with a real second host).
            subscribe_secondary_endpoint=False,
            reconcile_interval_sec=900.0,
            derivatives_poll_interval_sec=15.0,
            book_max_levels=book_max_levels,
            seed_limit=seed_limit,
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
