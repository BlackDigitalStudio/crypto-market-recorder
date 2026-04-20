"""P9 — LRU dedupe cache for multi-endpoint subscriptions.

When the gateway subscribes to two WS endpoints of the same venue for
redundancy (``fstream.binance.com`` + ``ws-fapi.binance.com``, or multi-
region Bybit), both deliver the same events. We want exactly one parquet
row per event; the first arrival wins, the second is silently dropped.

Key is intentionally caller-defined so the same cache can serve different
stream types under one process: for depth diffs use ``(stream_key,
final_update_id)``; for trades use ``(stream_key, trade_id)``; for
aggregated trades use ``(stream_key, aggregate_id)``.

Thread-safe. Bounded. The default size ``100_000`` holds roughly one
hour of Binance depth diffs at 10 Hz across two endpoints — ample for
the intended 1-2s inter-endpoint jitter window.
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Hashable


class DedupeCache:
    """LRU set; :meth:`check_and_mark` returns True iff already seen."""

    __slots__ = ("_seen", "_max", "_lock")

    def __init__(self, maxsize: int = 100_000) -> None:
        self._seen: "OrderedDict[Hashable, int]" = OrderedDict()
        self._max = max(1, int(maxsize))
        self._lock = threading.Lock()

    def check_and_mark(self, key: Hashable) -> bool:
        """Return True if ``key`` was already seen; mark it seen either way.

        LRU eviction keeps the set bounded; an evicted key that later
        reappears is treated as new.
        """
        with self._lock:
            if key in self._seen:
                self._seen.move_to_end(key)
                return True
            self._seen[key] = 1
            if len(self._seen) > self._max:
                self._seen.popitem(last=False)
            return False

    def __len__(self) -> int:
        with self._lock:
            return len(self._seen)

    def clear(self) -> None:
        with self._lock:
            self._seen.clear()
