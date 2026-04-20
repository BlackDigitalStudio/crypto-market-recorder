"""P6/P7/P10/P11 — maintained order book.

:class:`OrderBookV2` is a pure state container. It owns no I/O — the gateway
layer fetches REST snapshots and feeds them in via
:meth:`OrderBookV2.restore_from_snapshot`; WS diffs are fed in via
:meth:`OrderBookV2.apply_diff` which returns a list of :class:`BookChange`
objects enriched with ``insert`` / ``update`` / ``remove`` classification.

Three downstream products are derivable from the book:

- **P6 depth snapshot** — top-N (price, qty) levels per side, emitted at
  WS diff cadence (:meth:`snapshot_row`).
- **P10 checkpoint** — *every* maintained level serialized once every N
  minutes (:meth:`checkpoint_rows`).
- **P11 REST reconcile** — compare maintained state against a freshly
  pulled REST snapshot (:meth:`compare_to_rest`) to catch silent drift
  that per-message ``pu`` validation cannot detect.

Book contents are kept as ``dict[float, float]`` (price → qty). Float
prices are exact for the decimal grids Binance publishes; if a venue
requires decimal ticks below float precision we can switch to ``Decimal``
in a later bump (no schema change — the wire types stay ``float64``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .timestamps import Timestamps


@dataclass(frozen=True, slots=True)
class BookChange:
    """One classified level change emitted by :meth:`OrderBookV2.apply_diff`."""

    side: str           # "bid" | "ask"
    price: float
    qty: float          # new qty (0 on remove)
    update_type: str    # "insert" | "update" | "remove"


@dataclass(frozen=True, slots=True)
class DriftFinding:
    """One level where maintained state disagrees with a REST snapshot."""

    kind: str           # "qty_mismatch" | "missing_local" | "missing_rest"
    side: str
    price: float
    local_qty: float | None
    rest_qty: float | None


def _floats(entries: Iterable) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for e in entries:
        if not e or len(e) < 2:
            continue
        try:
            out.append((float(e[0]), float(e[1])))
        except (TypeError, ValueError):
            continue
    return out


class OrderBookV2:
    """L-flat maintained order book for one ``(source_id, symbol)`` pair."""

    __slots__ = (
        "symbol",
        "_bids",
        "_asks",
        "_last_update_id",
        "_synced",
        "_first_applied_update_id",
        "_applied_diffs",
    )

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_update_id: int | None = None
        self._synced: bool = False
        self._first_applied_update_id: int | None = None
        self._applied_diffs: int = 0

    # --- state queries ---

    @property
    def synced(self) -> bool:
        return self._synced

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    @property
    def applied_diffs(self) -> int:
        return self._applied_diffs

    def __len__(self) -> int:
        return len(self._bids) + len(self._asks)

    # --- REST snapshot init ---

    def restore_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Initialise / resync from a Binance-style REST depth snapshot.

        Expected shape: ``{"lastUpdateId": int, "bids": [[p,q], ...],
        "asks": [[p,q], ...]}``. Invalidates prior state.
        """
        self._bids.clear()
        self._asks.clear()
        for p, q in _floats(snapshot.get("bids") or []):
            if q > 0:
                self._bids[p] = q
        for p, q in _floats(snapshot.get("asks") or []):
            if q > 0:
                self._asks[p] = q
        last_id = snapshot.get("lastUpdateId")
        if last_id is None:
            last_id = snapshot.get("last_update_id")
        try:
            self._last_update_id = int(last_id) if last_id is not None else None
        except (TypeError, ValueError):
            self._last_update_id = None
        self._synced = True
        self._first_applied_update_id = None
        self._applied_diffs = 0

    # --- WS diff application ---

    def apply_diff(self, msg: dict[str, Any]) -> list[BookChange]:
        """Apply one ``depthUpdate`` frame; return classified changes.

        Behaviour:

        - Unsynced (no REST snapshot loaded): returns ``[]``.
        - Stale diff (``u <= last_update_id``): returns ``[]`` — Binance
          recommends discarding diffs already covered by the snapshot.
        - Gap in ``pu``/``u`` sequence: **not handled here**; the P3
          validator emits the gap signal and the gateway is expected to
          resync by calling :meth:`restore_from_snapshot` again.
        """
        if not self._synced:
            return []
        u = msg.get("u")
        try:
            u_int = int(u) if u is not None else None
        except (TypeError, ValueError):
            u_int = None
        if (
            u_int is not None
            and self._last_update_id is not None
            and u_int <= self._last_update_id
        ):
            return []

        changes: list[BookChange] = []
        for side_key, side, book in (("b", "bid", self._bids), ("a", "ask", self._asks)):
            for p, q in _floats(msg.get(side_key) or []):
                prev = book.get(p)
                if q == 0.0:
                    if prev is not None:
                        del book[p]
                        changes.append(BookChange(side, p, 0.0, "remove"))
                    # qty=0 for a level we don't track is a no-op.
                else:
                    book[p] = q
                    if prev is None:
                        changes.append(BookChange(side, p, q, "insert"))
                    elif prev != q:
                        changes.append(BookChange(side, p, q, "update"))
                    # prev == q is a wire-level dup; no change emitted.
        if u_int is not None:
            self._last_update_id = u_int
            if self._first_applied_update_id is None:
                self._first_applied_update_id = u_int
        self._applied_diffs += 1
        return changes

    # --- inspection ---

    def top_n(self, n: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """Return (bids desc, asks asc), capped at ``n`` each."""
        bids = sorted(self._bids.items(), key=lambda kv: -kv[0])[:n]
        asks = sorted(self._asks.items(), key=lambda kv: kv[0])[:n]
        return bids, asks

    def best_bid(self) -> tuple[float, float] | None:
        if not self._bids:
            return None
        p = max(self._bids)
        return (p, self._bids[p])

    def best_ask(self) -> tuple[float, float] | None:
        if not self._asks:
            return None
        p = min(self._asks)
        return (p, self._asks[p])

    # --- row builders ---

    def snapshot_row(
        self,
        *,
        levels: int,
        ts: Timestamps,
        source_id: str,
        first_update_id: int | None = None,
        prev_final_update_id: int | None = None,
    ) -> dict[str, Any]:
        """Build one DEPTH_SNAPSHOT row (P6)."""
        bids, asks = self.top_n(levels)
        bp = [0.0] * levels
        bq = [0.0] * levels
        ap = [0.0] * levels
        aq = [0.0] * levels
        for i, (p, q) in enumerate(bids):
            bp[i] = p
            bq[i] = q
        for i, (p, q) in enumerate(asks):
            ap[i] = p
            aq[i] = q
        return {
            "local_ts_us": ts.local_ts_us,
            "exchange_event_ts_us": ts.exchange_event_ts_us,
            "exchange_trans_ts_us": ts.exchange_trans_ts_us,
            "source_id": source_id,
            "symbol": self.symbol,
            "depth_levels": levels,
            "first_update_id": first_update_id,
            "final_update_id": self._last_update_id,
            "prev_final_update_id": prev_final_update_id,
            "bid_prices": bp,
            "bid_qtys": bq,
            "ask_prices": ap,
            "ask_qtys": aq,
        }

    def checkpoint_rows(
        self,
        *,
        ts: Timestamps,
        source_id: str,
    ) -> list[dict[str, Any]]:
        """Serialize *every* maintained level for a P10 checkpoint.

        One row per level. ``dump_id`` = ``ts.local_ts_us`` so rows from a
        single dump share a groupable key even across compaction.
        """
        dump_id = ts.local_ts_us
        rows: list[dict[str, Any]] = []
        preamble = {
            "local_ts_us": ts.local_ts_us,
            "exchange_event_ts_us": ts.exchange_event_ts_us,
            "exchange_trans_ts_us": ts.exchange_trans_ts_us,
            "source_id": source_id,
            "symbol": self.symbol,
            "dump_id": dump_id,
            "last_update_id": self._last_update_id,
        }
        for p in sorted(self._bids, reverse=True):
            rows.append({**preamble, "side": "bid", "price": p, "qty": self._bids[p]})
        for p in sorted(self._asks):
            rows.append({**preamble, "side": "ask", "price": p, "qty": self._asks[p]})
        return rows

    def change_to_depth_diff_row(
        self,
        change: BookChange,
        *,
        ts: Timestamps,
        source_id: str,
        first_update_id: int | None,
        final_update_id: int | None,
        prev_final_update_id: int | None,
    ) -> dict[str, Any]:
        """Turn one classified change into a DEPTH_DIFF row (P7)."""
        return {
            "local_ts_us": ts.local_ts_us,
            "exchange_event_ts_us": ts.exchange_event_ts_us,
            "exchange_trans_ts_us": ts.exchange_trans_ts_us,
            "source_id": source_id,
            "symbol": self.symbol,
            "first_update_id": first_update_id,
            "final_update_id": final_update_id,
            "prev_final_update_id": prev_final_update_id,
            "side": change.side,
            "price": change.price,
            "qty": change.qty,
            "update_type": change.update_type,
        }

    # --- P11 REST reconcile ---

    def compare_to_rest(
        self,
        snapshot: dict[str, Any],
        *,
        qty_tolerance: float = 0.0,
        max_price_levels: int | None = None,
    ) -> list[DriftFinding]:
        """Return levels that differ between maintained state and REST.

        ``qty_tolerance`` lets the caller ignore microscopic rounding (e.g.
        ``1e-9``) that can arise when the exchange reissues an equivalent
        level. ``max_price_levels`` optionally caps how deep we compare —
        REST often returns 1000 levels while we may only maintain the top
        500, so levels deeper than maintained should not be flagged.
        """
        rest_bids = dict(_floats(snapshot.get("bids") or []))
        rest_asks = dict(_floats(snapshot.get("asks") or []))

        if max_price_levels is not None:
            rest_bids = dict(sorted(rest_bids.items(), key=lambda kv: -kv[0])[:max_price_levels])
            rest_asks = dict(sorted(rest_asks.items(), key=lambda kv: kv[0])[:max_price_levels])

        findings: list[DriftFinding] = []
        for side, local_book, rest_book in (
            ("bid", self._bids, rest_bids),
            ("ask", self._asks, rest_asks),
        ):
            for p, q_local in local_book.items():
                q_rest = rest_book.get(p)
                if q_rest is None:
                    findings.append(DriftFinding("missing_rest", side, p, q_local, None))
                elif abs(q_local - q_rest) > qty_tolerance:
                    findings.append(DriftFinding("qty_mismatch", side, p, q_local, q_rest))
            for p, q_rest in rest_book.items():
                if p not in local_book:
                    findings.append(DriftFinding("missing_local", side, p, None, q_rest))
        return findings
