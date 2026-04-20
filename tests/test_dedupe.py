"""P9: LRU dedupe cache."""
from __future__ import annotations

from chronos.dedupe import DedupeCache


def test_first_sight_returns_false():
    c = DedupeCache(10)
    assert c.check_and_mark(("a", 1)) is False


def test_second_sight_returns_true():
    c = DedupeCache(10)
    c.check_and_mark(("a", 1))
    assert c.check_and_mark(("a", 1)) is True


def test_distinct_keys_independent():
    c = DedupeCache(10)
    assert c.check_and_mark(("a", 1)) is False
    assert c.check_and_mark(("a", 2)) is False
    assert c.check_and_mark(("b", 1)) is False
    assert c.check_and_mark(("a", 1)) is True


def test_lru_eviction_bounds_size():
    c = DedupeCache(maxsize=3)
    for i in range(10):
        c.check_and_mark(("k", i))
    assert len(c) == 3
    # Oldest entries should have been evicted → first-sight again.
    assert c.check_and_mark(("k", 0)) is False


def test_touch_refreshes_recency():
    c = DedupeCache(maxsize=3)
    c.check_and_mark(("k", 0))
    c.check_and_mark(("k", 1))
    c.check_and_mark(("k", 2))
    # Touch key 0 so it becomes most-recent; 1 should evict on insert.
    assert c.check_and_mark(("k", 0)) is True
    c.check_and_mark(("k", 3))
    # 1 was least-recent and should have been evicted.
    assert c.check_and_mark(("k", 1)) is False
    # 0 stays.
    assert c.check_and_mark(("k", 0)) is True


def test_clear():
    c = DedupeCache(10)
    c.check_and_mark(("a", 1))
    c.clear()
    assert c.check_and_mark(("a", 1)) is False
