#!/usr/bin/env python3
"""
cache-patterns — four classic caching patterns in pure Python.

    Cache-Aside    — the app manages the cache explicitly (works for ~90% of services)
    Read-Through   — the cache itself goes to the DB on a miss
    Write-Through  — writes go to cache and DB synchronously
    Write-Behind   — write to cache, flush to the DB asynchronously in batches

Choosing a strategy comes down to one question: what are you willing to lose —
consistency, write latency, or code simplicity.

Usage:
    python3 cache_patterns.py --demo   # scenario with hit/miss stats and flush
    python3 cache_patterns.py --test   # mini test suite
"""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class FakeDB:
    """A toy "database" with request counters."""

    def __init__(self, rows: Optional[Dict[str, Any]] = None) -> None:
        self.rows: Dict[str, Any] = dict(rows or {})
        self.reads = 0
        self.writes = 0

    def get(self, key: str) -> Any:
        self.reads += 1
        return self.rows.get(key)

    def put(self, key: str, value: Any) -> None:
        self.writes += 1
        self.rows[key] = value


@dataclass
class Entry:
    value: Any
    expires_at: float = 0.0

    def alive(self, now: float) -> bool:
        return self.expires_at == 0.0 or now < self.expires_at


class BaseCache:
    """Shared plumbing: TTL, hit/miss stats, thread safety."""

    def __init__(self, db: FakeDB, ttl: float = 0.0) -> None:
        self.db = db
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
        self._store: Dict[str, Entry] = {}
        self._lock = threading.Lock()

    def _now(self) -> float:
        return time.monotonic()

    def _get_fresh(self, key: str) -> Optional[Entry]:
        entry = self._store.get(key)
        if entry is not None and entry.alive(self._now()):
            return entry
        self._store.pop(key, None)
        return None

    def _put(self, key: str, value: Any) -> None:
        ttl = self.ttl
        exp = (self._now() + ttl) if ttl else 0.0
        self._store[key] = Entry(value, exp)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    @property
    def stats(self) -> str:
        total = self.hits + self.misses
        rate = (self.hits / total * 100) if total else 0.0
        return f"hit={self.hits} miss={self.misses} (hit rate {rate:.0f}%)"


class CacheAside(BaseCache):
    """The app does it all: miss -> DB -> put into cache."""

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._get_fresh(key)
        if entry is not None:
            self.hits += 1
            return entry.value
        self.misses += 1
        value = self.db.get(key)
        if value is not None:
            with self._lock:
                self._put(key, value)
        return value

    def put(self, key: str, value: Any) -> None:
        self.db.put(key, value)      # the DB is the source of truth
        self.invalidate(key)         # classic: write to DB, invalidate the cache


class ReadThrough(BaseCache):
    """The cache itself goes to the DB on a miss; the app only sees the cache."""

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._get_fresh(key)
            if entry is not None:
                self.hits += 1
                return entry.value
            self.misses += 1
            value = self.db.get(key)
            if value is not None:
                self._put(key, value)
            return value

    def put(self, key: str, value: Any) -> None:
        self.db.put(key, value)
        self.invalidate(key)


class WriteThrough(BaseCache):
    """Write: to cache and DB synchronously. Reads come from the cache."""

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._put(key, value)
        self.db.put(key, value)

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._get_fresh(key)
        if entry is not None:
            self.hits += 1
            return entry.value
        self.misses += 1
        return self.db.get(key)


class WriteBehind(WriteThrough):
    """Write: instantly to cache, flushed to the DB in batches from a background thread.

    The cache temporarily becomes the source of truth: on a crash the last
    flush_interval worth of changes can be lost — a conscious trade-off.
    """

    def __init__(self, db: FakeDB, ttl: float = 0.0, flush_interval: float = 0.2) -> None:
        super().__init__(db, ttl)
        self.flush_interval = flush_interval
        self.queue: List[Tuple[str, Any]] = []
        self.flushed = 0
        self._stop = threading.Event()

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._put(key, value)
            self.queue.append((key, value))

    def start(self) -> None:
        threading.Thread(target=self._worker, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _worker(self) -> None:
        while not self._stop.wait(self.flush_interval):
            self.flush()

    def flush(self) -> int:
        with self._lock:
            batch, self.queue = self.queue, []
        for key, value in batch:
            self.db.put(key, value)
        self.flushed += len(batch)
        return len(batch)


def demo() -> None:
    print("=== Cache-Aside: 1st request is a miss, 2nd comes from cache ===")
    db = FakeDB({"user:1": "alice"})
    c = CacheAside(db)
    print(c.get("user:1"), "| db reads:", db.reads)
    print(c.get("user:1"), "| db reads:", db.reads)
    print(c.stats)

    print("\n=== Write-Through: write goes to cache and DB at once ===")
    db2 = FakeDB()
    wt = WriteThrough(db2)
    wt.put("cfg:limit", 42)
    print("right after put ->", wt.get("cfg:limit"), "| db writes:", db2.writes)

    print("\n=== Write-Behind: write to cache, batched flush to DB ===")
    db3 = FakeDB()
    wb = WriteBehind(db3)
    wb.start()
    for i in range(5):
        wb.put(f"metric:{i}", i)
    print("put 5 values, db writes so far:", db3.writes)
    time.sleep(0.5)
    print("after ~0.5s db writes:", db3.writes, "| flushed:", wb.flushed)
    wb.stop()
    wb.flush()

    print("\n=== TTL: entry expires after the timeout ===")
    db4 = FakeDB({"tmp": "value"})
    ttl_cache = CacheAside(db4, ttl=0.15)
    print(ttl_cache.get("tmp"), end=" -> ")
    time.sleep(0.2)
    print(ttl_cache.get("tmp"), "(miss again, value re-read)")


def test() -> None:
    db = FakeDB({"k": "v"})
    c = CacheAside(db)
    assert c.get("k") == "v" and db.reads == 1
    assert c.get("k") == "v" and db.reads == 1          # from cache
    c.put("k", "v2")
    assert c.get("k") == "v2" and db.reads == 2          # invalidation worked

    wt = WriteThrough(FakeDB())
    wt.put("a", 1)
    assert wt.db.rows["a"] == 1 and wt.get("a") == 1

    wb = WriteBehind(FakeDB())
    wb.put("b", 2)
    assert wb.get("b") == 2 and wb.db.rows.get("b") is None  # not in the DB yet
    assert wb.flush() == 1 and wb.db.rows["b"] == 2

    ttl = CacheAside(FakeDB({"t": "x"}), ttl=0.05)
    assert ttl.get("t") == "x"
    time.sleep(0.08)
    assert ttl.get("t") == "x" and ttl.misses == 2
    print("ok: all mini-tests passed")


def main() -> int:
    args = set(sys.argv[1:])
    if "--test" in args:
        test()
    else:
        demo()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
