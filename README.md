# cache-patterns

Four classic caching patterns in pure Python — TTL, hit/miss stats and a
background flush, all in a single dependency-free file.

| Pattern | Read | Write | Risk |
|---|---|---|---|
| `CacheAside` | app: miss -> DB -> fill cache | write to DB, invalidate cache | forget to invalidate -> incoherence |
| `ReadThrough` | cache goes to the DB itself | write to DB, invalidate | the cache is a mandatory component |
| `WriteThrough` | cache only | synchronously to cache + DB | write latency = DB latency |
| `WriteBehind` | cache only | instantly to cache, batched to DB from a background thread | last changes lost on crash |

Choosing a strategy comes down to one question: what are you willing to lose —
consistency, write latency, or code simplicity. Most services do fine with
Cache-Aside and a sane TTL policy.

## Usage

```bash
python3 cache_patterns.py --demo   # a scenario for each pattern
python3 cache_patterns.py --test   # mini test suite
```

## As a library

```python
from cache_patterns import FakeDB, CacheAside, WriteBehind

db = FakeDB({"user:1": "alice"})
cache = CacheAside(db, ttl=60)

cache.get("user:1")   # miss -> DB
cache.get("user:1")   # hit
print(cache.stats)    # hit=1 miss=1 (hit rate 50%)
```

No dependencies, Python 3.10+.

## License

MIT
