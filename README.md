# gocache

Multi-tier caching for Go: an in-memory L1 in front of a distributed L2, kept in sync across
instances by a message bus. Namespaces, tag invalidation, stampede protection, grace periods,
factory timeouts and distributed atomic locks.

```go
user, err := gocache.GetOrSet(ctx, cache, "user:42", func(ctx context.Context) (User, error) {
	return db.FindUser(ctx, 42)
}, gocache.WithTags("users"))
```

One database call per instance, no matter how many goroutines ask at once. Served from process
memory on the way back. Invalidated everywhere with `cache.DeleteByTag(ctx, "users")`.

## Install

```bash
go get github.com/swissy-dev/gocache
```

Requires Go 1.25.

## Quick start

The smallest useful setup — one process, memory only:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/memory"
)

type Article struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func main() {
	cache, err := gocache.New(
		gocache.WithL1(memory.New(memory.WithMaxEntries(10_000))),
		gocache.WithDefaultTTL(5*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cache.Close() }()

	ctx := context.Background()

	article, err := gocache.GetOrSet(ctx, cache, "article:1", func(ctx context.Context) (Article, error) {
		return Article{ID: 1, Title: "Caching in Go"}, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(article.Title)
}
```

A runnable version lives in [`example/`](example/) — `go run ./example`.

## How it works

Every entry is stored with a *logical* expiry (its TTL) and, if grace is configured, kept
physically a little longer by the grace period, so a stale copy is still available if the source
fails.

- **L1** is process-local memory. Fast, but every instance has its own.
- **L2** is shared — Redis or SQL. Slower, but authoritative.
- **The bus** tells peers to drop an L1 entry when someone writes or deletes it. Without it, each
  instance serves its own stale copy until the TTL runs out.

A read checks L1, then L2 (backfilling L1 on the way), then misses. A write goes to L2 first, then
L1, then publishes an invalidation. You can run L1 alone, L2 alone, or both; the bus requires both.

## Setting up

### One process

Memory only. Nothing shared, nothing to invalidate remotely:

```go
cache, err := gocache.New(
	gocache.WithL1(memory.New(memory.WithMaxEntries(10_000))),
	gocache.WithDefaultTTL(5*time.Minute),
)
```

### Multiple instances, Redis

L1 for speed, Redis for sharing, Redis Pub/Sub to keep the L1s honest:

```go
import (
	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/bus/redisbus"
	"github.com/swissy-dev/gocache/driver/memory"
	redisdrv "github.com/swissy-dev/gocache/driver/redisdriver"
	"github.com/redis/go-redis/v9"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

cache, err := gocache.New(
	gocache.WithL1(memory.New(memory.WithMaxEntries(10_000))),
	gocache.WithL2(redisdrv.New(client)),
	gocache.WithBus(redisbus.New(client)),
	gocache.WithDefaultTTL(30*time.Minute),
	gocache.WithDefaultGrace(6*time.Hour),
)
if err != nil {
	return err
}
defer func() { _ = cache.Close() }()
```

The Redis client is yours: gocache never closes it. Close it after the cache.

### Multiple instances, SQL

Same shape, with your existing database instead of Redis. Postgres, MySQL and SQLite are supported
from one driver:

```go
import (
	"database/sql"

	sqldrv "github.com/swissy-dev/gocache/driver/sqldriver"
	_ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", dsn)
if err != nil {
	return err
}
db.SetMaxOpenConns(25)

store, err := sqldrv.New(db, sqldrv.Postgres)
if err != nil {
	return err
}

cache, err := gocache.New(
	gocache.WithL1(memory.New()),
	gocache.WithL2(store),
	gocache.WithDefaultTTL(30*time.Minute),
)
```

The table is not created for you. Get the DDL and run it through your own migration tooling:

```go
for _, stmt := range store.Schema() {
	fmt.Println(stmt)
}
```

In development you can shortcut with `store.Migrate(ctx)`. A background sweeper deletes expired rows
in batches; `sqldrv.WithSweepInterval(0)` turns it off if you'd rather sweep from a cron job with
`store.SweepOnce(ctx)`.

## Using it

### Cache-aside — the main pattern

`GetOrSet` is the one you'll reach for. On a miss it runs your factory, stores the result and returns
it. When fifty goroutines miss the same key at once, **the factory runs once** and the other
forty-nine wait for that result:

```go
user, err := gocache.GetOrSet(ctx, cache, "user:42", func(ctx context.Context) (User, error) {
	return db.FindUser(ctx, 42)
}, gocache.WithTTL(time.Minute), gocache.WithTags("users"))
```

The factory's context is detached from any single caller's cancellation, so one caller giving up
doesn't cancel the work everybody else is waiting on. It still inherits the *values* of whichever
caller happened to start the flight, though — request IDs, trace spans, request-scoped handles — so
don't rely on request-scoped values inside a factory.

`GetOrSetForever` is the same thing with no expiry.

### Reading and writing directly

```go
user, found, err := gocache.Get[User](ctx, cache, "user:42")
if err != nil {
	return err
}
if !found {
	return fetchTheHardWay(ctx)
}

err = gocache.Set(ctx, cache, "user:42", user, gocache.WithTTL(time.Minute))
err = gocache.SetForever(ctx, cache, "config", cfg)
```

A miss is `found == false` with a nil error — not an error. `Get` never returns stale data.

`Pull` reads then deletes — it's `Get` followed by `Delete`, not one atomic step, so two concurrent
callers can both receive the same value. Fine for a one-shot value where a duplicate read is
harmless, like a flash message shown once per session:

```go
msg, found, err := gocache.Pull[FlashMessage](ctx, cache, "flash:"+userID)
```

A value that must be consumed exactly once needs `Lock` instead.

### Invalidating

By key:

```go
existed, err := cache.Delete(ctx, "user:42")
err = cache.DeleteMany(ctx, []string{"user:42", "user:43"})
```

By tag — this is the useful one. Tag entries as you write them, then invalidate the whole group in a
single write, no matter how many keys carry the tag:

```go
err := gocache.Set(ctx, cache, "user:42", user, gocache.WithTags("users", "team:7"))
err = gocache.Set(ctx, cache, "user:43", other, gocache.WithTags("users"))

err = cache.DeleteByTag(ctx, "users")   // both entries are now misses
```

There's no tag→keys index to maintain: invalidation writes one timestamp and entries created before
it stop matching. Cost is the same whether the tag covers ten keys or ten million.

### Namespaces

A namespace is a derived cache that prefixes its keys. Use them to keep subsystems from colliding and
to get a `Clear` that's bounded to your own data:

```go
users := cache.Namespace("users")
posts := cache.Namespace("posts")

err := gocache.Set(ctx, users, "42", user)   // stored as "users:42"
err = users.Clear(ctx)                        // clears only "users:*"
```

Namespaces share the parent's drivers, bus and lifecycle. Tags are global, so one tag can span
several namespaces.

### Distributed locks

For work that must happen once across the whole fleet — a nightly rebuild, a migration, anything
that shouldn't run twice:

```go
err := cache.Lock("reports:rebuild", 10*time.Minute).Do(ctx, func(ctx context.Context) error {
	return rebuildReports(ctx)
})
if errors.Is(err, gocache.ErrLockHeld) {
	// someone else is already doing it
}
```

`Do` acquires, runs, and releases — including if your callback panics. For finer control:

```go
lock := cache.Lock("reports:rebuild", 10*time.Minute)

ok, err := lock.Acquire(ctx)            // returns false if held
err = lock.Block(ctx, 30*time.Second)   // wait for it, ErrLockTimeout on giving up
err = lock.Release(ctx)
err = lock.ForceRelease(ctx)            // break someone else's lock
```

Each lock carries a random owner token, so if your TTL lapses and another process takes over, your
`Release` can't delete their lock. The TTL is the deadlock ceiling: crash while holding it and it
frees itself.

## Surviving a bad day

Two independent settings, both off by default.

**Grace** — when the database your factory reads from is down, serve stale data rather than
failing:

```go
gocache.WithDefaultGrace(6 * time.Hour)
```

With grace configured, a factory error on a key that has a recently-expired copy returns that copy
with a nil error, provided the copy is still there to read — once a cache tier has swept the key
away, there's nothing to fall back on and the factory error surfaces normally. You asked for
stale-on-failure, so it's a success, not an error — the underlying failure arrives as
`EventGraceHit` and a log line. Grace covers factory failures only: if a cache tier itself is unreachable,
`GetOrSet` returns that error directly and never consults grace. Narrow it per call with
`WithGrace(time.Minute)`, or turn it off for one call with `WithGrace(0)`.

**Timeouts** — when your database is merely slow:

```go
gocache.WithSoftTimeout(100 * time.Millisecond)
gocache.WithHardTimeout(5 * time.Second)
```

Past the soft timeout, a waiting caller gets the stale copy immediately while the factory keeps
running in the background and stores its result when it finishes. The hard timeout cancels the
factory's context outright. Soft timeouts need grace to have something to serve.

## Watching what it does

Every operation emits a typed event. Leaving the hook unset skips your callback, but the event
value is still allocated:

```go
gocache.WithEventHook(func(e gocache.Event) {
	switch ev := e.(type) {
	case gocache.EventHit:
		metrics.Inc("cache.hit", "tier", string(ev.Tier))
	case gocache.EventMiss:
		metrics.Inc("cache.miss")
	case gocache.EventGraceHit:
		log.Warn("serving stale", "key", ev.Key, "err", ev.Err)
	}
})
```

`EventGraceHit.Err` is nil when the hit came from a soft timeout — nothing has actually failed at
that point. It's only populated when the hit came from a real factory error.

The full set: `EventHit`, `EventMiss`, `EventWritten`, `EventDeleted`, `EventCleared`,
`EventTagInvalidated`, `EventGraceHit`, `EventFactoryError`, `EventWriteFailed`,
`EventBusPublishFailed`, `EventBusMessageReceived`, `EventLockAcquired`, `EventLockReleased`.

Hooks run after internal locks are released, so reads and writes from a hook are safe. Never call
`Close()` from a hook, though — on the `GetOrSet` path, it deadlocks permanently. A panicking hook
is recovered and logged rather than taking down the process. Most failures that are swallowed by
design also go to an `*slog.Logger` — `WithLogger(nil)` silences it — but not every one: a retried
or dropped bus publish only emits `EventBusPublishFailed`, with no log line.

## Testing code that uses gocache

Use the memory driver for a real cache with no infrastructure, and inject a clock so expiry is
deterministic:

```go
now := time.Now()
cache, _ := gocache.New(
	gocache.WithL1(memory.New()),
	gocache.WithClock(func() time.Time { return now }),
)
now = now.Add(2 * time.Hour)   // everything with a shorter TTL is now expired
```

This is safe exactly as written: `GetOrSet` waits for the flight before your test goroutine
continues. It stops being safe under `-race` once a soft timeout is configured, since the factory
can then keep running in a background goroutine after the wait returns — guard the clock with a
mutex if you use soft timeouts. The package's own `fakeClock` in `ops_test.go` does this and is a
fine thing to copy.

Or disable caching entirely without changing any calling code:

```go
gocache.WithL1(null.New())   // stores nothing, every read misses
```

Note that locks on a null-backed cache always acquire — `null` stores nothing, so it provides no
mutual exclusion.

## Reference

### Operations

| Call | Behaviour |
|---|---|
| `Get[T](ctx, c, key)` | `(value, found, err)`; never returns stale data |
| `Set(ctx, c, key, value, opts...)` | write-through L2 then L1, then bus invalidation |
| `SetForever(ctx, c, key, value, opts...)` | `Set` with no expiry |
| `GetOrSet[T](ctx, c, key, factory, opts...)` | cache-aside with stampede protection, grace and timeouts |
| `GetOrSetForever[T](...)` | `GetOrSet` with no expiry |
| `Pull[T](ctx, c, key)` | read then delete, not atomically |
| `c.Has`, `c.Delete`, `c.DeleteMany`, `c.Clear` | management |
| `c.DeleteByTag(ctx, tags...)` | O(1) tag invalidation |
| `c.Namespace(name)` | derived cache with a key prefix |
| `c.Lock(name, ttl)` | distributed atomic lock |
| `c.Close()` | stops background goroutines and closes bus and drivers |

> ### WARNING — `Clear()` on a **root** cache clears the entire keyspace of the store
>
> `Clear` is `ClearPrefix(namespacePrefix)` on every tier, and a root cache's prefix is empty. gocache has
> no global key prefix, so it cannot tell its own keys from anyone else's: `c.Clear(ctx)` on a root cache
> deletes **every key in the underlying store, including keys gocache never wrote**. For Redis that is
> every key in the selected logical database — sessions, queues, rate limiters, everything.
>
> If the store is shared with anything else, keep cache data under a namespace (which prefixes its keys
> with `cache:`) and clear that instead — it is bounded to the prefix:
>
> ```go
> app := c.Namespace("cache")
> err := app.Clear(ctx)
> ```
>
> A root `Clear` also removes gocache's own lock keys (`__gocache:lock:*`) and tag keys
> (`__gocache:tag:*`), so held locks are released and tag invalidation history is lost.

Notes:

- `__gocache:` is a reserved key prefix (lock and tag keys live there). Application keys must not start
  with it — this is a convention, not something gocache validates.
- `c.Namespace(name)` returns a cache sharing the parent's drivers, bus and **lifecycle**. Calling
  `Close()` on a namespace closes the root cache — and therefore every other namespace derived from it.
- Every operation returns `ErrClosed` once the cache is closed.

### Options

Constructor: `WithL1`, `WithL2`, `WithBus`, `WithDefaultTTL`, `WithDefaultGrace`, `WithSoftTimeout`,
`WithHardTimeout`, `WithTagCacheTTL`, `WithCodec`, `WithClock`, `WithEventHook`, `WithLogger`,
`WithBusRetryQueueSize`.

Per call: `WithTTL`, `WithTags`, `WithGrace`, `WithCallSoftTimeout`, `WithCallHardTimeout`,
`WithSkipL1`, `WithSkipBus`.

Defaults: TTL 30 minutes, grace off, timeouts off, tag-cache TTL 10 seconds, JSON codec,
`slog.Default()`, bus retry queue 1024. `New` validates its options and returns an error rather than
failing later — a cache with no tier, a bus without both tiers, a nil driver or a negative duration
are all rejected at construction.

`WithGrace(d)` bounds staleness for the call: a stale entry is served only while it is within `d` of its
logical expiry, even when the constructor default is larger.

**`WithCodec` must produce valid JSON.** The default codec is `encoding/json`. Values are embedded verbatim
in a JSON envelope (`{"v": <value>, "c": …, "x": …}`), so a codec emitting msgpack, gob or protobuf bytes
cannot be stored. `New` marshals a probe value through the configured codec and fails construction if the
output is not valid JSON — an alternative JSON producer (a faster JSON library, a wrapper around
`encoding/json`) is fine, a binary codec is not.

### Errors

`err != nil` always means the returned value is the zero value. A cache miss is not an error. Some
failures are swallowed by design and surfaced through the event hook, the logger, or both, instead
of an error return: a grace hit (you configured grace, so serving stale on factory failure is the
success you asked for), a failed bus publish (retried asynchronously), and — inside `GetOrSet` — a
cache write failure after the factory has already succeeded. That last one returns the factory's
value with a **nil error** even though the write didn't happen — the same failure through plain
`Set` is returned as an error — so `EventWriteFailed` is the signal to watch. `Lock.Do` swallows a
failed `Release` the same way, but only to the logger; there's no event for it. Sentinels:
`ErrClosed`, `ErrLockTimeout`, `ErrLockHeld`, `ErrLockTTL`.

### Drivers

| Driver | Package | Notes |
|---|---|---|
| Memory | `driver/memory` | LRU + TTL, also the L1 tier |
| Redis | `driver/redisdriver` | go-redis v9, caller-owned client |
| SQL | `driver/sqldriver` | Postgres, MySQL, SQLite; caller-owned `*sql.DB` |
| Null | `driver/null` | stores nothing |

> ### WARNING — Redis and a root `Clear()`
>
> A root `Clear` reaches the Redis driver as `ClearPrefix("")`, which is `SCAN MATCH *` plus `DEL` — the
> practical equivalent of `FLUSHDB` on the selected logical database. Sharing one Redis database between
> gocache and sessions/queues/anything else is common, so never call `Clear` on a root cache in that
> setup: clear a namespace (`c.Namespace("cache").Clear(ctx)`), or give the cache its own logical
> database. The SQL driver is inherently bounded to its own table, and the memory driver to its own
> process-local map.

The SQL driver never configures the connection pool — set `SetMaxOpenConns`, `SetMaxIdleConns`,
`SetConnMaxLifetime` and `SetConnMaxIdleTime` on the `*sql.DB` you pass in. Create its table with
your own migration tooling using `driver.Schema()`, or call `driver.Migrate(ctx)` in development. The
schema carries no version marker, so a future schema change has no automatic upgrade path — you would
migrate the table yourself. On MySQL the key column is `VARBINARY(255)`, so keys are limited to 255
bytes. Postgres and SQLite use `TEXT`: SQLite has no limit, while Postgres's key doubles as a btree
primary-key index, which caps entries at roughly 2704 bytes on a default 8 KB page — much higher
than MySQL's limit, but not unbounded.

`Driver.Close()` implementations must tolerate a concurrent in-flight operation: `Cache.Close` joins its
own background goroutines, but foreground data operations are not joined, so a `Get` or `Set` running in
another goroutine can still be inside the driver when `Close` reaches it.

### Bus

| Bus | Package | Notes |
|---|---|---|
| Memory | `bus/memorybus` | single process, delivers to subscribers synchronously within `Publish` |
| Redis | `bus/redisbus` | go-redis v9 Pub/Sub, caller-owned client |

A bus requires both tiers — it exists to evict L1 copies that L2 has moved past, so it is rejected at
construction without an L1 and an L2. Publish failures never fail your call — they go to a bounded
retry queue and surface as `EventBusPublishFailed` — but the publish itself is bounded by a 5-second
timeout, so a slow bus can hold up the caller for that long. If the bus is down entirely, peers serve
their own L1 copies until those expire, so staleness stays bounded by the TTL — or by TTL plus grace,
if grace is on and the origin fails too.

`redisbus`'s `Close` joins the goroutine that delivers messages to your handler, so a handler
that calls `Bus.Close()` on itself will deadlock — this is a deliberate constraint, not a bug.
Close the bus from outside the handler.

The Redis integration test suite runs against logical database 15 and flushes it before each test case.

## Developing gocache

```bash
make test                                    # unit tests
make race                                    # with the race detector
make lint                                    # golangci-lint, including tagged files
GOCACHE_TEST_POSTGRES=... make integration    # real Postgres/MySQL/Redis
```

Integration tests skip unless their DSN is set: `GOCACHE_TEST_POSTGRES`, `GOCACHE_TEST_MYSQL`,
`GOCACHE_TEST_REDIS`.

## License

MIT
