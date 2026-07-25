# gocache — Multi-tier caching library for Go

**Date:** 2026-07-23
**Status:** Approved design, pending implementation (revised after golang-skills review)
**Module:** `github.com/swissy-dev/gocache` · Go 1.25

## 1. Overview

A Go caching library modeled on [bentocache](https://github.com/Julien-R44/bentocache): a two-tier cache (L1 in-memory, L2 distributed) with bus-driven cross-instance invalidation, namespaces, tag-based invalidation, stampede protection, grace periods, factory timeouts, atomic locks, and pluggable byte-level drivers.

### Goals (v1)

- Multi-tier caching: optional L1 (memory) + optional L2 (Redis/SQL), used together or alone.
- Bus synchronization of L1 across instances (Redis pub/sub).
- Namespaces with bulk clear; tags with O(1) `DeleteByTag`.
- Resiliency: singleflight stampede protection, grace periods (serve stale on factory failure), soft/hard factory timeouts.
- Laravel-style distributed atomic locks with owner-token release.
- Typed access via generics; drivers deal only in `[]byte`.
- Go-idiomatic API: `ctx` first, functional options, explicit errors, small interfaces.
- Drivers: memory, Redis, SQL (Postgres/MySQL/SQLite), null.
- Events hook for observability.

### Non-goals (v1)

- File, DynamoDB, MongoDB, or other additional drivers.
- OpenTelemetry / Prometheus integrations (the events hook is their future foundation).
- Distributed (cross-instance) stampede protection — singleflight is per-instance, as in bentocache.
- Encryption, compression, or alternative envelope codecs beyond the pluggable value codec.

Naming decision (recorded): the module stays `gocache` — `package cache` would collide with a common local identifier and the well-known `patrickmn/go-cache`.

## 2. Architecture

Layered core with dumb drivers (bentocache's own shape):

- **Drivers** implement a minimal byte-store contract. No knowledge of tiers, tags, namespaces, envelopes, or grace.
- **Core** (`gocache` package) owns all semantics: envelope encoding, tier orchestration, tag versions, namespaces, singleflight, grace/timeouts, locks, events, bus publication. It also owns the `Driver` and `Bus` interfaces — contracts live where they are consumed; implementations satisfy them structurally without importing the core.
- **Bus** implementations provide cross-instance L1 invalidation transport.

### Package layout

```
gocache/                        core package
├── go.mod                      module github.com/swissy-dev/gocache
├── README.md · LICENSE · .gitignore · .golangci.yml · Makefile
├── cache.go                    Cache type, New(), management methods
├── driver.go                   Driver interface (consumed here)
├── bus.go                      Bus interface (consumed here)
├── ops.go                      Get[T], Set, SetForever, GetOrSet[T], GetOrSetForever[T], Pull[T]
├── options.go                  Option (constructor) + CallOption (per-call)
├── envelope.go                 stored-entry format
├── tags.go                     tag-version invalidation
├── lifecycle.go                lifecycle context, WaitGroup, Close, bus handling
├── lock.go                     atomic locks
├── events.go                   event types + hook
├── example/                    runnable usage examples
├── driver/
│   ├── drivertest/             shared conformance suite (imports gocache)
│   ├── memory/                 LRU + TTL (also the L1 tier)
│   ├── redisdriver/            go-redis v9
│   ├── sqldriver/              database/sql, dialects: postgres, mysql, sqlite
│   └── null/                   no-op
└── bus/
    ├── redisbus/               Redis pub/sub
    └── memorybus/              in-process, tests / single-instance
```

`redisdriver`/`sqldriver` are named to avoid guaranteed import collisions: consumers already import `github.com/redis/go-redis/v9` (package `redis`) and `database/sql` at every call site. `memory`, `null`, `redisbus`, `memorybus` don't collide and stay short.

Dependency policy: the core package uses the standard library plus `golang.org/x/sync` only. go-redis is linked only when `driver/redisdriver` or `bus/redisbus` is imported; SQL drivers are chosen by the consumer.

## 3. Public API

```go
c, err := gocache.New(
    gocache.WithL1(memory.New(memory.WithMaxEntries(10_000))),
    gocache.WithL2(redisdriver.New(client)),
    gocache.WithBus(redisbus.New(client)),
    gocache.WithDefaultTTL(30*time.Minute),
    gocache.WithDefaultGrace(6*time.Hour),
    gocache.WithEventHook(hook),
    gocache.WithLogger(logger),
)
if err != nil { ... }
defer c.Close()
```

`New(opts ...Option) (*Cache, error)` validates at construction and fails on: no tier configured, a bus without both L1 and L2, nil driver/bus, negative durations.

Data operations are package-level generic functions (Go has no generic methods); management operations are methods on `*Cache`.

```go
value, ok, err := gocache.Get[User](ctx, c, "user:42")
err  = gocache.Set(ctx, c, "user:42", user, gocache.WithTTL(time.Minute), gocache.WithTags("users"))
err  = gocache.SetForever(ctx, c, "config", cfg)
v, err := gocache.GetOrSet(ctx, c, "user:42", factory, gocache.WithTTL(time.Minute))
v, err := gocache.GetOrSetForever(ctx, c, "config", factory)
v, ok, err := gocache.Pull[User](ctx, c, "user:42")

ok, err := c.Has(ctx, "user:42")
ok, err := c.Delete(ctx, "user:42")
err  = c.DeleteMany(ctx, keys)
err  = c.DeleteByTag(ctx, "users")
err  = c.Clear(ctx)
users := c.Namespace("users")
lock  := c.Lock("reports:generate", 10*time.Second)
err  = c.Close()
```

Factory signature: `func(ctx context.Context) (T, error)`.

### Options

Two exported option types: `Option` (constructor) and `CallOption` (per-call).

- Constructor: `WithL1`, `WithL2`, `WithBus`, `WithKeyPrefix`, `WithDefaultTTL`, `WithDefaultGrace`, `WithSoftTimeout`, `WithHardTimeout`, `WithCodec`, `WithClock`, `WithEventHook`, `WithLogger`, `WithBusRetryQueueSize`.
- Per-call: `WithTTL(d)`, `WithTags(...)`, `WithGrace(d)` (0 disables for the call), `WithCallSoftTimeout(d)`, `WithCallHardTimeout(d)`, `WithSkipL1()` (also removes any existing L1 copy, so the writer cannot serve an older value than its peers), `WithSkipBus()`.

Per-call options override constructor defaults. Defaults: TTL 30 m, grace off, timeouts off.

### Codec & logging

`WithCodec(codec)` where `Codec` is `Marshal(any) ([]byte, error)` / `Unmarshal([]byte, any) error`; default `encoding/json`. The codec must emit valid JSON, because values are embedded verbatim in a JSON envelope; `New` marshals a probe value and rejects a codec whose output is not valid JSON. `WithLogger(*slog.Logger)` defaults to `slog.Default()`; pass `nil` to silence. The logger is used only for failures that are swallowed by design (§9): grace hits, best-effort write failures, bus retry exhaustion, recovered panics.

## 4. Driver contract

Defined in the core package, composed from focused interfaces:

```go
type Reader interface {
    Get(ctx context.Context, key string) (value []byte, found bool, err error)
}

type Writer interface {
    Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) (bool, error)
    DeleteMany(ctx context.Context, keys []string) error
    ClearPrefix(ctx context.Context, prefix string) error
}

type Atomic interface {
    Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
    DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error)
}

type Driver interface {
    Reader
    Writer
    Atomic
    io.Closer
}
```

- `ttl == 0` means no expiry.
- `Add` is atomic set-if-not-exists (expired entries count as absent); `DeleteIfEquals` is atomic compare-and-delete. Both exist for locks; all v1 drivers implement them natively.
- Drivers never interpret values.

### Memory driver

Map + `container/list` LRU guarded by a `sync.Mutex` (`Get` mutates recency order, so `RWMutex` buys nothing). `WithMaxEntries` option (default 10 000). Expired entries evicted lazily on access, and reclaimed during capacity eviction by scanning a small fixed window at the cold end of the LRU list before falling back to evicting the tail. The window is bounded so eviction stays constant-time: a full-list scan would make every write O(n) under the driver's lock. The driver copies value bytes on `Set`; bytes returned by `Get` are read-only by contract (the core never mutates driver-returned bytes — its `json.RawMessage` aliases them).

### Redis driver

go-redis v9. `Set` → `SET PX`; `Add` → `SET NX PX`; `DeleteIfEquals` → Lua compare-and-delete script; `ClearPrefix` → `SCAN` + batched `DEL`.

### SQL driver

Accepts a **caller-owned** `*sql.DB`; the driver never configures the pool and its `Close` stops the sweeper only — it never closes the caller's DB. Docs recommend the standard pool setters (`SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime`, `SetConnMaxIdleTime`).

Single table (name configurable, default `gocache`):

```sql
key        TEXT PRIMARY KEY   -- VARBINARY(255) on MySQL, which cannot index a TEXT primary key
value      BYTEA / BLOB
expires_at BIGINT NULL        -- unix milliseconds, NULL = forever
-- index on expires_at (sweeper + expiry filters); inline in CREATE TABLE on MySQL
```

Dialects: `postgres`, `mysql`, `sqlite` (placeholder style + upsert syntax). Expired rows are treated as absent on read and deleted lazily. Expiry is computed and compared from database server time inside every statement — `expires_at` is written as `<server now> + <ttl ms>` (a NULL ttl parameter yields a NULL expiry in all three dialects) and reads, deletes, compare-and-delete, the set-if-absent cleanup and the sweeper all filter against the same expression — so no client clock takes part and skewed nodes cannot disagree about a row's, or a lock lease's, lifetime. A background sweeper (`time.NewTicker`, default every 5 m, configurable) deletes expired rows in bounded batches (default 1 000 rows per statement, looping while full batches are deleted), each pass under its own timeout on a context derived from the driver's lifecycle and canceled by `Close`, which joins the goroutine before returning.

Schema setup: `Migrate(ctx)` is a dev/test convenience; `(*Driver).Schema() []string` exports the DDL statements so production users apply it through their own migration tooling (golang-migrate, Flyway, CI/CD) under least-privilege credentials. Nothing runs implicitly.

### Null driver

`Get` → miss; writes succeed and store nothing; `Add` → always true. Locks on a null-backed cache always acquire (documented).

## 5. Envelope

Every cache entry is stored as JSON:

```json
{"v": <raw JSON value>, "c": 1690000000000, "x": 1690000060000, "t": ["users"]}
```

- `v` — value bytes (`json.RawMessage`, no double encoding).
- `c` — createdAt, unix ms (from the injected clock).
- `x` — logical expiry, unix ms; 0 = forever.
- `t` — tags, omitted when empty.

**Physical TTL handed to drivers = logical TTL + grace period.** The entry outlives its freshness so it can be served stale. With grace off, physical = logical. Lock keys and tag-version keys are raw values, not envelopes.

## 6. Data flow

### Get

L1 hit and logically fresh → return. Else L2: fresh → backfill L1 (physical TTL recomputed as remaining logical TTL + configured grace) → return. Else miss. When L1 held a stale entry and L2 reports absent, that stale envelope is carried out of the read as a grace candidate — `Get` still treats it as a miss, but `GetOrSet` can serve it under grace. `Get` never returns stale values. `Pull` is `Get` followed by delete-through (both tiers + bus) and shares its signature.

#### 6b. Backfill fencing

Reading L2 and writing L1 are separate steps. A mutation completing between them would otherwise be undone: the backfill writes the pre-mutation value into L1 *after* the invalidation, and that resurrected copy is served until its physical TTL expires — never, for an entry written with no expiry. Both the data backfill and the L1 tag-marker cache (§7) go through this shape and are fenced identically.

- **Tracker.** A fixed-size array of 4 096 striped `atomic.Uint64` counters on the cache runtime, indexed by `maphash.String(seed, fullKey) % 4096`. Bounded by construction: an unbounded per-key map would be a memory-growth DoS in its own right. Two keys sharing a stripe cause a *skipped* backfill, never a stale write — the next read simply repeats the L2 fetch.
- **Bump before mutate.** Every local mutation path increments its keys' stripes *before* touching any tier: `writeEnvelope`, `Delete`, `DeleteMany`, `DeleteByTag` (on the tag's marker key), and the inbound bus handler for `delete` / `clear` / `tag`. `Clear` invalidates a whole prefix, which no per-key stripe can express, so local and inbound clears bump *every* stripe; clears are rare and 4 096 atomic adds are cheap.
- **Check, write, re-check.** The reader loads the generation before the L2 read and, after it, (1) skips the backfill outright if the generation moved, (2) otherwise writes L1, then (3) re-loads the generation and deletes the key from L1 if it moved during the write.
- **Why the ordering is sound.** Step 1 alone is not enough — a mutation can land between the check and the L1 write. The re-check closes it: a mutator bumps before its own L1 eviction, so if the backfill's write landed after that eviction (the only ordering that resurrects anything), the mutator's bump precedes the reader's re-load and the reader deletes what it just wrote. The residual case — the re-check firing against a *newer* value written by a concurrent `Set` — evicts a live L1 entry, which is a wasted repopulation, not stale data.



### GetOrSet

1. Fast path: fresh hit in L1/L2 → return.
2. Miss/stale → per-key singleflight (§6a): one flight per key, all concurrent callers — leader included — wait on the flight result while selecting on their own `ctx.Done()`.
3. The flight runs the factory.
   - **Soft timeout:** factory exceeds it *and* a graced value exists → the blocked caller returns the stale value now; the flight keeps running and writes on completion.
   - **Hard timeout:** the flight context is canceled.
4. Success → envelope → write L2, then L1 → bus-publish invalidation.
5. Failure → graced value available → return it (see §9) and emit `EventGraceHit`; else return the wrapped factory error.

### 6a. Singleflight semantics

Flights are **owned by the cache, not by any caller** — this avoids the classic singleflight pitfall where the leader's cancellation poisons every waiter:

- Flight context = `context.WithoutCancel(initiating ctx)` (values preserved for tracing) raced with the cache lifecycle context and the hard timeout when set.
- Implementation: `golang.org/x/sync/singleflight` via `DoChan`; every caller selects on the result channel and its own `ctx.Done()`. A caller whose ctx cancels leaves; the flight continues for the others.
- Flight results are never cached on error (`Forget` on failure); a failed flight's error is delivered to all current waiters.
- A factory panic is recovered at the flight boundary, converted to an error delivered to all waiters, logged, and emitted as `EventFactoryError` — waiters are never stranded and the process never crashes.

### Set / Delete / DeleteMany / Clear

Write-through or delete-through: L2 first, then L1, then bus publish. `Clear` = `ClearPrefix` of the current namespace's data-domain prefix (§7) on both tiers + bus broadcast; it is never called with an empty prefix.

**Publishing on failure is asymmetric (§8).** The delete-shaped paths — `Delete`, `DeleteMany`, `Clear`, `DeleteByTag` — publish for every key they attempted and *then* return the tier error. `Set` publishes only when the authoritative write succeeded.

## 7. Key layout, tags and namespaces

### Key layout

Every stored key is domain-separated and versioned:

```
<prefix>:1:d:<ns...>:<key>    data entries
<prefix>:1:t:<tag>            tag markers
<prefix>:1:l:<ns...>:<name>   locks
```

`<prefix>` defaults to `gocache` and is set with `WithKeyPrefix` (empty or whitespace-only is rejected by `New`), so two applications can share one logical database. `1` is the layout version. Namespace segments appear only when the cache is not a root cache.

Every variable segment — each namespace name, the key, the tag, the lock name, and the configured prefix — is escaped before the segments are joined with `:`: `\` → `\\` and `:` → `\:` (one pass, via `strings.Replacer`). That makes the encoding injective: `Namespace("a")` + key `"b:c"` stores `gocache:1:d:a:b\:c`, while `Namespace("a:b")` + key `"c"` stores `gocache:1:d:a\:b:c`. It also makes the prefix match used by `Clear` sound — an escaped segment can never contain the unescaped `:` that ends a namespace.

No application key is reserved. Because tag markers and locks are in their own domains, an application key literally named `__gocache:tag:users` or `__gocache:lock:job` is an ordinary data entry and cannot suppress a tag invalidation or disturb a lock.

### Tags

`DeleteByTag(tag)` writes the current clock timestamp to `<prefix>:1:t:<tag>` (raw unix-ms value, stored forever) in L2 (or the sole tier) and publishes it on the bus. On read, if an envelope has tags, the stack fetches each tag's timestamp and treats the entry as a miss when `createdAt <= tagInvalidatedAt`, so a write racing an invalidation in the same millisecond loses.

Tag timestamps are cached in L1 with a short TTL (default 10 s, configurable) so lookups are usually local; the bus makes invalidation immediate, and the short TTL bounds staleness to ~10 s even with the bus down. Untagged entries never touch tag keys.

### Namespaces

`c.Namespace("users")` returns a derived `*Cache` sharing drivers, bus, and config, carrying the escaped colon-joined namespace path (`users`); nesting appends a segment (`users:posts`). `Clear` prefix-clears the data domain for that path only — `<prefix>:1:d:` on a root cache, `<prefix>:1:d:users:` inside a namespace — so it can reach neither foreign keys, nor tag markers, nor locks. A root `Clear` therefore no longer releases held locks or discards tag invalidation history. Tag markers are global and carry no namespace, so a tag can span namespaces. Lock names are namespace-scoped.

## 8. Bus

```go
type Bus interface {
    Publish(ctx context.Context, msg []byte) error
    Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error
    Close() error
}
```

`Subscribe`'s ctx bounds the subscription lifetime (the core passes its lifecycle context; `Close` also terminates it). Implementations own their receive goroutine and must not invoke the handler after `Close` returns.

One channel (default `gocache:bus`, configurable). JSON messages:

```json
{"o": "<origin-id>", "op": "delete|clear|tag", "k": ["gocache:1:d:user\\:42"], "p": "gocache:1:d:users:", "t": "users"}
```

- Each instance generates a random origin ID at startup and ignores its own messages.
- On receipt: `delete` evicts the listed keys from L1; `clear` prefix-evicts L1; `tag` evicts the cached tag-timestamp key so the next read refetches it. L2 is never touched — it is already correct. Each case first bumps the corresponding invalidation stripes (§6b) — every stripe for `clear` — so an L2 read already in flight on the receiver cannot backfill the value the message just invalidated.
- `k` and `p` carry fully-encoded keys and prefixes (§7); `t` carries the raw tag name and the receiver applies its own prefix. Two caches with different `WithKeyPrefix` values sharing one bus channel therefore exchange inert `delete`/`clear` messages, and a `tag` message only makes the peer refetch its own tag marker — correct, if mildly wasteful. Give them separate channels if that matters.
- Set operations publish a `delete` for the written keys (peers drop stale L1 copies and re-read from L2 on demand).
- **Delete-shaped operations publish unconditionally.** `Delete`, `DeleteMany`, `Clear` and `DeleteByTag` publish for every key/prefix/tag they attempted *before* returning any tier error. Drivers mutate in chunks, so a later chunk failing leaves earlier ones committed, and an ambiguous network error may hide a delete that landed; gating the publish on success would leave peers serving L1 copies of entries whose authoritative rows are gone, unbounded for entries with no expiry. Over-publishing costs one message plus a peer re-read that finds the value still present. `Set` is the deliberate exception — a rejected authoritative write changed nothing, so publishing could only trigger pointless re-reads, and it stays gated on success.
- **Publish failures are asynchronous by design** (§9 exception): they go to a bounded in-memory retry queue (default 1 024 messages, `WithBusRetryQueueSize`) drained with backoff by a cache-owned goroutine that exits via the lifecycle context and is joined by `Close`. On overflow the oldest message is dropped and `EventBusPublishFailed` fires per drop; messages still queued at `Close` are dropped (staleness stays bounded by L1 logical TTLs). Worst case with the bus fully down: peers serve their L1 copy until its logical TTL expires.

## 9. Error handling

Invariant: **`err != nil` ⇒ the returned value is the zero value and must not be used.** No dual-meaning returns.

- `Get[T]` → `(value, ok, error)`. Miss: `(zero, false, nil)`. Real failure (store unreachable, decode error): `(zero, false, err)` wrapped with `%w`.
- `Set` / `Delete` / `DeleteByTag` / `Clear` / lock methods → return every error. One deliberate exception: bus publishes triggered by these calls are asynchronous (§8) — their failures are handled once by the retry queue and surfaced via `EventBusPublishFailed` + logger, never returned from the triggering call.
- `GetOrSet[T]` → `(value, error)`:
  - Grace not configured (default): factory failure returns the wrapped factory error.
  - Grace configured: the caller has declared stale acceptable on failure, so a grace hit returns `(staleValue, nil)`; `EventGraceHit` carries the underlying factory error (wrapped originals — `errors.Is`/`As` work inside hooks) and the logger records it.
  - Factory succeeded but the cache write failed: the value is returned with nil error; `EventWriteFailed` carries the write error and the logger records it.
- Wrapping policy: library sentinels and `context.Canceled`/`context.DeadlineExceeded` always traverse via `%w`. Underlying driver errors are also wrapped for debuggability, but matching driver-internal errors (e.g. go-redis types) is documented as unsupported API — only gocache sentinels are stable.
- Independent multi-tier failures (`Delete`, `Clear`, `Close` touching L1+L2+bus) are combined with `errors.Join`.
- Exported sentinels: `ErrClosed`, `ErrLockTimeout`, `ErrLockHeld`, `ErrLockTTL`. Error strings are package-prefixed lowercase (`"gocache: ..."`). Driver errors are wrapped for debuggability, but only these sentinels (plus `context.Canceled` / `context.DeadlineExceeded`) are a stable matching surface.
- Two further failures are swallowed by design beyond the two above: `GetOrSet` returns its value with a nil error when the cache write fails (`EventWriteFailed`), and the best-effort L1 delete behind `WithSkipL1` is logged rather than returned.
- All operations take `ctx` and respect cancellation.

## 10. Lifecycle and panic policy

- `New` creates an internal lifecycle context; every background goroutine (bus subscriber, bus retry drainer, SQL sweeper, in-flight detached factories) is tracked by a `sync.WaitGroup`.
- `Close` is idempotent (`sync.Once`; second call returns nil): stop accepting operations → cancel the lifecycle context → join all background goroutines → close bus → close drivers (`errors.Join` on failures). Operations after `Close` return `ErrClosed`; a detached factory completing after `Close` discards its write and fires `EventWriteFailed`.
- Panic policy: panics are recovered at every internally-spawned goroutine boundary (flight, subscriber, drainer, sweeper) and converted to errors/events + log. The flight and drainer are tracked by the cache's own WaitGroup; the subscriber and sweeper are owned and joined by the bus and driver, which `Close` shuts down in turn — the library never crashes the host process. `lock.Do` releases the lock via `defer` and re-panics after release. Event-hook panics are recovered and logged.

## 11. Atomic locks

```go
lock := c.Lock("reports:generate", 10*time.Second)
ok, err := lock.Acquire(ctx)
err = lock.Block(ctx, 30*time.Second)
err = lock.Do(ctx, func(ctx context.Context) error { ... })
err = lock.Release(ctx)
err = lock.ForceRelease(ctx)
```

- Every successful `Acquire` mints a fresh crypto/rand owner token and stores it on the `*Lock` under a mutex. `Release` consumes it — takes the token, clears the slot, then deletes via `DeleteIfEquals(key, token)` — so a lock re-acquired by someone else after TTL expiry cannot be released by the old holder, and a token retired with its lease can never match a later one. `Release` with nothing held is a nil no-op; `ForceRelease` deletes unconditionally and retires the local token too. A `*Lock` holds one lease at a time: reusing a value sequentially is safe, sharing one between independent holders is not.
- `Acquire` → driver `Add` with the lock TTL. `Block` retries with jittered backoff (default 50–250 ms, `time.NewTimer`+`Reset`, select on `ctx.Done()`) until acquired, ctx canceled, or timeout → `ErrLockTimeout`. `Do` = acquire (`ErrLockHeld` if taken), run, release via `defer` (runs on panic too).
- Lock keys: `<prefix>:1:l:<namespaced name>` (§7), stored on L2 when present (distributed), else the sole driver. Locks bypass L1, envelopes, tags, and the bus.
- TTL is the deadlock ceiling: a crashed holder's lock frees itself at expiry. A zero or negative TTL is rejected with `ErrLockTTL` rather than taking a lock that would never expire.
- `Clear` never touches lock keys, root cache included: locks are in their own key domain (§7), so clearing a cache cannot release a lock another process is holding.

## 12. Events

Concrete event structs delivered synchronously to an optional hook (`WithEventHook(func(gocache.Event))`). Hooks are invoked only after all internal locks are released — re-entrant cache calls from a hook are safe; hook panics are recovered and logged. Zero cost when unset.

`EventHit`, `EventMiss`, `EventWritten`, `EventDeleted`, `EventCleared`, `EventTagInvalidated`, `EventGraceHit` (carries factory error), `EventFactoryError`, `EventWriteFailed` (carries write error), `EventBusPublishFailed`, `EventBusMessageReceived`, `EventLockAcquired`, `EventLockReleased`.

Each event carries the fully-prefixed key(s) where applicable. `EventHit` and `EventWritten` also carry the tier (`L1`/`L2`); the others have no meaningful tier.

## 13. Testing

**Status note (post-implementation).** This spec has been trued up against what shipped: the three behaviour changes in `fix: close three silent-failure gaps and add CI` are reflected above, and several claims that overpromised — a tier on every event, the null driver running the conformance suite, in-memory SQLite — have been corrected to describe the code as built.

- **Injected clock:** `WithClock(func() time.Time)`; all TTL/grace/tag comparisons read it — deterministic expiry math everywhere, including conformance runs against real backends.
- **`testing/synctest`** (Go 1.25) for everything driven by real timers that the injected clock can't reach: soft/hard timeout orchestration, singleflight waiter cancellation, detached-factory completion, lock `Block` backoff, retry-queue backoff.
- **Conformance suite** (`driver/drivertest`, imports `gocache`): exported functions asserting Get/Set/TTL/Add/DeleteIfEquals/ClearPrefix semantics, run by every driver. Default runs: memory directly, Redis via miniredis, SQL via a file-backed temporary SQLite database (`:memory:` is per-connection in modernc.org/sqlite, so it cannot serve a multi-connection pool). The null driver cannot satisfy the suite — it stores nothing, so read-back and `Add` semantics do not apply — and has its own targeted test instead.
- **Integration tests** behind `//go:build integration` (env vars supply DSNs only: `GOCACHE_TEST_REDIS`, `GOCACHE_TEST_POSTGRES`, `GOCACHE_TEST_MYSQL`): the same suite against real Redis, Postgres, and MySQL — the postgres and mysql dialects have no coverage otherwise, so CI must run this tag before release.
- **Core tests:** memory drivers as L1+L2 plus memorybus with the fake clock — tier read/write/backfill, grace serving, tag invalidation, namespace isolation, envelope round-trips.
- **Stampede test:** N concurrent goroutines on one key assert exactly one factory execution.
- **Multi-instance tests:** two `Cache` values sharing one L2 and one memorybus; writes/deletes/tag-flushes on A must evict B's L1. Lock exclusion across both instances.
- **Goroutine leaks:** `goleak.VerifyTestMain` in core, `driver/sqldriver`, and bus test packages; explicit test that `Close` reaps every background goroutine, including closing mid-flight during a detached soft-timeout factory.
- **`go test -race`** everywhere; TDD throughout.

## 14. Dependencies

| Dependency | Where | Purpose |
|---|---|---|
| `golang.org/x/sync` | core | singleflight |
| `github.com/redis/go-redis/v9` | `driver/redisdriver`, `bus/redisbus` | Redis client |
| `github.com/alicebob/miniredis/v2` | tests | in-process Redis |
| `modernc.org/sqlite` | tests | pure-Go SQLite for the SQL conformance suite |
| `go.uber.org/goleak` | tests | goroutine-leak detection |
| `github.com/jackc/pgx/v5/stdlib`, `github.com/go-sql-driver/mysql` | integration tests | real-backend dialect coverage |

Core package: standard library + `golang.org/x/sync` only.
