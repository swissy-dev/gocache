// Package gocache is a multi-tier cache for Go: an in-process L1 in front of a
// shared L2, kept coherent across instances by a message bus.
//
// A cache is built with [New] and a set of [Option] values. Reads and writes go
// through the generic package-level functions rather than methods, because Go
// does not allow type parameters on methods:
//
//	cache, err := gocache.New(
//		gocache.WithL1(memory.New()),
//		gocache.WithL2(redisdriver.New(client)),
//		gocache.WithDefaultTTL(5*time.Minute),
//	)
//	if err != nil {
//		return err
//	}
//	defer cache.Close()
//
//	user, err := gocache.GetOrSet(ctx, cache, "user:1", loadUser)
//
// Operations that do not need a type parameter, such as [Cache.Delete] and
// [Cache.Clear], are methods.
//
// # Tiers
//
// Either tier may be used alone. With both, a read checks L1 first, falls back
// to L2, and backfills L1 on the way out. Writes go to L2 first, so the shared
// tier is authoritative: if L2 rejects a write the call fails, while an L1
// failure is reported but does not lose the value.
//
// L1 is per-process, so a write on one instance leaves every other instance
// holding a stale copy. A [Bus] closes that gap by broadcasting invalidations;
// without one, peers serve their own L1 copy until it expires.
//
// # Expiry
//
// Values are stored in an envelope carrying the value, its creation time, a
// logical expiry and any tags. The logical expiry is what determines whether a
// value is fresh. The physical TTL handed to the driver is the remaining
// logical lifetime plus any grace, so an entry can outlive its logical expiry
// in the store and still be reported as a miss.
//
// That gap is what makes grace periods work: when a factory fails and a
// logically expired value is still physically present, [GetOrSet] can serve the
// stale value instead of propagating the error. See [WithGrace].
//
// # Stampedes
//
// [GetOrSet] collapses concurrent misses for the same key into a single factory
// call. The factory runs on a context detached from any individual caller, so a
// caller giving up does not cancel work the others are waiting on, and it is
// bounded by the cache's lifetime and by [WithHardTimeout].
//
// # Errors
//
// Every operation returns an error rather than panicking or silently degrading.
// Failures that are not the caller's concern, such as an L1 write failing when
// L2 already holds the value, are reported through [WithEventHook] and the
// logger instead. The sentinel errors are [ErrClosed], [ErrFactoryLimit],
// [ErrLockTimeout], [ErrLockHeld] and [ErrLockTTL].
//
// # Keys
//
// Keys are namespaced and escaped before reaching a driver, so two caches
// sharing one Redis instance cannot collide. [Cache.Namespace] returns a view
// with an additional segment; the underlying tiers, bus and configuration are
// shared with the parent.
//
// Full documentation, including driver-specific guidance and worked examples,
// is at https://github.com/swissy-dev/gocache.
package gocache
