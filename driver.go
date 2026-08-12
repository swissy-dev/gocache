package gocache

import (
	"context"
	"io"
	"time"
)

// Reader is the read half of a [Driver]. A miss is (nil, false, nil) — not an
// error — so drivers must distinguish an absent key from a failed lookup.
//
// An entry whose TTL has passed must be reported as absent.
type Reader interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
}

// Writer is the write half of a [Driver].
//
// Set must not retain or alias the value slice, since the caller reuses it.
// Delete reports whether a live entry was removed; an expired one counts as
// absent. DeleteMany must tolerate keys that are not present. ClearPrefix
// removes every key with the prefix, and an empty prefix clears everything the
// driver holds.
type Writer interface {
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) (bool, error)
	DeleteMany(ctx context.Context, keys []string) error
	ClearPrefix(ctx context.Context, prefix string) error
}

// Atomic is the compare-and-set half of a [Driver], and is what makes
// [Cache.Lock] safe across processes.
//
// Add must store the value only if the key is absent or expired, and must be
// atomic against concurrent Adds — exactly one caller may see true. Similarly,
// DeleteIfEquals must delete only if the stored value matches, so a lock
// holder cannot delete a lease that has since been taken by someone else.
type Atomic interface {
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
	DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error)
}

// Driver is a cache tier: a key-value store with expiry, prefix clearing and
// atomic compare-and-set.
//
// Implementations must be safe for concurrent use, and must not alias the
// caller's value slices in either direction. Run a candidate through
// drivertest.Run, the conformance suite the shipped drivers are held to.
type Driver interface {
	Reader
	Writer
	Atomic
	io.Closer
}
