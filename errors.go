package gocache

import "errors"

// Sentinel errors returned by cache operations. Compare with [errors.Is];
// operations wrap them with context.
var (
	ErrClosed = errors.New("gocache: cache closed")
	// ErrFactoryLimit means the call waited for factory capacity and its context
	// ended first. It does not mean the limit rejected the call outright: past
	// the ceiling a flight waits. The context's own error is wrapped inside.
	ErrFactoryLimit = errors.New("gocache: timed out waiting for factory capacity")
	ErrLockTimeout  = errors.New("gocache: lock timeout")
	ErrLockHeld     = errors.New("gocache: lock held")
	ErrLockTTL      = errors.New("gocache: lock ttl must be positive")
)
