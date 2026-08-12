package gocache

import "errors"

// Sentinel errors returned by cache operations. Compare with [errors.Is];
// operations wrap them with context.
var (
	ErrClosed       = errors.New("gocache: cache closed")
	ErrFactoryLimit = errors.New("gocache: concurrent factory limit reached")
	ErrLockTimeout  = errors.New("gocache: lock timeout")
	ErrLockHeld     = errors.New("gocache: lock held")
	ErrLockTTL      = errors.New("gocache: lock ttl must be positive")
)
