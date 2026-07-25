package gocache

import "errors"

var (
	ErrClosed       = errors.New("gocache: cache closed")
	ErrFactoryLimit = errors.New("gocache: concurrent factory limit reached")
	ErrLockTimeout  = errors.New("gocache: lock timeout")
	ErrLockHeld     = errors.New("gocache: lock held")
	ErrLockTTL      = errors.New("gocache: lock ttl must be positive")
)
