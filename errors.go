package gocache

import "errors"

var (
	ErrClosed      = errors.New("gocache: cache closed")
	ErrLockTimeout = errors.New("gocache: lock timeout")
	ErrLockHeld    = errors.New("gocache: lock held")
)
