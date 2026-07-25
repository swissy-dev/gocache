package gocache

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"
)

const lockPrefix = "__gocache:lock:"

type Lock struct {
	cache *Cache
	key   string
	ttl   time.Duration
	token []byte
}

func (c *Cache) Lock(name string, ttl time.Duration) *Lock {
	b := make([]byte, 16)
	crand.Read(b)
	return &Lock{
		cache: c,
		key:   lockPrefix + c.key(name),
		ttl:   ttl,
		token: []byte(hex.EncodeToString(b)),
	}
}

func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	if l.cache.rt.closed.Load() {
		return false, ErrClosed
	}
	if l.ttl <= 0 {
		return false, ErrLockTTL
	}
	ok, err := l.cache.authoritative().Add(ctx, l.key, l.token, l.ttl)
	if err != nil {
		return false, fmt.Errorf("gocache: lock acquire: %w", err)
	}
	if ok {
		l.cache.emit(EventLockAcquired{Key: l.key})
	}
	return ok, nil
}

func (l *Lock) Release(ctx context.Context) error {
	if l.cache.rt.closed.Load() {
		return ErrClosed
	}
	ok, err := l.cache.authoritative().DeleteIfEquals(ctx, l.key, l.token)
	if err != nil {
		return fmt.Errorf("gocache: lock release: %w", err)
	}
	if !ok {
		l.cache.logf("lock no longer owned at release", "key", l.key)
		return nil
	}
	l.cache.emit(EventLockReleased{Key: l.key})
	return nil
}

func (l *Lock) ForceRelease(ctx context.Context) error {
	if l.cache.rt.closed.Load() {
		return ErrClosed
	}
	if _, err := l.cache.authoritative().Delete(ctx, l.key); err != nil {
		return fmt.Errorf("gocache: lock force release: %w", err)
	}
	l.cache.emit(EventLockReleased{Key: l.key})
	return nil
}

func (l *Lock) Block(ctx context.Context, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var backoff *time.Timer
	defer func() {
		if backoff != nil {
			backoff.Stop()
		}
	}()
	for {
		ok, err := l.Acquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		d := lockBackoff()
		if backoff == nil {
			backoff = time.NewTimer(d)
		} else {
			backoff.Reset(d)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return ErrLockTimeout
		case <-backoff.C:
		}
	}
}

func lockBackoff() time.Duration {
	return 50*time.Millisecond + time.Duration(rand.Int64N(int64(200*time.Millisecond)))
}

func (l *Lock) Do(ctx context.Context, fn func(context.Context) error) error {
	ok, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLockHeld
	}
	defer func() {
		if rerr := l.Release(context.WithoutCancel(ctx)); rerr != nil {
			l.cache.logf("lock release failed", "key", l.key, "err", rerr)
		}
	}()
	return fn(ctx)
}
