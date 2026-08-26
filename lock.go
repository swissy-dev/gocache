package gocache

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Lock is a distributed mutex held in the authoritative tier. Obtain one with
// [Cache.Lock].
//
// A Lock records the token of the lease it currently holds, so a release only
// removes a lease this Lock still owns. That makes it safe for a lock whose TTL
// expired mid-work to fail its release rather than delete a lease another
// process has since acquired. A Lock is safe for concurrent use, but represents
// a single lease: acquiring twice without releasing loses track of the first.
type Lock struct {
	cache *Cache
	key   string
	ttl   time.Duration
	mu    sync.Mutex
	token []byte
}

// Lock returns a distributed lock named within this cache's namespace. It
// acquires nothing; call [Lock.Acquire], [Lock.Block] or [Lock.Do].
//
// The lock lives in the authoritative tier, so it coordinates across every
// instance pointed at the same L2. With only an L1 configured it is
// process-local and coordinates nothing beyond it.
//
// ttl bounds how long the lock survives without being released, so a process
// that dies holding it cannot block others forever. It must be positive.
//
// The lease is never renewed. Exclusion holds only while it is valid: work that
// outruns its ttl loses the lock, and another process can acquire it while the
// first is still running. Size ttl above the worst case you are willing to
// tolerate, and do not treat this as a guarantee that protected work never
// overlaps. [Lock.Release] will not delete a lease that has since been taken by
// someone else, so an overrun is at worst duplicated work, not a stolen lock.
func (c *Cache) Lock(name string, ttl time.Duration) *Lock {
	return &Lock{
		cache: c,
		key:   c.lockKey(name),
		ttl:   ttl,
	}
}

func newLockToken() []byte {
	return []byte(crand.Text())
}

func (l *Lock) hold(token []byte) {
	l.mu.Lock()
	l.token = token
	l.mu.Unlock()
}

func (l *Lock) retire() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	token := l.token
	l.token = nil
	return token
}

// Acquire tries once to take the lock and reports whether it succeeded. It
// does not wait; see [Lock.Block] to retry until a deadline.
//
// A false return with a nil error means the lock is held elsewhere. Acquire
// returns [ErrLockTTL] if the lock was created with a non-positive TTL.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	if l.cache.rt.isClosed.Load() {
		return false, ErrClosed
	}
	if l.ttl <= 0 {
		return false, ErrLockTTL
	}
	token := newLockToken()
	ok, err := l.cache.authoritative().Add(ctx, l.key, token, l.ttl)
	if err != nil {
		return false, fmt.Errorf("gocache: lock acquire: %w", err)
	}
	if ok {
		l.hold(token)
		l.cache.emit(EventLockAcquired{Key: l.key})
	}
	return ok, nil
}

// Release gives up the lock, but only if this Lock still owns the lease it
// took. Releasing a lock that was never acquired is a no-op.
//
// If the lease expired and another process has since acquired it, Release
// leaves that lease alone and returns nil, logging the loss — the work it
// guarded has already overrun its TTL, and deleting a stranger's lock would
// make that worse.
func (l *Lock) Release(ctx context.Context) error {
	if l.cache.rt.isClosed.Load() {
		return ErrClosed
	}
	token := l.retire()
	if token == nil {
		return nil
	}
	ok, err := l.cache.authoritative().DeleteIfEquals(ctx, l.key, token)
	if err != nil {
		return fmt.Errorf("gocache: lock release: %w", err)
	}
	if !ok {
		lost := fmt.Errorf("gocache: lock lease for %q expired and was taken by another process", l.key)
		l.cache.emit(EventLockReleaseFailed{Key: l.key, Err: lost})
		l.cache.logf("lock no longer owned at release", "key", l.key)
		return nil
	}
	l.cache.emit(EventLockReleased{Key: l.key})
	return nil
}

// ForceRelease deletes the lock regardless of who holds it.
//
// This breaks the guarantee [Lock.Release] provides, so it is meant for
// operational recovery — a process died without releasing and the TTL is too
// long to wait out — not for normal flow.
func (l *Lock) ForceRelease(ctx context.Context) error {
	if l.cache.rt.isClosed.Load() {
		return ErrClosed
	}
	l.retire()
	if _, err := l.cache.authoritative().Delete(ctx, l.key); err != nil {
		return fmt.Errorf("gocache: lock force release: %w", err)
	}
	l.cache.emit(EventLockReleased{Key: l.key})
	return nil
}

// Block retries acquisition with jittered backoff until it succeeds, the
// timeout elapses, or ctx is cancelled. It returns [ErrLockTimeout] if the
// timeout is reached, or the context's error if ctx ends first.
//
// The caller holds the lock when Block returns nil and is responsible for
// calling [Lock.Release].
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

// Do acquires the lock, runs fn, and releases the lock afterwards. It returns
// [ErrLockHeld] without running fn if the lock is already taken; use
// [Lock.Block] first if you would rather wait.
//
// The release runs even if fn panics or ctx is cancelled, and is not itself
// cancelled by ctx, so work that overruns its context still gives the lock
// back. A release failure is logged rather than returned, so Do returns fn's
// error unchanged.
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
			l.cache.emit(EventLockReleaseFailed{Key: l.key, Err: rerr})
			l.cache.logf("lock release failed", "key", l.key, "err", rerr)
		}
	}()
	return fn(ctx)
}
