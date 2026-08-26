package gocache

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

type ctxCheckingDriver struct {
	*memory.Driver
}

func (d *ctxCheckingDriver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return d.Driver.Add(ctx, key, value, ttl)
}

func (d *ctxCheckingDriver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return d.Driver.DeleteIfEquals(ctx, key, value)
}

func (d *ctxCheckingDriver) Delete(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return d.Driver.Delete(ctx, key)
}

type tokenRecordingDriver struct {
	*memory.Driver
	mu       sync.Mutex
	acquired [][]byte
	compared [][]byte
}

func (d *tokenRecordingDriver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	ok, err := d.Driver.Add(ctx, key, value, ttl)
	if ok {
		d.mu.Lock()
		d.acquired = append(d.acquired, bytes.Clone(value))
		d.mu.Unlock()
	}
	return ok, err
}

func (d *tokenRecordingDriver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	d.mu.Lock()
	d.compared = append(d.compared, bytes.Clone(value))
	d.mu.Unlock()
	return d.Driver.DeleteIfEquals(ctx, key, value)
}

func (d *tokenRecordingDriver) tokens() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.acquired)
}

func (d *tokenRecordingDriver) comparisons() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.compared)
}

func newRecordingCache(t *testing.T) (*Cache, *tokenRecordingDriver) {
	t.Helper()
	d := &tokenRecordingDriver{Driver: memory.New()}
	c, err := New(WithL1(d))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, d
}

func TestAcquireMintsAFreshTokenForEachLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, d := newRecordingCache(t)
		ctx := context.Background()
		l := c.Lock("job", time.Second)
		if ok, err := l.Acquire(ctx); err != nil || !ok {
			t.Fatalf("first acquire ok=%v err=%v", ok, err)
		}
		time.Sleep(2 * time.Second)
		if ok, err := l.Acquire(ctx); err != nil || !ok {
			t.Fatalf("second acquire ok=%v err=%v", ok, err)
		}
		tokens := d.tokens()
		if len(tokens) != 2 {
			t.Fatalf("recorded %d leases", len(tokens))
		}
		if bytes.Equal(tokens[0], tokens[1]) {
			t.Fatal("the second lease reuses the lapsed lease's owner token")
		}
		ok, err := d.DeleteIfEquals(ctx, c.lockKey("job"), tokens[0])
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("a token retired with the first lease deleted the second lease")
		}
		if ok, err := c.Lock("job", time.Second).Acquire(ctx); err != nil || ok {
			t.Fatalf("the second lease is no longer held: ok=%v err=%v", ok, err)
		}
	})
}

func TestReleaseWithNothingHeldIsANoOp(t *testing.T) {
	c, d := newRecordingCache(t)
	ctx := context.Background()
	l := c.Lock("job", time.Minute)
	if ok, err := l.Acquire(ctx); err != nil || !ok {
		t.Fatalf("acquire ok=%v err=%v", ok, err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	before := d.comparisons()
	if err := l.Release(ctx); err != nil {
		t.Fatalf("releasing nothing must return nil: %v", err)
	}
	if d.comparisons() != before {
		t.Fatal("a release with nothing held replayed a retired owner token")
	}
	other := c.Lock("job", time.Minute)
	if ok, err := other.Acquire(ctx); err != nil || !ok {
		t.Fatalf("other acquire ok=%v err=%v", ok, err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if d.comparisons() != before {
		t.Fatal("a release with nothing held attacked another owner's lease")
	}
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || ok {
		t.Fatalf("another owner's lease was released: ok=%v err=%v", ok, err)
	}
}

func TestForceReleaseRetiresTheLocalToken(t *testing.T) {
	c, d := newRecordingCache(t)
	ctx := context.Background()
	l := c.Lock("job", time.Minute)
	if ok, err := l.Acquire(ctx); err != nil || !ok {
		t.Fatalf("acquire ok=%v err=%v", ok, err)
	}
	if err := l.ForceRelease(ctx); err != nil {
		t.Fatal(err)
	}
	before := d.comparisons()
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if d.comparisons() != before {
		t.Fatal("release replayed a token that force release should have retired")
	}
}

func TestDoRetiresTheTokenItReleased(t *testing.T) {
	c, d := newRecordingCache(t)
	ctx := context.Background()
	l := c.Lock("job", time.Minute)
	if err := l.Do(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("do did not release: ok=%v err=%v", ok, err)
	}
	before := d.comparisons()
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if d.comparisons() != before {
		t.Fatal("release replayed the token do already retired")
	}
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || ok {
		t.Fatalf("the lease taken after do returned was released: ok=%v err=%v", ok, err)
	}
}

func TestLockExclusion(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	a := c.Lock("job", time.Minute)
	b := c.Lock("job", time.Minute)
	ok, err := a.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("a ok=%v err=%v", ok, err)
	}
	ok, err = b.Acquire(ctx)
	if err != nil || ok {
		t.Fatalf("b ok=%v err=%v", ok, err)
	}
	if err := a.Release(ctx); err != nil {
		t.Fatal(err)
	}
	ok, err = b.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("b after release ok=%v err=%v", ok, err)
	}
}

func TestReleaseDoesNotStealAnotherOwnersLock(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	a := c.Lock("job", time.Minute)
	if ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a ok=%v err=%v", ok, err)
	}
	if err := a.ForceRelease(ctx); err != nil {
		t.Fatal(err)
	}
	b := c.Lock("job", time.Minute)
	if ok, err := b.Acquire(ctx); err != nil || !ok {
		t.Fatalf("b ok=%v err=%v", ok, err)
	}
	if err := a.Release(ctx); err != nil {
		t.Fatal(err)
	}
	third := c.Lock("job", time.Minute)
	if ok, err := third.Acquire(ctx); err != nil || ok {
		t.Fatalf("b's lock was stolen: ok=%v err=%v", ok, err)
	}
}

func TestLockDo(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	var ran bool
	if err := c.Lock("job", time.Minute).Do(ctx, func(context.Context) error {
		ran = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("callback did not run")
	}
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("lock not released: ok=%v err=%v", ok, err)
	}
}

func TestLockDoReturnsErrLockHeld(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	held := c.Lock("job", time.Minute)
	if ok, err := held.Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	err := c.Lock("job", time.Minute).Do(ctx, func(context.Context) error {
		t.Fatal("callback must not run")
		return nil
	})
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("err = %v", err)
	}
}

func TestLockDoReleasesOnPanic(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	func() {
		defer func() {
			if p := recover(); p == nil {
				t.Error("panic did not propagate")
			}
		}()
		_ = c.Lock("job", time.Minute).Do(ctx, func(context.Context) error {
			panic("boom")
		})
	}()
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("lock not released after panic: ok=%v err=%v", ok, err)
	}
}

func TestLockDoReleasesLockWhenCallerContextCanceled(t *testing.T) {
	c, err := New(WithL1(&ctxCheckingDriver{Driver: memory.New()}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Lock("job", time.Minute).Do(ctx, func(context.Context) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ok, err := c.Lock("job", time.Minute).Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("lock was not released when the caller context was canceled")
	}
}

func TestLockNamespacePrefix(t *testing.T) {
	l1 := memory.New()
	c, err := New(WithL1(l1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	users := c.Namespace("users")
	if ok, err := users.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if _, found, _ := l1.Get(ctx, users.lockKey("job")); !found {
		t.Fatal("lock key is not namespaced")
	}
	if _, found, _ := l1.Get(ctx, c.lockKey("job")); found {
		t.Fatal("a namespaced lock must not land on the root lock key")
	}
}

func TestLockLivesOnL2(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	c, err := New(WithL1(l1), WithL2(l2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if _, found, _ := l2.Get(ctx, c.lockKey("job")); !found {
		t.Fatal("lock must live on l2")
	}
	if _, found, _ := l1.Get(ctx, c.lockKey("job")); found {
		t.Fatal("lock must not touch l1")
	}
}

func TestLockBlockAcquiresAfterRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		ctx := context.Background()
		held := c.Lock("job", time.Minute)
		if ok, err := held.Acquire(ctx); err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			_ = held.Release(ctx)
		}()
		if err := c.Lock("job", time.Minute).Block(ctx, 10*time.Second); err != nil {
			t.Fatalf("block err = %v", err)
		}
	})
}

func TestLockBlockTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		ctx := context.Background()
		if ok, err := c.Lock("job", time.Hour).Acquire(ctx); err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		err = c.Lock("job", time.Hour).Block(ctx, 500*time.Millisecond)
		if !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestLockExpiresWithTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		ctx := context.Background()
		if ok, err := c.Lock("job", time.Second).Acquire(ctx); err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		time.Sleep(2 * time.Second)
		if ok, err := c.Lock("job", time.Second).Acquire(ctx); err != nil || !ok {
			t.Fatalf("expired lock not reclaimable: ok=%v err=%v", ok, err)
		}
	})
}

func TestLockRejectsNonPositiveTTL(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	for _, ttl := range []time.Duration{0, -time.Second} {
		ok, err := c.Lock("job", ttl).Acquire(ctx)
		if err == nil {
			t.Fatalf("ttl %v: expected an error, got ok=%v", ttl, ok)
		}
		if ok {
			t.Fatalf("ttl %v: acquired a lock that would never expire", ttl)
		}
	}

	if err := c.Lock("job", 0).Do(ctx, func(context.Context) error {
		t.Fatal("callback must not run")
		return nil
	}); err == nil {
		t.Fatal("Do: expected an error for a non-positive ttl")
	}

	if err := c.Lock("job", 0).Block(ctx, time.Second); err == nil {
		t.Fatal("Block: expected an error for a non-positive ttl")
	}

	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("a valid lock must still acquire: ok=%v err=%v", ok, err)
	}
}

func TestLockReleaseFailureIsObservable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var failures []EventLockReleaseFailed
		c, err := New(
			WithL1(memory.New()),
			WithEventHook(func(e Event) {
				if f, ok := e.(EventLockReleaseFailed); ok {
					mu.Lock()
					failures = append(failures, f)
					mu.Unlock()
				}
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		ctx := context.Background()

		l := c.Lock("job", time.Second)
		if ok, err := l.Acquire(ctx); err != nil || !ok {
			t.Fatalf("acquire ok=%v err=%v", ok, err)
		}

		time.Sleep(2 * time.Second)
		if ok, err := c.Lock("job", time.Second).Acquire(ctx); err != nil || !ok {
			t.Fatalf("a lapsed lease must be acquirable: ok=%v err=%v", ok, err)
		}

		if err := l.Release(ctx); err != nil {
			t.Fatalf("releasing a lost lease reports success, got %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(failures) != 1 {
			t.Fatalf("expected one release failure event, got %d", len(failures))
		}
		if failures[0].Key != c.lockKey("job") {
			t.Fatalf("event carries key %q", failures[0].Key)
		}
		if failures[0].Err == nil {
			t.Fatal("the event must say why the release failed")
		}
	})
}
