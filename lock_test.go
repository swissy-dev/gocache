package gocache

import (
	"context"
	"errors"
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
	if ok, err := c.Namespace("users").Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if _, found, _ := l1.Get(ctx, "__gocache:lock:users:job"); !found {
		t.Fatal("lock key is not namespaced")
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
	if _, found, _ := l2.Get(ctx, "__gocache:lock:job"); !found {
		t.Fatal("lock must live on l2")
	}
	if _, found, _ := l1.Get(ctx, "__gocache:lock:job"); found {
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
