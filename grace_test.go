package gocache

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
	"github.com/swissy-dev/gocache/driver/null"
)

func staleEntry(t *testing.T, c *Cache, clk *fakeClock) {
	t.Helper()
	if err := Set(context.Background(), c, "k", user{Name: "stale"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
}

func TestGraceServesStaleOnFactoryError(t *testing.T) {
	c, clk := newTestCache(t, WithDefaultGrace(time.Hour))
	staleEntry(t, c, clk)
	boom := errors.New("boom")
	v, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{}, boom
	})
	if err != nil {
		t.Fatalf("expected grace hit, err = %v", err)
	}
	if v.Name != "stale" {
		t.Fatalf("v = %+v", v)
	}
}

func TestGraceHitEmitsEventWithFactoryError(t *testing.T) {
	boom := errors.New("boom")
	var graceErr error
	c, clk := newTestCache(t, WithDefaultGrace(time.Hour), WithEventHook(func(e Event) {
		if ev, ok := e.(EventGraceHit); ok && ev.Err != nil {
			graceErr = ev.Err
		}
	}))
	staleEntry(t, c, clk)
	if _, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{}, boom
	}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(graceErr, boom) {
		t.Fatalf("event error = %v", graceErr)
	}
}

func TestNoGraceReturnsFactoryError(t *testing.T) {
	c, clk := newTestCache(t)
	staleEntry(t, c, clk)
	boom := errors.New("boom")
	if _, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{}, boom
	}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestPerCallGraceZeroDisablesGrace(t *testing.T) {
	c, clk := newTestCache(t, WithDefaultGrace(time.Hour))
	staleEntry(t, c, clk)
	boom := errors.New("boom")
	if _, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{}, boom
	}, WithGrace(0)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestPerCallGraceNarrowerThanDefaultIsHonored(t *testing.T) {
	c, clk := newTestCache(t, WithDefaultGrace(6*time.Hour))
	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "stale"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute + 2*time.Hour)
	boom := errors.New("boom")
	if _, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
		return user{}, boom
	}, WithGrace(time.Minute)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestPerCallGraceWithinWindowStillServesStale(t *testing.T) {
	c, clk := newTestCache(t, WithDefaultGrace(6*time.Hour))
	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "stale"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute + 30*time.Second)
	v, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
		return user{}, errors.New("boom")
	}, WithGrace(time.Minute))
	if err != nil || v.Name != "stale" {
		t.Fatalf("v=%+v err=%v", v, err)
	}
}

func TestSoftTimeoutServesStaleWhileFactoryFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := newFakeClock()
		c, err := New(
			WithL1(memory.New()),
			WithClock(clk.Now),
			WithDefaultGrace(time.Hour),
			WithSoftTimeout(50*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		staleEntry(t, c, clk)
		release := make(chan struct{})
		start := time.Now()
		v, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
			<-release
			return user{Name: "fresh"}, nil
		})
		if err != nil || v.Name != "stale" {
			t.Fatalf("v=%+v err=%v", v, err)
		}
		if d := time.Since(start); d < 50*time.Millisecond || d > time.Second {
			t.Fatalf("soft timeout fired after %v", d)
		}
		close(release)
		synctest.Wait()
		got, ok, err := Get[user](context.Background(), c, "k")
		if err != nil || !ok || got.Name != "fresh" {
			t.Fatalf("background write missing: got=%+v ok=%v err=%v", got, ok, err)
		}
	})
}

func TestHardTimeoutCancelsFactory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()), WithHardTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		_, err = GetOrSet(context.Background(), c, "k", func(fctx context.Context) (user, error) {
			select {
			case <-fctx.Done():
				return user{}, fctx.Err()
			case <-time.After(time.Hour):
				return user{Name: "never"}, nil
			}
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestCloseWaitsForDetachedFactory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := newFakeClock()
		c, err := New(
			WithL1(memory.New()),
			WithClock(clk.Now),
			WithDefaultGrace(time.Hour),
			WithSoftTimeout(50*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		staleEntry(t, c, clk)
		release := make(chan struct{})
		if _, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
			<-release
			return user{Name: "fresh"}, nil
		}); err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- c.Close() }()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("Close returned while a factory was still in flight")
		default:
		}
		close(release)
		synctest.Wait()
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	})
}

func TestGraceServesStaleL1WhenL2HasDroppedTheKey(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	clk := newFakeClock()
	c, err := New(WithL1(l1), WithL2(l2), WithClock(clk.Now), WithDefaultGrace(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "stale"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := l2.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)

	boom := errors.New("boom")
	v, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
		return user{}, boom
	})
	if err != nil {
		t.Fatalf("expected a grace hit from the stale L1 copy, got %v", err)
	}
	if v.Name != "stale" {
		t.Fatalf("v = %+v", v)
	}
}

func TestGraceStillWorksWhenL2IsNullBacked(t *testing.T) {
	clk := newFakeClock()
	c, err := New(WithL1(memory.New()), WithL2(null.New()), WithClock(clk.Now), WithDefaultGrace(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "stale"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)

	v, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
		return user{}, errors.New("boom")
	})
	if err != nil {
		t.Fatalf("expected a grace hit, got %v", err)
	}
	if v.Name != "stale" {
		t.Fatalf("v = %+v", v)
	}
}
