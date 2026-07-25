package gocache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

func TestGetOrSetComputesOnMiss(t *testing.T) {
	c, _ := newTestCache(t)
	v, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{Name: "ana"}, nil
	})
	if err != nil || v.Name != "ana" {
		t.Fatalf("v=%+v err=%v", v, err)
	}
	got, ok, err := Get[user](context.Background(), c, "k")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("cached got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestGetOrSetSkipsFactoryOnHit(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "cached"}); err != nil {
		t.Fatal(err)
	}
	v, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
		t.Fatal("factory must not run")
		return user{}, nil
	})
	if err != nil || v.Name != "cached" {
		t.Fatalf("v=%+v err=%v", v, err)
	}
}

func TestStampedeProtection(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	var calls atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := GetOrSet(ctx, c, "k", func(context.Context) (user, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return user{Name: "ana"}, nil
			})
			if err != nil || v.Name != "ana" {
				t.Errorf("v=%+v err=%v", v, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("factory ran %d times", calls.Load())
	}
}

func TestGetOrSetFactoryErrorNoGrace(t *testing.T) {
	c, _ := newTestCache(t)
	boom := errors.New("boom")
	_, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{}, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	v, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		return user{Name: "recovered"}, nil
	})
	if err != nil || v.Name != "recovered" {
		t.Fatalf("flight not forgotten after error: v=%+v err=%v", v, err)
	}
}

func TestGetOrSetFactoryPanicBecomesError(t *testing.T) {
	c, _ := newTestCache(t)
	_, err := GetOrSet(context.Background(), c, "k", func(context.Context) (user, error) {
		panic("kaboom")
	})
	if err == nil || !strings.Contains(err.Error(), "factory panic") {
		t.Fatalf("err = %v", err)
	}
}

func TestFactoryConcurrencyIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 2
		const keys = 8
		c, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(limit))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		var mu sync.Mutex
		var live, peak int
		release := make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		defer releaseAll()
		var wg sync.WaitGroup
		for i := range keys {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := GetOrSet(context.Background(), c, "k"+strconv.Itoa(i), func(context.Context) (user, error) {
					mu.Lock()
					live++
					peak = max(peak, live)
					mu.Unlock()
					<-release
					mu.Lock()
					live--
					mu.Unlock()
					return user{Name: "v"}, nil
				})
				if err != nil {
					t.Errorf("GetOrSet: %v", err)
				}
			}()
		}
		synctest.Wait()

		mu.Lock()
		running := live
		mu.Unlock()
		if running != limit {
			t.Fatalf("%d factories running at once, want %d", running, limit)
		}

		releaseAll()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if peak != limit {
			t.Fatalf("peak concurrent factories = %d, want %d", peak, limit)
		}
	})
}

func TestSameKeyCallersShareOneFactorySlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(2))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		var shared, other atomic.Int32
		release := make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		defer releaseAll()
		var wg sync.WaitGroup
		for range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := GetOrSet(context.Background(), c, "same", func(context.Context) (user, error) {
					shared.Add(1)
					<-release
					return user{Name: "shared"}, nil
				}); err != nil {
					t.Errorf("shared GetOrSet: %v", err)
				}
			}()
		}
		synctest.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := GetOrSet(context.Background(), c, "other", func(context.Context) (user, error) {
				other.Add(1)
				<-release
				return user{Name: "other"}, nil
			}); err != nil {
				t.Errorf("other GetOrSet: %v", err)
			}
		}()
		synctest.Wait()

		if got := shared.Load(); got != 1 {
			t.Fatalf("the shared key ran %d factories, want 1", got)
		}
		if got := other.Load(); got != 1 {
			t.Fatalf("the second key ran %d factories, want 1: callers on one key consumed more than one slot", got)
		}

		releaseAll()
		wg.Wait()
		if got := shared.Load(); got != 1 {
			t.Fatalf("the shared key ran %d factories in total, want 1", got)
		}
	})
}

func TestPanickingFactoryReleasesItsSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		if _, err := GetOrSet(context.Background(), c, "boom", func(context.Context) (user, error) {
			panic("kaboom")
		}); err == nil {
			t.Fatal("expected the panic to surface as an error")
		}
		v, err := GetOrSet(context.Background(), c, "next", func(context.Context) (user, error) {
			return user{Name: "next"}, nil
		})
		if err != nil || v.Name != "next" {
			t.Fatalf("v=%+v err=%v: the panicking flight never gave its slot back", v, err)
		}
	})
}

func TestCallerWaitingForAFactorySlotObservesItsOwnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		release := make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		defer releaseAll()
		held := make(chan error, 1)
		go func() {
			_, err := GetOrSet(context.Background(), c, "holder", func(context.Context) (user, error) {
				<-release
				return user{Name: "holder"}, nil
			})
			held <- err
		}()
		synctest.Wait()

		wctx, cancel := context.WithCancel(context.Background())
		var ran atomic.Bool
		waited := make(chan error, 1)
		go func() {
			_, err := GetOrSet(wctx, c, "waiter", func(context.Context) (user, error) {
				ran.Store(true)
				return user{Name: "waiter"}, nil
			})
			waited <- err
		}()
		synctest.Wait()

		select {
		case err := <-waited:
			t.Fatalf("the waiting caller returned %v before its context was cancelled", err)
		default:
		}

		cancel()
		synctest.Wait()
		select {
		case err := <-waited:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("the caller never returned after its own context was cancelled")
		}
		if ran.Load() {
			t.Fatal("the waiting caller's factory ran while the only slot was held")
		}

		releaseAll()
		synctest.Wait()
		if err := <-held; err != nil {
			t.Fatal(err)
		}
	})
}

func TestCloseReleasesFlightsQueuedForAFactorySlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(1))
		if err != nil {
			t.Fatal(err)
		}

		release := make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		defer releaseAll()
		go func() {
			_, _ = GetOrSet(context.Background(), c, "holder", func(context.Context) (user, error) {
				<-release
				return user{Name: "holder"}, nil
			})
		}()
		synctest.Wait()

		var queuedRan atomic.Bool
		queued := make(chan error, 1)
		go func() {
			_, err := GetOrSet(context.Background(), c, "queued", func(context.Context) (user, error) {
				queuedRan.Store(true)
				return user{Name: "queued"}, nil
			})
			queued <- err
		}()
		synctest.Wait()

		closed := make(chan error, 1)
		go func() { closed <- c.Close() }()
		synctest.Wait()

		select {
		case <-closed:
			t.Fatal("Close returned while a factory was still running")
		default:
		}
		select {
		case err := <-queued:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("queued flight err = %v, want ErrClosed", err)
			}
		default:
			t.Fatal("Close left a flight queued for a slot instead of releasing it")
		}
		if queuedRan.Load() {
			t.Fatal("a queued factory started after the cache closed")
		}

		releaseAll()
		synctest.Wait()
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	})
}

func TestCallerCancellationLeavesFlightRunning(t *testing.T) {
	c, _ := newTestCache(t)
	release := make(chan struct{})
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := GetOrSet(cctx, c, "k", func(fctx context.Context) (user, error) {
			<-release
			if fctx.Err() != nil {
				return user{}, fctx.Err()
			}
			return user{Name: "late"}, nil
		})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok, _ := Get[user](context.Background(), c, "k"); ok && v.Name == "late" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("flight result never written after caller left")
}
