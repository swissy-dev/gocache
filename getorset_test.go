package gocache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
