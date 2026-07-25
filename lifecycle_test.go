package gocache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

type failingBus struct {
	handler func(ctx context.Context, msg []byte)
}

func (b *failingBus) Publish(ctx context.Context, msg []byte) error {
	return errors.New("gocache: mock bus publish failed")
}

func (b *failingBus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	b.handler = handler
	return nil
}

func (b *failingBus) Close() error {
	return nil
}

type syncEventLog struct {
	mu   sync.Mutex
	list []Event
}

func (s *syncEventLog) record(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.list = append(s.list, e)
}

func (s *syncEventLog) snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.list))
	copy(out, s.list)
	return out
}

func TestPublishFailureEmitsBusPublishFailedEvent(t *testing.T) {
	var log syncEventLog
	c, err := New(
		WithL1(memory.New()),
		WithL2(memory.New()),
		WithBus(&failingBus{}),
		WithEventHook(log.record),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, e := range log.snapshot() {
		pf, ok := e.(EventBusPublishFailed)
		if !ok {
			continue
		}
		if pf.Err == nil {
			t.Fatal("EventBusPublishFailed.Err must not be nil")
		}
		found = true
	}
	if !found {
		t.Fatal("expected an EventBusPublishFailed event")
	}
}

func TestEnqueueRetryDropsOldestOnOverflow(t *testing.T) {
	const capacity = 3
	var events []Event
	c, err := New(
		WithL1(memory.New()),
		WithBusRetryQueueSize(capacity),
		WithEventHook(func(e Event) { events = append(events, e) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < capacity; i++ {
		c.enqueueRetry([]byte{byte('0' + i)})
	}
	c.enqueueRetry([]byte{byte('0' + capacity)})

	var dropped bool
	for _, e := range events {
		if pf, ok := e.(EventBusPublishFailed); ok && pf.Dropped {
			dropped = true
		}
	}
	if !dropped {
		t.Fatal("expected an EventBusPublishFailed{Dropped: true} event")
	}

	if got := len(c.rt.retryQ); got != capacity {
		t.Fatalf("retryQ length = %d, want %d", got, capacity)
	}

	want := []byte{'1', '2', '3'}
	for _, w := range want {
		got := <-c.rt.retryQ
		if len(got) != 1 || got[0] != w {
			t.Fatalf("retryQ contents = %q, want oldest dropped and %q remaining", got, want)
		}
	}
}

type retryBus struct {
	mu       sync.Mutex
	attempts []time.Time
	failFor  int
}

func (b *retryBus) Publish(ctx context.Context, msg []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempts = append(b.attempts, time.Now())
	if len(b.attempts) <= b.failFor {
		return errors.New("gocache: bus down")
	}
	return nil
}

func (b *retryBus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	return nil
}

func (b *retryBus) Close() error { return nil }

func (b *retryBus) gaps() []time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []time.Duration
	for i := 1; i < len(b.attempts); i++ {
		out = append(out, b.attempts[i].Sub(b.attempts[i-1]))
	}
	return out
}

func TestRetryDrainerBacksOffExponentially(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := &retryBus{failFor: 4}
		c, err := New(
			WithL1(memory.New()),
			WithL2(memory.New()),
			WithBus(bus),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		ctx := context.Background()
		if err := Set(ctx, c, "k", user{Name: "v"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()

		var backoffs []time.Duration
		for _, g := range bus.gaps() {
			if g >= 50*time.Millisecond {
				backoffs = append(backoffs, g)
			}
		}
		if len(backoffs) < 3 {
			t.Fatalf("expected the drainer to back off repeatedly, got %v", backoffs)
		}
		if backoffs[0] > 200*time.Millisecond {
			t.Fatalf("first backoff was %v, want it to start small (all: %v)", backoffs[0], backoffs)
		}
		for i := 1; i < len(backoffs); i++ {
			if backoffs[i] != backoffs[i-1]*2 {
				t.Fatalf("backoff %d was %v, want double the previous %v (all: %v)",
					i+1, backoffs[i], backoffs[i-1], backoffs)
			}
		}
	})
}

func TestRetryDrainerBackoffIsCapped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := &retryBus{failFor: 10}
		c, err := New(
			WithL1(memory.New()),
			WithL2(memory.New()),
			WithBus(bus),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		if err := Set(context.Background(), c, "k", user{Name: "v"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()

		gaps := bus.gaps()
		if len(gaps) < 8 {
			t.Fatalf("expected the drainer to keep retrying, got %d gaps", len(gaps))
		}
		for i, gap := range gaps {
			if gap > 5*time.Second {
				t.Fatalf("retry %d waited %v, which exceeds the 5s cap", i+1, gap)
			}
		}
	})
}
