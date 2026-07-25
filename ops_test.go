package gocache

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.UnixMilli(1_700_000_000_000)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

type user struct {
	Name string `json:"name"`
}

func newTestCache(t *testing.T, opts ...Option) (*Cache, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	c, err := New(append([]Option{WithL1(memory.New()), WithClock(clk.Now)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, clk
}

func TestSetGetRoundTrip(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Get[user](ctx, c, "u")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestGetMiss(t *testing.T) {
	c, _ := newTestCache(t)
	_, ok, err := Get[user](context.Background(), c, "absent")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestLogicalExpiry(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}, WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
	_, ok, err := Get[user](ctx, c, "u")
	if err != nil || ok {
		t.Fatalf("expected logical expiry, ok=%v err=%v", ok, err)
	}
}

func TestSetForever(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()
	if err := SetForever(ctx, c, "cfg", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(1000 * time.Hour)
	_, ok, err := Get[user](ctx, c, "cfg")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestTwoTierBackfill(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	clk := newFakeClock()
	c, err := New(WithL1(l1), WithL2(l2), WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	if err := l1.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, c, "u"); !ok {
		t.Fatal("expected L2 hit")
	}
	if _, ok, _ := l1.Get(ctx, "u"); !ok {
		t.Fatal("expected L1 backfill")
	}
}

func TestDeleteRemovesBothTiers(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	c, err := New(WithL1(l1), WithL2(l2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	existed, err := c.Delete(ctx, "u")
	if err != nil || !existed {
		t.Fatalf("existed=%v err=%v", existed, err)
	}
	if _, ok, _ := l2.Get(ctx, "u"); ok {
		t.Fatal("still in l2")
	}
	if _, ok, _ := l1.Get(ctx, "u"); ok {
		t.Fatal("still in l1")
	}
}

func TestPull(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Pull[user](ctx, c, "u")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := Get[user](ctx, c, "u"); ok {
		t.Fatal("pull must delete")
	}
}

func TestNamespaceIsolation(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	users := c.Namespace("users")
	posts := c.Namespace("posts")
	if err := Set(ctx, users, "1", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	if err := Set(ctx, posts, "1", user{Name: "bob"}); err != nil {
		t.Fatal(err)
	}
	if err := users.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, users, "1"); ok {
		t.Fatal("users:1 should be cleared")
	}
	if _, ok, _ := Get[user](ctx, posts, "1"); !ok {
		t.Fatal("posts:1 should survive")
	}
}

func TestHas(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	ok, err := c.Has(ctx, "u")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	ok, err = c.Has(ctx, "u")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestNegativeGraceCannotProduceImmortalEntry(t *testing.T) {
	l1 := memory.New()
	clk := newFakeClock()
	c, err := New(WithL1(l1), WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if got := c.callOpts([]CallOption{WithGrace(-2 * time.Hour)}).grace; got != 0 {
		t.Fatalf("expected negative per-call grace to be clamped to 0, got %v", got)
	}

	if err := Set(ctx, c, "u", user{Name: "ana"}, WithTTL(time.Minute), WithGrace(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l1.Get(ctx, "u"); !ok {
		t.Fatal("expected entry to be stored in l1 immediately after Set")
	}

	clk.Advance(2 * time.Minute)
	if _, ok, _ := Get[user](ctx, c, "u"); ok {
		t.Fatal("expected logical expiry to hide the entry despite the negative grace request")
	}
}

func TestNegativeTTLIsClampedToNoExpiry(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()

	if got := c.callOpts([]CallOption{WithTTL(-time.Hour)}).ttl; got != 0 {
		t.Fatalf("expected negative per-call ttl to be clamped to 0, got %v", got)
	}

	if err := Set(ctx, c, "u", user{Name: "ana"}, WithTTL(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour)
	if _, ok, err := Get[user](ctx, c, "u"); err != nil || !ok {
		t.Fatalf("negative ttl must mean no expiry, ok=%v err=%v", ok, err)
	}
}

func TestForeverHelpersDoNotWriteCallerSpareCapacity(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	opts := make([]CallOption, 1, 2)
	opts[0] = WithTags("users")
	spare := opts[:2]
	spare[1] = WithTTL(time.Hour)

	if err := SetForever(ctx, c, "a", user{Name: "ana"}, opts...); err != nil {
		t.Fatal(err)
	}
	var o callOpts
	spare[1](&o)
	if o.ttl != time.Hour {
		t.Fatal("SetForever wrote into the caller's spare capacity")
	}

	if _, err := GetOrSetForever(ctx, c, "b", func(context.Context) (user, error) {
		return user{Name: "bob"}, nil
	}, opts...); err != nil {
		t.Fatal(err)
	}
	o = callOpts{}
	spare[1](&o)
	if o.ttl != time.Hour {
		t.Fatal("GetOrSetForever wrote into the caller's spare capacity")
	}
}

type failingWriteDriver struct {
	*memory.Driver
	failSet    bool
	failDelete bool
}

func (d *failingWriteDriver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if d.failSet {
		return errors.New("gocache: mock l1 set failed")
	}
	return d.Driver.Set(ctx, key, value, ttl)
}

func (d *failingWriteDriver) Delete(ctx context.Context, key string) (bool, error) {
	if d.failDelete {
		return false, errors.New("gocache: mock l1 delete failed")
	}
	return d.Driver.Delete(ctx, key)
}

type countingBus struct {
	mu   sync.Mutex
	msgs [][]byte
}

func (b *countingBus) Publish(ctx context.Context, msg []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = append(b.msgs, slices.Clone(msg))
	return nil
}

func (b *countingBus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	return nil
}

func (b *countingBus) Close() error { return nil }

func (b *countingBus) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.msgs)
}

func TestSetPublishesWhenOnlyTheL1WriteFails(t *testing.T) {
	bus := &countingBus{}
	l2 := memory.New()
	c, err := New(
		WithL1(&failingWriteDriver{Driver: memory.New(), failSet: true}),
		WithL2(l2),
		WithBus(bus),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if err := Set(ctx, c, "u", user{Name: "ana"}); err == nil {
		t.Fatal("expected the l1 write failure to be returned")
	}
	if _, ok, err := l2.Get(ctx, "u"); err != nil || !ok {
		t.Fatalf("l2 must hold the value, ok=%v err=%v", ok, err)
	}
	if got := bus.count(); got != 1 {
		t.Fatalf("bus publishes = %d, want 1 so peers drop their stale l1 copies", got)
	}
}

func TestDeletePublishesWhenOnlyTheL1DeleteFails(t *testing.T) {
	bus := &countingBus{}
	l2 := memory.New()
	l1 := &failingWriteDriver{Driver: memory.New()}
	c, err := New(WithL1(l1), WithL2(l2), WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	l1.failDelete = true
	before := bus.count()
	if _, err := c.Delete(ctx, "u"); err == nil {
		t.Fatal("expected the l1 delete failure to be returned")
	}
	if _, ok, err := l2.Get(ctx, "u"); err != nil || ok {
		t.Fatalf("l2 must have dropped the value, ok=%v err=%v", ok, err)
	}
	if got := bus.count(); got != before+1 {
		t.Fatalf("bus publishes = %d, want %d", got, before+1)
	}
}

func TestPhysicalTTLGraceKeepsEntryPhysicallyPresent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l1 := memory.New()
		clk := newFakeClock()
		grace := 10 * time.Minute
		c, err := New(WithL1(l1), WithClock(clk.Now), WithDefaultGrace(grace))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		ctx := context.Background()

		if err := Set(ctx, c, "u", user{Name: "ana"}, WithTTL(time.Minute)); err != nil {
			t.Fatal(err)
		}

		advance := 5 * time.Minute
		clk.Advance(advance)
		time.Sleep(advance)

		if _, ok, err := l1.Get(ctx, "u"); err != nil || !ok {
			t.Fatalf("expected entry to still be physically present in l1 under grace, ok=%v err=%v", ok, err)
		}
		if _, ok, err := Get[user](ctx, c, "u"); err != nil || ok {
			t.Fatalf("expected logical expiry to hide the entry despite physical presence, ok=%v err=%v", ok, err)
		}
	})
}

func TestEventsDeletedCleared(t *testing.T) {
	var events []Event
	c, _ := newTestCache(t, WithEventHook(func(e Event) { events = append(events, e) }))
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Delete(ctx, "u"); err != nil {
		t.Fatal(err)
	}
	if err := c.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	var deleted, cleared bool
	for _, e := range events {
		switch e.(type) {
		case EventDeleted:
			deleted = true
		case EventCleared:
			cleared = true
		}
	}
	if !deleted || !cleared {
		t.Fatalf("deleted=%v cleared=%v", deleted, cleared)
	}
}

func TestEventsHitMiss(t *testing.T) {
	var events []Event
	c, _ := newTestCache(t, WithEventHook(func(e Event) { events = append(events, e) }))
	ctx := context.Background()
	_, _, _ = Get[user](ctx, c, "u")
	_ = Set(ctx, c, "u", user{Name: "ana"})
	_, _, _ = Get[user](ctx, c, "u")
	var miss, hit, written bool
	for _, e := range events {
		switch e.(type) {
		case EventMiss:
			miss = true
		case EventHit:
			hit = true
		case EventWritten:
			written = true
		}
	}
	if !miss || !hit || !written {
		t.Fatalf("miss=%v hit=%v written=%v", miss, hit, written)
	}
}

func TestSkipL1RemovesAnyExistingL1Copy(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	c, err := New(WithL1(l1), WithL2(l2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := Set(ctx, c, "k", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if err := Set(ctx, c, "k", user{Name: "v2"}, WithSkipL1()); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := l1.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("WithSkipL1 left a stale copy in l1")
	}

	got, found, err := Get[user](ctx, c, "k")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got.Name != "v2" {
		t.Fatalf("served a stale value after WithSkipL1: %+v", got)
	}
}
