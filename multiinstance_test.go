package gocache

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/bus/memorybus"
	"github.com/swissy-dev/gocache/driver/memory"
)

func twoInstances(t *testing.T) (*Cache, *Cache, *memory.Driver, *memory.Driver, *fakeClock) {
	t.Helper()
	l2 := memory.New()
	bus := memorybus.New()
	clk := newFakeClock()
	l1a := memory.New()
	a, err := New(WithL1(l1a), WithL2(l2), WithBus(bus), WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	l1b := memory.New()
	b, err := New(WithL1(l1b), WithL2(l2), WithBus(bus), WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return a, b, l1a, l1b, clk
}

func TestBusEvictsPeerL1OnSet(t *testing.T) {
	a, b, l1a, l1b, _ := twoInstances(t)
	ctx := context.Background()
	if err := Set(ctx, a, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := Get[user](ctx, b, "u"); !ok || v.Name != "v1" {
		t.Fatalf("b read v=%+v ok=%v", v, ok)
	}
	if _, ok, _ := l1b.Get(ctx, b.key("u")); !ok {
		t.Fatal("b should have L1 copy after read")
	}
	if err := Set(ctx, a, "u", user{Name: "v2"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l1a.Get(ctx, a.key("u")); !ok {
		t.Fatal("a must keep its own L1 write (origin filter)")
	}
	if _, ok, _ := l1b.Get(ctx, b.key("u")); ok {
		t.Fatal("b's L1 must be evicted by the bus")
	}
	if v, ok, _ := Get[user](ctx, b, "u"); !ok || v.Name != "v2" {
		t.Fatalf("b must see v2, got %+v ok=%v", v, ok)
	}
}

func TestBusEvictsPeerL1OnDelete(t *testing.T) {
	a, b, _, l1b, _ := twoInstances(t)
	ctx := context.Background()
	if err := Set(ctx, a, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, b, "u"); !ok {
		t.Fatal("warm b")
	}
	if _, err := a.Delete(ctx, "u"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l1b.Get(ctx, b.key("u")); ok {
		t.Fatal("b's L1 must be evicted")
	}
	if _, ok, _ := Get[user](ctx, b, "u"); ok {
		t.Fatal("entry must be gone everywhere")
	}
}

func TestBusPropagatesTagInvalidation(t *testing.T) {
	a, b, _, _, clk := twoInstances(t)
	ctx := context.Background()
	if err := Set(ctx, a, "u", user{Name: "v1"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, b, "u"); !ok {
		t.Fatal("warm b")
	}
	clk.Advance(time.Millisecond)
	if err := a.DeleteByTag(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, b, "u"); ok {
		t.Fatal("b must see the tag invalidation immediately")
	}
}

func TestBusPropagatesClear(t *testing.T) {
	a, b, _, l1b, _ := twoInstances(t)
	ctx := context.Background()
	if err := Set(ctx, a, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, b, "u"); !ok {
		t.Fatal("warm b")
	}
	if err := a.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l1b.Get(ctx, b.key("u")); ok {
		t.Fatal("b's L1 must be cleared")
	}
}

func TestClosedInstanceIgnoresPeerBusMessage(t *testing.T) {
	a, b, _, l1b, _ := twoInstances(t)
	ctx := context.Background()
	if err := Set(ctx, a, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, b, "u"); !ok {
		t.Fatal("warm b")
	}
	if _, ok, _ := l1b.Get(ctx, b.key("u")); !ok {
		t.Fatal("b should have L1 copy before close")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	msg, err := json.Marshal(busMsg{Origin: a.rt.origin, Op: "delete", Keys: []string{a.key("u")}})
	if err != nil {
		t.Fatal(err)
	}
	b.handleBusMsg(ctx, msg)
	if _, ok, _ := l1b.Get(ctx, b.key("u")); !ok {
		t.Fatal("closed instance must not evict its L1 for a peer's bus message")
	}
}

func TestLockExcludesAcrossInstances(t *testing.T) {
	a, b, _, _, _ := twoInstances(t)
	ctx := context.Background()
	if ok, err := a.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("a ok=%v err=%v", ok, err)
	}
	if ok, err := b.Lock("job", time.Minute).Acquire(ctx); err != nil || ok {
		t.Fatalf("b acquired a lock held by a: ok=%v err=%v", ok, err)
	}
}
