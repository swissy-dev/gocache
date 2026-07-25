package gocache

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

type interceptedGetDriver struct {
	*memory.Driver
	mu      sync.Mutex
	armedAt string
	hook    func()
}

func (d *interceptedGetDriver) arm(fullKey string, hook func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armedAt = fullKey
	d.hook = hook
}

func (d *interceptedGetDriver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	value, found, err := d.Driver.Get(ctx, key)
	d.mu.Lock()
	hook := d.hook
	if key == d.armedAt {
		d.armedAt = ""
		d.hook = nil
	} else {
		hook = nil
	}
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	return value, found, err
}

func fencedPair(t *testing.T, opts ...Option) (*Cache, *memory.Driver, *interceptedGetDriver) {
	t.Helper()
	l1 := memory.New()
	l2 := &interceptedGetDriver{Driver: memory.New()}
	c, err := New(append([]Option{WithL1(l1), WithL2(l2)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, l1, l2
}

func seedForeverAndColdL1(t *testing.T, c *Cache, l1 *memory.Driver, key string) {
	t.Helper()
	ctx := context.Background()
	if err := SetForever(ctx, c, key, user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if err := l1.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}
}

func assertNotResurrected(t *testing.T, c *Cache, l1 *memory.Driver, key string) {
	t.Helper()
	ctx := context.Background()
	if _, ok, err := l1.Get(ctx, c.key(key)); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("a late backfill wrote an invalidated value back into l1")
	}
	if _, ok, err := Get[user](ctx, c, key); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("an invalidated key must read as a miss")
	}
}

func TestBackfillDoesNotResurrectAValueDeletedDuringTheL2Read(t *testing.T) {
	c, l1, l2 := fencedPair(t)
	ctx := context.Background()
	seedForeverAndColdL1(t, c, l1, "u")

	l2.arm(c.key("u"), func() {
		if _, err := c.Delete(ctx, "u"); err != nil {
			t.Error(err)
		}
	})
	if _, _, err := Get[user](ctx, c, "u"); err != nil {
		t.Fatal(err)
	}

	assertNotResurrected(t, c, l1, "u")
}

func TestBackfillDoesNotResurrectAValueClearedDuringTheL2Read(t *testing.T) {
	c, l1, l2 := fencedPair(t)
	ctx := context.Background()
	seedForeverAndColdL1(t, c, l1, "u")

	l2.arm(c.key("u"), func() {
		if err := c.Clear(ctx); err != nil {
			t.Error(err)
		}
	})
	if _, _, err := Get[user](ctx, c, "u"); err != nil {
		t.Fatal(err)
	}

	assertNotResurrected(t, c, l1, "u")
}

func TestBackfillDoesNotResurrectAValueAPeerDeletedDuringTheL2Read(t *testing.T) {
	c, l1, l2 := fencedPair(t, WithBus(nopBus{}))
	ctx := context.Background()
	seedForeverAndColdL1(t, c, l1, "u")

	msg, err := json.Marshal(busMsg{Origin: "peer", Op: "delete", Keys: []string{c.key("u")}})
	if err != nil {
		t.Fatal(err)
	}
	l2.arm(c.key("u"), func() {
		if _, derr := l2.Delete(ctx, c.key("u")); derr != nil {
			t.Error(derr)
		}
		c.handleBusMsg(ctx, msg)
	})
	if _, _, err := Get[user](ctx, c, "u"); err != nil {
		t.Fatal(err)
	}

	assertNotResurrected(t, c, l1, "u")
}

func TestTagMarkerCacheDoesNotResurrectAnInvalidatedMarker(t *testing.T) {
	clk := newFakeClock()
	c, l1, l2 := fencedPair(t, WithClock(clk.Now))
	ctx := context.Background()

	if err := SetForever(ctx, c, "u", user{Name: "v1"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if err := l1.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}

	l2.arm(c.tagKey("users"), func() {
		clk.Advance(time.Millisecond)
		if err := c.DeleteByTag(ctx, "users"); err != nil {
			t.Error(err)
		}
	})
	if _, _, err := Get[user](ctx, c, "u"); err != nil {
		t.Fatal(err)
	}

	if raw, ok, err := l1.Get(ctx, c.tagKey("users")); err != nil {
		t.Fatal(err)
	} else if ok && string(raw) == "0" {
		t.Fatal("a late tag-marker cache write resurrected the pre-invalidation marker")
	}
	if _, ok, err := Get[user](ctx, c, "u"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("a tagged entry must stay invalid after DeleteByTag")
	}
}

func TestUncontendedReadStillBackfillsL1(t *testing.T) {
	var hits []Tier
	l1 := memory.New()
	l2 := memory.New()
	c, err := New(WithL1(l1), WithL2(l2), WithEventHook(func(e Event) {
		if hit, ok := e.(EventHit); ok {
			hits = append(hits, hit.Tier)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if err := SetForever(ctx, c, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	if err := l1.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}

	if got, ok, err := Get[user](ctx, c, "u"); err != nil || !ok || got.Name != "v1" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := l1.Get(ctx, c.key("u")); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("an uncontended read must still backfill l1")
	}
	if got, ok, err := Get[user](ctx, c, "u"); err != nil || !ok || got.Name != "v1" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
	if want := []Tier{TierL2, TierL1}; !slices.Equal(hits, want) {
		t.Fatalf("hit tiers = %v, want %v", hits, want)
	}
}

type partialDeleteDriver struct {
	*memory.Driver
	failSet     bool
	failDelete  bool
	commitFirst int
}

func (d *partialDeleteDriver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if d.failSet {
		return errors.New("gocache: mock authoritative set failed")
	}
	return d.Driver.Set(ctx, key, value, ttl)
}

func (d *partialDeleteDriver) Delete(ctx context.Context, key string) (bool, error) {
	if d.failDelete {
		return false, errors.New("gocache: mock authoritative delete failed")
	}
	return d.Driver.Delete(ctx, key)
}

func (d *partialDeleteDriver) DeleteMany(ctx context.Context, keys []string) error {
	if !d.failDelete {
		return d.Driver.DeleteMany(ctx, keys)
	}
	if err := d.Driver.DeleteMany(ctx, keys[:min(d.commitFirst, len(keys))]); err != nil {
		return err
	}
	return errors.New("gocache: mock authoritative delete many failed after the first chunk")
}

func (d *partialDeleteDriver) ClearPrefix(ctx context.Context, prefix string) error {
	if d.failDelete {
		return errors.New("gocache: mock authoritative clear failed")
	}
	return d.Driver.ClearPrefix(ctx, prefix)
}

func failingAuthCache(t *testing.T) (*Cache, *partialDeleteDriver, *countingBus) {
	t.Helper()
	bus := &countingBus{}
	l2 := &partialDeleteDriver{Driver: memory.New(), commitFirst: 1}
	c, err := New(WithL1(memory.New()), WithL2(l2), WithBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, l2, bus
}

func publishedSince(t *testing.T, bus *countingBus, before int) []busMsg {
	t.Helper()
	raw := bus.messages()[before:]
	out := make([]busMsg, len(raw))
	for i, r := range raw {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestDeleteManyPublishesEveryKeyWhenTheAuthoritativeTierFailsPartway(t *testing.T) {
	c, l2, bus := failingAuthCache(t)
	ctx := context.Background()
	for _, k := range []string{"a", "b"} {
		if err := SetForever(ctx, c, k, user{Name: k}); err != nil {
			t.Fatal(err)
		}
	}

	l2.failDelete = true
	before := bus.count()
	if err := c.DeleteMany(ctx, []string{"a", "b"}); err == nil {
		t.Fatal("expected the partial l2 failure to reach the caller")
	}

	if _, ok, err := l2.Get(ctx, c.key("a")); err != nil || ok {
		t.Fatalf("the committed chunk must be gone from l2, ok=%v err=%v", ok, err)
	}
	msgs := publishedSince(t, bus, before)
	if len(msgs) != 1 {
		t.Fatalf("bus publishes = %d, want 1 despite the partial failure", len(msgs))
	}
	want := []string{c.key("a"), c.key("b")}
	if msgs[0].Op != "delete" || !slices.Equal(msgs[0].Keys, want) {
		t.Fatalf("published %+v, want a delete for %v", msgs[0], want)
	}
}

func TestDeletePublishesWhenTheAuthoritativeDeleteFails(t *testing.T) {
	c, l2, bus := failingAuthCache(t)
	ctx := context.Background()
	if err := SetForever(ctx, c, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}

	l2.failDelete = true
	before := bus.count()
	if _, err := c.Delete(ctx, "u"); err == nil {
		t.Fatal("expected the l2 failure to reach the caller")
	}

	msgs := publishedSince(t, bus, before)
	if len(msgs) != 1 || msgs[0].Op != "delete" || !slices.Equal(msgs[0].Keys, []string{c.key("u")}) {
		t.Fatalf("published %+v, want one delete for %q", msgs, c.key("u"))
	}
}

func TestClearPublishesWhenTheAuthoritativeClearFails(t *testing.T) {
	c, l2, bus := failingAuthCache(t)
	ctx := context.Background()
	if err := SetForever(ctx, c, "u", user{Name: "v1"}); err != nil {
		t.Fatal(err)
	}

	l2.failDelete = true
	before := bus.count()
	if err := c.Clear(ctx); err == nil {
		t.Fatal("expected the l2 failure to reach the caller")
	}

	msgs := publishedSince(t, bus, before)
	if len(msgs) != 1 || msgs[0].Op != "clear" || msgs[0].Prefix != c.scopedPrefix(domainData) {
		t.Fatalf("published %+v, want one clear for %q", msgs, c.scopedPrefix(domainData))
	}
}

func TestDeleteByTagPublishesEveryTagWhenTheMarkerWriteFails(t *testing.T) {
	c, l2, bus := failingAuthCache(t)
	ctx := context.Background()

	l2.failSet = true
	before := bus.count()
	if err := c.DeleteByTag(ctx, "users", "posts"); err == nil {
		t.Fatal("expected the l2 failure to reach the caller")
	}

	msgs := publishedSince(t, bus, before)
	if len(msgs) != 2 {
		t.Fatalf("bus publishes = %d, want one per attempted tag", len(msgs))
	}
	for i, want := range []string{"users", "posts"} {
		if msgs[i].Op != "tag" || msgs[i].Tag != want {
			t.Fatalf("published %+v, want a tag message for %q", msgs[i], want)
		}
	}
}
