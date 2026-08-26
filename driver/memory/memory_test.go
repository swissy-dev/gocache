package memory

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/drivertest"
)

func TestConformance(t *testing.T) {
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			t.Helper()
			return New()
		},
	})
}

func TestMaxEntriesClampedToOne(t *testing.T) {
	for _, n := range []int{0, -5} {
		d := New(WithMaxEntries(n))
		ctx := context.Background()
		if err := d.Set(ctx, "a", []byte("1"), 0); err != nil {
			t.Fatal(err)
		}
		v, ok, err := d.Get(ctx, "a")
		if err != nil || !ok || string(v) != "1" {
			t.Fatalf("max entries %d: got %q ok=%v err=%v", n, v, ok, err)
		}
	}
}

func TestLRUEviction(t *testing.T) {
	d := New(WithMaxEntries(2))
	ctx := context.Background()
	if err := d.Set(ctx, "a", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "b", []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Get(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "c", []byte("3"), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(ctx, "b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok, _ := d.Get(ctx, "a"); !ok {
		t.Fatal("a should survive")
	}
	if _, ok, _ := d.Get(ctx, "c"); !ok {
		t.Fatal("c should survive")
	}
}

func TestEvictionPrefersExpiredEntries(t *testing.T) {
	d := New(WithMaxEntries(3))
	ctx := context.Background()

	set := func(k string, ttl time.Duration) {
		t.Helper()
		if err := d.Set(ctx, k, []byte(k), ttl); err != nil {
			t.Fatal(err)
		}
	}
	held := func(k string) bool {
		t.Helper()
		_, ok, err := d.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	set("live-old", time.Hour)
	set("expired", 20*time.Millisecond)
	set("live-new", time.Hour)
	time.Sleep(50 * time.Millisecond)

	set("live-c", time.Hour)

	if !held("live-old") {
		t.Fatal("evicted the least-recently-used live entry while an expired one was still held")
	}
	if held("expired") {
		t.Fatal("the expired entry should have been reclaimed")
	}
	for _, k := range []string{"live-new", "live-c"} {
		if !held(k) {
			t.Fatalf("%s should still be cached", k)
		}
	}
}

func liveKeys(t *testing.T, d *Driver, keys ...string) []string {
	t.Helper()
	var live []string
	for _, k := range keys {
		if _, ok, _ := d.Get(context.Background(), k); ok {
			live = append(live, k)
		}
	}
	return live
}

func TestMaxBytesEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxEntries(100), WithMaxBytes(int64(3*(len("a")+64))))
	val := make([]byte, 64)

	for _, k := range []string{"a", "b", "c"} {
		if err := d.Set(ctx, k, val, 0); err != nil {
			t.Fatal(err)
		}
	}
	if got := liveKeys(t, d, "a", "b", "c"); len(got) != 3 {
		t.Fatalf("all three fit the budget, got %v", got)
	}

	if _, ok, _ := d.Get(ctx, "a"); !ok {
		t.Fatal("a should still be live")
	}
	if err := d.Set(ctx, "e", val, 0); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := d.Get(ctx, "b"); ok {
		t.Fatal("b was least recently used and should have been evicted")
	}
	for _, k := range []string{"a", "c", "e"} {
		if _, ok, _ := d.Get(ctx, k); !ok {
			t.Fatalf("%s should have survived", k)
		}
	}
}

func TestMaxBytesTracksUpdatesAndRemovals(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxBytes(1000))

	if err := d.Set(ctx, "k", make([]byte, 500), 0); err != nil {
		t.Fatal(err)
	}
	if d.bytes != int64(len("k")+500) {
		t.Fatalf("after insert bytes=%d", d.bytes)
	}

	if err := d.Set(ctx, "k", make([]byte, 100), 0); err != nil {
		t.Fatal(err)
	}
	if d.bytes != int64(len("k")+100) {
		t.Fatalf("shrinking a value must shrink the total, got %d", d.bytes)
	}

	if _, err := d.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if d.bytes != 0 {
		t.Fatalf("deleting the only entry must zero the total, got %d", d.bytes)
	}
}

func TestMaxBytesReclaimsOnExpiry(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxBytes(1000))
	if err := d.Set(ctx, "gone", make([]byte, 400), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, ok, _ := d.Get(ctx, "gone"); ok {
		t.Fatal("entry should have expired")
	}
	if d.bytes != 0 {
		t.Fatalf("an expired entry dropped on read must release its bytes, got %d", d.bytes)
	}
}

func TestValueLargerThanBudgetIsNotRetained(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxBytes(100))
	if err := d.Set(ctx, "huge", make([]byte, 500), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(ctx, "huge"); ok {
		t.Fatal("a value larger than the whole budget must not be retained")
	}
	if d.bytes != 0 {
		t.Fatalf("bytes must not stay above budget, got %d", d.bytes)
	}
}

func TestBothBoundsApply(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxEntries(2), WithMaxBytes(1_000_000))
	for _, k := range []string{"a", "b", "c"} {
		if err := d.Set(ctx, k, []byte("x"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if got := liveKeys(t, d, "a", "b", "c"); len(got) != 2 {
		t.Fatalf("entry bound must still apply under a generous byte budget, live=%v", got)
	}
}

func TestOversizedValueDoesNotEvictRetainedEntries(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxBytes(1000))
	for _, k := range []string{"a", "b", "c"} {
		if err := d.Set(ctx, k, make([]byte, 100), 0); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.Set(ctx, "huge", make([]byte, 5000), 0); err != nil {
		t.Fatal(err)
	}

	if got := liveKeys(t, d, "a", "b", "c"); len(got) != 3 {
		t.Fatalf("a value that cannot fit must not evict entries that do: only %v survived", got)
	}
	if _, ok, _ := d.Get(ctx, "huge"); ok {
		t.Fatal("the oversized value must not be retained")
	}
}

func TestOversizedUpdateDropsThePreviousValue(t *testing.T) {
	ctx := context.Background()
	d := New(WithMaxBytes(1000))
	if err := d.Set(ctx, "k", make([]byte, 100), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "k", make([]byte, 5000), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(ctx, "k"); ok {
		t.Fatal("overwriting with an unstorable value must not leave the previous one readable")
	}
	if d.bytes != 0 {
		t.Fatalf("bytes=%d after the entry was dropped", d.bytes)
	}
}
