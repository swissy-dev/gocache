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
		New: func(t *testing.T) gocache.Driver { return New() },
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
