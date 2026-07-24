package drivertest

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/gocache"
)

type Config struct {
	New     func(t *testing.T) gocache.Driver
	Advance func(t *testing.T, d time.Duration)
}

func (cfg Config) advance(t *testing.T, d time.Duration) {
	t.Helper()
	if cfg.Advance != nil {
		cfg.Advance(t, d)
		return
	}
	time.Sleep(d)
}

func Run(t *testing.T, cfg Config) {
	t.Run("SetGet", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		v, ok, err := d.Get(ctx, "k")
		if err != nil || !ok || string(v) != "v" {
			t.Fatalf("got %q ok=%v err=%v", v, ok, err)
		}
	})

	t.Run("GetMissing", func(t *testing.T) {
		d := cfg.New(t)
		_, ok, err := d.Get(context.Background(), "absent")
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("a"), 0); err != nil {
			t.Fatal(err)
		}
		if err := d.Set(ctx, "k", []byte("b"), 0); err != nil {
			t.Fatal(err)
		}
		v, _, _ := d.Get(ctx, "k")
		if string(v) != "b" {
			t.Fatalf("got %q", v)
		}
	})

	t.Run("TTLExpiry", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		cfg.advance(t, 200*time.Millisecond)
		_, ok, err := d.Get(ctx, "k")
		if err != nil || ok {
			t.Fatalf("expected expired, ok=%v err=%v", ok, err)
		}
	})

	t.Run("NoExpiry", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		cfg.advance(t, 200*time.Millisecond)
		v, ok, err := d.Get(ctx, "k")
		if err != nil || !ok || string(v) != "v" {
			t.Fatalf("zero ttl must not expire: got %q ok=%v err=%v", v, ok, err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		existed, err := d.Delete(ctx, "k")
		if err != nil || !existed {
			t.Fatalf("existed=%v err=%v", existed, err)
		}
		existed, err = d.Delete(ctx, "k")
		if err != nil || existed {
			t.Fatalf("second delete existed=%v err=%v", existed, err)
		}
	})

	t.Run("DeleteMany", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		for _, k := range []string{"a", "b", "c"} {
			if err := d.Set(ctx, k, []byte("v"), 0); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.DeleteMany(ctx, []string{"a", "b", "missing"}); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.Get(ctx, "a"); ok {
			t.Fatal("a not deleted")
		}
		if _, ok, _ := d.Get(ctx, "c"); !ok {
			t.Fatal("c should remain")
		}
	})

	t.Run("ClearPrefix", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		for _, k := range []string{"users:1", "users:2", "posts:1"} {
			if err := d.Set(ctx, k, []byte("v"), 0); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.ClearPrefix(ctx, "users:"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.Get(ctx, "users:1"); ok {
			t.Fatal("users:1 not cleared")
		}
		if _, ok, _ := d.Get(ctx, "posts:1"); !ok {
			t.Fatal("posts:1 should remain")
		}
	})

	t.Run("ClearPrefixEmpty", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		if err := d.ClearPrefix(ctx, ""); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.Get(ctx, "k"); ok {
			t.Fatal("empty prefix must clear everything")
		}
	})

	t.Run("Add", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		ok, err := d.Add(ctx, "k", []byte("a"), 0)
		if err != nil || !ok {
			t.Fatalf("first add ok=%v err=%v", ok, err)
		}
		ok, err = d.Add(ctx, "k", []byte("b"), 0)
		if err != nil || ok {
			t.Fatalf("second add ok=%v err=%v", ok, err)
		}
		v, _, _ := d.Get(ctx, "k")
		if string(v) != "a" {
			t.Fatalf("got %q", v)
		}
	})

	t.Run("AddAfterExpiry", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if _, err := d.Add(ctx, "k", []byte("a"), 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		cfg.advance(t, 200*time.Millisecond)
		ok, err := d.Add(ctx, "k", []byte("b"), 0)
		if err != nil || !ok {
			t.Fatalf("add after expiry ok=%v err=%v", ok, err)
		}
	})

	t.Run("DeleteIfEquals", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("token"), 0); err != nil {
			t.Fatal(err)
		}
		ok, err := d.DeleteIfEquals(ctx, "k", []byte("wrong"))
		if err != nil || ok {
			t.Fatalf("mismatch ok=%v err=%v", ok, err)
		}
		ok, err = d.DeleteIfEquals(ctx, "k", []byte("token"))
		if err != nil || !ok {
			t.Fatalf("match ok=%v err=%v", ok, err)
		}
		if _, found, _ := d.Get(ctx, "k"); found {
			t.Fatal("key should be gone")
		}
	})

	t.Run("DeleteIfEqualsExpired", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("token"), 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		cfg.advance(t, 200*time.Millisecond)
		ok, err := d.DeleteIfEquals(ctx, "k", []byte("token"))
		if err != nil || ok {
			t.Fatalf("expired ok=%v err=%v", ok, err)
		}
	})

	t.Run("DeleteExpiredReportsFalse", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		if err := d.Set(ctx, "k", []byte("v"), 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		cfg.advance(t, 200*time.Millisecond)
		existed, err := d.Delete(ctx, "k")
		if err != nil || existed {
			t.Fatalf("expired delete existed=%v err=%v", existed, err)
		}
	})

	t.Run("ValueIsolation", func(t *testing.T) {
		d := cfg.New(t)
		ctx := context.Background()
		buf := []byte("abc")
		if err := d.Set(ctx, "k", buf, 0); err != nil {
			t.Fatal(err)
		}
		buf[0] = 'x'
		v, _, _ := d.Get(ctx, "k")
		if string(v) != "abc" {
			t.Fatalf("driver aliased caller bytes: %q", v)
		}
	})
}
