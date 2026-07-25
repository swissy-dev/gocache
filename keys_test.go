package gocache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

func TestNamespaceAndKeyEncodingIsInjective(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	outer := c.Namespace("a")
	inner := c.Namespace("a:b")

	if err := SetForever(ctx, outer, "b:c", user{Name: "outer"}); err != nil {
		t.Fatal(err)
	}
	if err := SetForever(ctx, inner, "c", user{Name: "inner"}); err != nil {
		t.Fatal(err)
	}

	if got, other := outer.key("b:c"), inner.key("c"); got == other {
		t.Fatalf("distinct namespace/key pairs encode to the same stored key %q", got)
	}

	got, ok, err := Get[user](ctx, outer, "b:c")
	if err != nil || !ok || got.Name != "outer" {
		t.Fatalf(`Namespace("a") key "b:c": got=%+v ok=%v err=%v`, got, ok, err)
	}
	got, ok, err = Get[user](ctx, inner, "c")
	if err != nil || !ok || got.Name != "inner" {
		t.Fatalf(`Namespace("a:b") key "c": got=%+v ok=%v err=%v`, got, ok, err)
	}
}

func TestApplicationKeyCannotClobberATagMarker(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()

	if err := SetForever(ctx, c, "u1", user{Name: "ana"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	if err := c.DeleteByTag(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, c, "u1"); ok {
		t.Fatal("precondition: the tagged entry should already be invalidated")
	}

	if err := SetForever(ctx, c, "__gocache:tag:users", user{Name: "forged"}); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := Get[user](ctx, c, "u1"); ok {
		t.Fatal("an application key overwrote the tag marker and revived an invalidated entry")
	}
	got, ok, err := Get[user](ctx, c, "__gocache:tag:users")
	if err != nil || !ok || got.Name != "forged" {
		t.Fatalf("the application key must remain readable: got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestApplicationDeleteCannotReleaseALock(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	held := c.Lock("job", time.Minute)
	if ok, err := held.Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}

	if _, err := c.Delete(ctx, "__gocache:lock:job"); err != nil {
		t.Fatal(err)
	}

	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || ok {
		t.Fatalf("an application delete released a held lock: ok=%v err=%v", ok, err)
	}
}

func TestApplicationWriteCannotWedgeALock(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	held := c.Lock("job", time.Minute)
	if ok, err := held.Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}

	if err := SetForever(ctx, c, "__gocache:lock:job", user{Name: "forged"}); err != nil {
		t.Fatal(err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("an application key wedged the lock: ok=%v err=%v", ok, err)
	}
}

func TestRootClearLeavesForeignKeysAlone(t *testing.T) {
	l1 := memory.New()
	c, err := New(WithL1(l1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if err := l1.Set(ctx, "session:42", []byte(`{"sid":"42"}`), 0); err != nil {
		t.Fatal(err)
	}
	if err := SetForever(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}

	if err := c.Clear(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := l1.Get(ctx, "session:42"); err != nil || !ok {
		t.Fatalf("a root Clear deleted a key gocache never wrote: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := Get[user](ctx, c, "u"); ok {
		t.Fatal("a root Clear must still remove gocache's own data")
	}
}

func TestRootClearDoesNotReleaseLocks(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if err := c.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Lock("job", time.Minute).Acquire(ctx); err != nil || ok {
		t.Fatalf("a root Clear released a live lock: ok=%v err=%v", ok, err)
	}
}

func TestKeyPrefixIsolatesCachesSharingADriver(t *testing.T) {
	d := memory.New()
	a, err := New(WithL1(d), WithKeyPrefix("app-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, err := New(WithL1(d), WithKeyPrefix("app-b"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	if err := SetForever(ctx, a, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := Get[user](ctx, b, "u"); err != nil || ok {
		t.Fatalf("b read a's entry: ok=%v err=%v", ok, err)
	}

	if err := SetForever(ctx, b, "u", user{Name: "bob"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Clear(ctx); err != nil {
		t.Fatal(err)
	}

	got, ok, err := Get[user](ctx, a, "u")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("b's Clear reached a's entry: got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestNewRejectsAnEmptyKeyPrefix(t *testing.T) {
	for _, p := range []string{"", " ", "\t\n "} {
		if _, err := New(WithL1(memory.New()), WithKeyPrefix(p)); err == nil {
			t.Fatalf("expected an error for key prefix %q", p)
		}
	}
}

func TestSeparatorAndEscapeCharactersRoundTrip(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	cases := []struct {
		ns  string
		key string
	}{
		{"", `a:b`},
		{"a", `b`},
		{"", `a\:b`},
		{`a\`, `b`},
		{"", `a\\:b`},
		{`a\\`, `b`},
		{`a:b`, `c`},
		{"a", `b:c`},
		{"", `a:b:c`},
		{`x:`, `:y`},
		{`\`, `\`},
	}

	target := func(ns string) *Cache {
		if ns == "" {
			return c
		}
		return c.Namespace(ns)
	}

	for i, tc := range cases {
		if err := SetForever(ctx, target(tc.ns), tc.key, user{Name: fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatalf("case %d (ns=%q key=%q): %v", i, tc.ns, tc.key, err)
		}
	}
	for i, tc := range cases {
		want := fmt.Sprintf("v%d", i)
		got, ok, err := Get[user](ctx, target(tc.ns), tc.key)
		if err != nil || !ok || got.Name != want {
			t.Fatalf("case %d (ns=%q key=%q): got=%+v ok=%v err=%v, want %q", i, tc.ns, tc.key, got, ok, err, want)
		}
	}
}
