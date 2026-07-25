package gocache

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

func TestDeleteByTagInvalidates(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "u1", user{Name: "ana"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if err := Set(ctx, c, "p1", user{Name: "post"}, WithTags("posts")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	if err := c.DeleteByTag(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, c, "u1"); ok {
		t.Fatal("tagged entry should be invalidated")
	}
	if _, ok, _ := Get[user](ctx, c, "p1"); !ok {
		t.Fatal("other tag should survive")
	}
}

func TestEntryWrittenAfterTagInvalidationSurvives(t *testing.T) {
	c, clk := newTestCache(t)
	ctx := context.Background()
	if err := c.DeleteByTag(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	if err := Set(ctx, c, "u1", user{Name: "ana"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, c, "u1"); !ok {
		t.Fatal("entry created after invalidation must survive")
	}
}

func TestSameMillisecondInvalidation(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	if err := Set(ctx, c, "u1", user{Name: "ana"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteByTag(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, c, "u1"); ok {
		t.Fatal("same-ms invalidation must win")
	}
}

func TestUnparseableTagTimestampDoesNotFailReads(t *testing.T) {
	l1 := memory.New()
	var logs bytes.Buffer
	clk := newFakeClock()
	c, err := New(WithL1(l1), WithClock(clk.Now), WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if err := Set(ctx, c, "u1", user{Name: "ana"}, WithTags("users")); err != nil {
		t.Fatal(err)
	}
	if err := l1.Set(ctx, c.tagKey("users"), []byte("not-a-timestamp"), 0); err != nil {
		t.Fatal(err)
	}

	got, ok, err := Get[user](ctx, c, "u1")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("a poisoned tag key must not fail reads: got=%+v ok=%v err=%v", got, ok, err)
	}
	if !strings.Contains(logs.String(), "tag timestamp") {
		t.Fatalf("expected the unparseable tag timestamp to be logged, got %q", logs.String())
	}
}

func TestTagSpansNamespaces(t *testing.T) {
	l1 := memory.New()
	l2 := memory.New()
	clk := newFakeClock()
	c, err := New(WithL1(l1), WithL2(l2), WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	users := c.Namespace("users")
	posts := c.Namespace("posts")
	if err := Set(ctx, users, "1", user{Name: "a"}, WithTags("hot")); err != nil {
		t.Fatal(err)
	}
	if err := Set(ctx, posts, "1", user{Name: "b"}, WithTags("hot")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	if err := c.DeleteByTag(ctx, "hot"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := Get[user](ctx, users, "1"); ok {
		t.Fatal("users:1 should be invalidated")
	}
	if _, ok, _ := Get[user](ctx, posts, "1"); ok {
		t.Fatal("posts:1 should be invalidated")
	}
}
