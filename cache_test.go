package gocache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

func TestNewRequiresATier(t *testing.T) {
	_, err := New()
	if err == nil {
		t.Fatal("expected error with no tiers")
	}
}

func TestNewRejectsBusWithoutBothTiers(t *testing.T) {
	_, err := New(WithL1(memory.New()), WithBus(nopBus{}))
	if err == nil {
		t.Fatal("expected error for bus without L2")
	}
}

func TestNewRejectsNilDriver(t *testing.T) {
	_, err := New(WithL1(nil))
	if err == nil {
		t.Fatal("expected error for nil driver")
	}
}

func TestNewRejectsNegativeDurations(t *testing.T) {
	_, err := New(WithL1(memory.New()), WithDefaultTTL(-time.Second))
	if err == nil {
		t.Fatal("expected error for negative ttl")
	}
}

func TestNewRejectsNegativeHardTimeout(t *testing.T) {
	if _, err := New(WithL1(memory.New()), WithHardTimeout(-time.Second)); err == nil {
		t.Fatal("expected error for negative hard timeout")
	}
}

func TestNewRejectsNonPositiveMaxConcurrentFactories(t *testing.T) {
	if _, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(0)); err == nil {
		t.Fatal("expected error for a zero factory limit")
	}
	if _, err := New(WithL1(memory.New()), WithMaxConcurrentFactories(-1)); err == nil {
		t.Fatal("expected error for a negative factory limit")
	}
}

func TestNewRejectsZeroTagCacheTTL(t *testing.T) {
	if _, err := New(WithL1(memory.New()), WithTagCacheTTL(0)); err == nil {
		t.Fatal("expected error for zero tag cache ttl")
	}
	if _, err := New(WithL1(memory.New()), WithTagCacheTTL(-time.Second)); err == nil {
		t.Fatal("expected error for negative tag cache ttl")
	}
}

type nonJSONCodec struct{}

func (nonJSONCodec) Marshal(v any) ([]byte, error) { return []byte{0x01, 0x02, 0x03}, nil }

func (nonJSONCodec) Unmarshal(data []byte, v any) error { return nil }

func TestNewRejectsCodecThatDoesNotProduceJSON(t *testing.T) {
	_, err := New(WithL1(memory.New()), WithCodec(nonJSONCodec{}))
	if err == nil {
		t.Fatal("expected error for a codec that emits non-json bytes")
	}
}

type stringJSONCodec struct{}

func (stringJSONCodec) Marshal(v any) ([]byte, error) {
	inner, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(inner))
}

func (stringJSONCodec) Unmarshal(data []byte, v any) error {
	var inner string
	if err := json.Unmarshal(data, &inner); err != nil {
		return err
	}
	return json.Unmarshal([]byte(inner), v)
}

func TestAlternativeJSONCodecRoundTrips(t *testing.T) {
	c, err := New(WithL1(memory.New()), WithCodec(stringJSONCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	if err := Set(ctx, c, "u", user{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Get[user](ctx, c, "u")
	if err != nil || !ok || got.Name != "ana" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestNamespacePrefixes(t *testing.T) {
	c, err := New(WithL1(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ns := c.Namespace("users").Namespace("posts")
	if got, want := ns.ns, "users:posts"; got != want {
		t.Fatalf("namespace path = %q, want %q", got, want)
	}
	if got, want := ns.key("1"), ns.scopedPrefix(domainData)+"1"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

func TestCloseIdempotent(t *testing.T) {
	c, err := New(WithL1(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Has(t.Context(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v", err)
	}
}

type nopBus struct{}

func (nopBus) Publish(ctx context.Context, msg []byte) error { return nil }
func (nopBus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	return nil
}
func (nopBus) Close() error { return nil }
