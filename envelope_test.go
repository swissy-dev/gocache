package gocache

import (
	"testing"
	"time"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	e := newEnvelope([]byte(`{"a":1}`), now, time.Minute, []string{"users"})
	data, err := e.encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != `{"a":1}` {
		t.Fatalf("value = %s", got.Value)
	}
	if got.CreatedAt != now.UnixMilli() {
		t.Fatalf("createdAt = %d", got.CreatedAt)
	}
	if got.ExpiresAt != now.Add(time.Minute).UnixMilli() {
		t.Fatalf("expiresAt = %d", got.ExpiresAt)
	}
	if len(got.Tags) != 1 || got.Tags[0] != "users" {
		t.Fatalf("tags = %v", got.Tags)
	}
}

func TestEnvelopeFreshness(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	e := newEnvelope([]byte(`1`), now, time.Minute, nil)
	if !e.isFresh(now.Add(59 * time.Second)) {
		t.Fatal("expected fresh before expiry")
	}
	if e.isFresh(now.Add(61 * time.Second)) {
		t.Fatal("expected stale after expiry")
	}
}

func TestEnvelopeForever(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	e := newEnvelope([]byte(`1`), now, 0, nil)
	if e.ExpiresAt != 0 {
		t.Fatalf("expiresAt = %d", e.ExpiresAt)
	}
	if !e.isFresh(now.Add(1000 * time.Hour)) {
		t.Fatal("forever entry must always be fresh")
	}
}
