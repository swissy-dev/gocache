package memorybus

import (
	"context"
	"testing"
)

func TestPublishReachesAllLiveSubscribers(t *testing.T) {
	b := New()
	var got1, got2 []byte
	ctx := context.Background()
	if err := b.Subscribe(ctx, func(ctx context.Context, msg []byte) { got1 = msg }); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	if err := b.Subscribe(canceled, func(ctx context.Context, msg []byte) { got2 = msg }); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := b.Publish(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if string(got1) != "hello" {
		t.Fatalf("got1 = %q", got1)
	}
	if got2 != nil {
		t.Fatal("canceled subscriber must not receive")
	}
}
