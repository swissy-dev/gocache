package redisbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

func TestPublishReachesSubscriber(t *testing.T) {
	server := miniredis.RunT(t)
	pubClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	subClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = pubClient.Close() }()
	defer func() { _ = subClient.Close() }()

	pub := New(pubClient)
	sub := New(subClient)
	ctx := t.Context()

	got := make(chan []byte, 1)
	if err := sub.Subscribe(ctx, func(ctx context.Context, msg []byte) { got <- msg }); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if string(m) != "hello" {
			t.Fatalf("got %q", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseStopsSubscriberGoroutine(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	ignore := goleak.IgnoreCurrent()

	bus := New(client, WithChannel("gocache:test"))
	if err := bus.Subscribe(context.Background(), func(context.Context, []byte) {}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	goleak.VerifyNone(t, ignore)
}

func TestCloseWaitsForInFlightHandler(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	bus := New(client, WithChannel("gocache:inflight"))
	started := make(chan struct{})
	release := make(chan struct{})
	if err := bus.Subscribe(context.Background(), func(context.Context, []byte) {
		close(started)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}

	closed := make(chan struct{})
	go func() {
		_ = bus.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a handler was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the handler finished")
	}
}

func TestSubscribeAfterCloseReturnsErrClosed(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	ignore := goleak.IgnoreCurrent()

	bus := New(client, WithChannel("gocache:test-closed"))
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	err := bus.Subscribe(context.Background(), func(context.Context, []byte) {})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close returned %v, want ErrClosed", err)
	}
	goleak.VerifyNone(t, ignore)
}
