// Package redisbus provides a Redis pub/sub invalidation bus.
//
// It is what keeps in-process L1 tiers coherent across instances: after a write
// each instance broadcasts the affected keys, and its peers drop their local
// copies. The same redis.UniversalClient can back both this and
// [github.com/swissy-dev/gocache/driver/redisdriver].
//
// Redis pub/sub is fire-and-forget, so a message published while an instance is
// disconnected is not replayed. A missed invalidation costs staleness bounded
// by the entry's TTL, which is the trade the cache is built around.
package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

const defaultChannel = "gocache:bus"

// ErrClosed is returned by [Bus.Subscribe] after the bus has been closed.
var ErrClosed = errors.New("redisbus: bus closed")

// Option configures a [Bus].
type Option func(*Bus)

// WithChannel sets the pub/sub channel, defaulting to "gocache:bus". Give
// unrelated applications sharing one Redis different channels so they do not
// process each other's invalidations.
func WithChannel(name string) Option {
	return func(b *Bus) { b.channel = name }
}

// Bus broadcasts invalidations over Redis pub/sub. Use [New] to create one. It
// is safe for concurrent use.
type Bus struct {
	client   redis.UniversalClient
	channel  string
	mu       sync.Mutex
	subs     []*redis.PubSub
	wg       sync.WaitGroup
	once     sync.Once
	isClosed bool
}

// New returns a bus publishing on the given client.
//
// The bus does not own the client: [Bus.Close] stops delivery but leaves the
// client open, so it can be shared with a driver.
func New(client redis.UniversalClient, opts ...Option) *Bus {
	b := &Bus{client: client, channel: defaultChannel}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Publish implements gocache.Bus. It does not wait for subscribers, so a
// message sent while a peer is disconnected is simply lost.
func (b *Bus) Publish(ctx context.Context, msg []byte) error {
	if err := b.client.Publish(ctx, b.channel, msg).Err(); err != nil {
		return fmt.Errorf("redisbus: publish: %w", err)
	}
	return nil
}

// Subscribe implements gocache.Bus, delivering every message on the channel
// to handler until ctx is cancelled or the bus is closed. It returns
// [ErrClosed] if the bus is already closed.
//
// Messages are delivered from a single goroutine per subscription, so a slow
// handler delays the ones behind it.
func (b *Bus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	pubsub := b.client.Subscribe(ctx, b.channel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return fmt.Errorf("redisbus: subscribe: %w", err)
	}
	b.mu.Lock()
	if b.isClosed {
		b.mu.Unlock()
		_ = pubsub.Close()
		return ErrClosed
	}
	b.subs = append(b.subs, pubsub)
	b.wg.Add(1)
	b.mu.Unlock()
	go func() {
		defer b.wg.Done()
		defer func() { _ = pubsub.Close() }()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				handler(ctx, []byte(m.Payload))
			}
		}
	}()
	return nil
}

// Close implements gocache.Bus. It closes every subscription and waits for
// their delivery goroutines to finish, so no handler runs after Close returns.
//
// Because it waits, a handler that calls Close on its own bus deadlocks. Close
// from outside the handler. The underlying client is left open.
func (b *Bus) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.isClosed = true
		subs := b.subs
		b.subs = nil
		b.mu.Unlock()
		for _, s := range subs {
			_ = s.Close()
		}
		b.wg.Wait()
	})
	return nil
}
