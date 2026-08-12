// Package memorybus provides an in-process invalidation bus.
//
// It connects caches inside a single process, which makes it useful in tests
// and in single-binary deployments where several caches share one L2. It cannot
// reach another process — for that use
// [github.com/swissy-dev/gocache/bus/redisbus].
//
// Delivery is synchronous: publishing runs every subscriber's handler on the
// calling goroutine before returning.
package memorybus

import (
	"context"
	"slices"
	"sync"
)

type subscriber struct {
	ctx     context.Context
	handler func(ctx context.Context, msg []byte)
}

// Bus delivers invalidations to subscribers in the same process. Use [New] to
// create one. It is safe for concurrent use.
type Bus struct {
	mu   sync.Mutex
	subs []subscriber
}

// New returns an in-process bus with no subscribers.
func New() *Bus {
	return &Bus{}
}

// Publish implements gocache.Bus. Handlers run synchronously on the calling
// goroutine, so publishing costs as much as the slowest subscriber, and a
// handler that blocks will hold up the write that triggered it.
//
// Each subscriber receives its own copy of the message. Subscribers whose
// context has ended are skipped.
func (b *Bus) Publish(ctx context.Context, msg []byte) error {
	b.mu.Lock()
	subs := slices.Clone(b.subs)
	b.mu.Unlock()
	for _, s := range subs {
		if s.ctx.Err() != nil {
			continue
		}
		m := make([]byte, len(msg))
		copy(m, msg)
		s.handler(s.ctx, m)
	}
	return nil
}

// Subscribe implements gocache.Bus, registering handler until ctx ends.
func (b *Bus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, subscriber{ctx: ctx, handler: handler})
	return nil
}

// Close implements gocache.Bus, dropping every subscriber. It always returns
// nil.
func (b *Bus) Close() error {
	return nil
}
