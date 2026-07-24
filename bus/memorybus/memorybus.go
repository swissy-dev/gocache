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

type Bus struct {
	mu   sync.Mutex
	subs []subscriber
}

func New() *Bus {
	return &Bus{}
}

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

func (b *Bus) Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, subscriber{ctx: ctx, handler: handler})
	return nil
}

func (b *Bus) Close() error {
	return nil
}
