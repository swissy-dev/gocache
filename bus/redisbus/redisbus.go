package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

const defaultChannel = "gocache:bus"

var ErrClosed = errors.New("redisbus: bus closed")

type Option func(*Bus)

func WithChannel(name string) Option {
	return func(b *Bus) { b.channel = name }
}

type Bus struct {
	client   redis.UniversalClient
	channel  string
	mu       sync.Mutex
	subs     []*redis.PubSub
	wg       sync.WaitGroup
	once     sync.Once
	isClosed bool
}

func New(client redis.UniversalClient, opts ...Option) *Bus {
	b := &Bus{client: client, channel: defaultChannel}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Bus) Publish(ctx context.Context, msg []byte) error {
	if err := b.client.Publish(ctx, b.channel, msg).Err(); err != nil {
		return fmt.Errorf("redisbus: publish: %w", err)
	}
	return nil
}

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
