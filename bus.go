package gocache

import "context"

type Bus interface {
	Publish(ctx context.Context, msg []byte) error
	Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error
	Close() error
}
