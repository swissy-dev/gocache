package gocache

import (
	"context"
	"io"
	"time"
)

type Reader interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
}

type Writer interface {
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) (bool, error)
	DeleteMany(ctx context.Context, keys []string) error
	ClearPrefix(ctx context.Context, prefix string) error
}

type Atomic interface {
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
	DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error)
}

type Driver interface {
	Reader
	Writer
	Atomic
	io.Closer
}
