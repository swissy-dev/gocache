package null

import (
	"context"
	"time"
)

type Driver struct{}

func New() *Driver {
	return &Driver{}
}

func (*Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return nil, false, nil
}

func (*Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}

func (*Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return true, nil
}

func (*Driver) Delete(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (*Driver) DeleteMany(ctx context.Context, keys []string) error {
	return nil
}

func (*Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	return false, nil
}

func (*Driver) ClearPrefix(ctx context.Context, prefix string) error {
	return nil
}

func (*Driver) Close() error {
	return nil
}
