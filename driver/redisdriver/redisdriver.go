package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	scanCount   = 500
	deleteBatch = 500
)

var deleteIfEquals = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

type Driver struct {
	client redis.UniversalClient
}

func New(client redis.UniversalClient) *Driver {
	return &Driver{client: client}
}

func (d *Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := d.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redisdriver: get: %w", err)
	}
	return v, true, nil
}

func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := d.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redisdriver: set: %w", err)
	}
	return nil
}

func (d *Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	ok, err := d.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: add: %w", err)
	}
	return ok, nil
}

func (d *Driver) Delete(ctx context.Context, key string) (bool, error) {
	n, err := d.client.Del(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: delete: %w", err)
	}
	return n > 0, nil
}

func (d *Driver) DeleteMany(ctx context.Context, keys []string) error {
	for chunk := range slices.Chunk(keys, deleteBatch) {
		if err := d.client.Del(ctx, chunk...).Err(); err != nil {
			return fmt.Errorf("redisdriver: delete many: %w", err)
		}
	}
	return nil
}

func (d *Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	n, err := deleteIfEquals.Run(ctx, d.client, []string{key}, value).Int64()
	if err != nil {
		return false, fmt.Errorf("redisdriver: delete if equals: %w", err)
	}
	return n > 0, nil
}

func (d *Driver) ClearPrefix(ctx context.Context, prefix string) error {
	match := escapeGlob(prefix) + "*"
	var cursor uint64
	for {
		keys, next, err := d.client.Scan(ctx, cursor, match, scanCount).Result()
		if err != nil {
			return fmt.Errorf("redisdriver: scan: %w", err)
		}
		if err := d.DeleteMany(ctx, keys); err != nil {
			return err
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func escapeGlob(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '^', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (d *Driver) Close() error {
	return nil
}
