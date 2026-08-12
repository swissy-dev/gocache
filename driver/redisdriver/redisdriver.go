// Package redisdriver provides a Redis-backed cache driver.
//
// It is the usual choice for the L2 tier, where several instances share one
// store. Expiry is delegated to Redis, so entries disappear without the driver
// sweeping them.
//
// Any redis.UniversalClient works, including a single node, a Cluster and a
// Ring. Sharded clients are detected and handled: multi-key deletes are
// regrouped per shard rather than issued as one cross-slot command, and prefix
// clears fan out to every master instead of only the one the client happens to
// reach.
//
// See [github.com/swissy-dev/gocache/bus/redisbus] to reuse the same client for
// cross-instance invalidation.
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

type clusterClient interface {
	ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
}

type ringClient interface {
	ForEachShard(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
}

// Driver stores cache entries in Redis. Use [New] to create one. It is safe for
// concurrent use, and holds no state beyond the client it was given.
type Driver struct {
	client redis.UniversalClient
}

// New returns a driver backed by the given client.
//
// The driver does not own the client: [Driver.Close] leaves it open, so one
// client can back both a driver and a bus. Close it yourself when done.
func New(client redis.UniversalClient) *Driver {
	return &Driver{client: client}
}

// Get implements gocache.Reader. A missing key is a miss rather than an
// error, and Redis has already dropped anything past its TTL.
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

// Set implements gocache.Writer. A ttl of zero or less stores the entry with
// no expiry.
func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := d.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redisdriver: set: %w", err)
	}
	return nil
}

// Add implements gocache.Atomic using SET NX, so exactly one caller wins a
// race. This is what makes gocache.Cache.Lock safe across processes.
func (d *Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	ok, err := d.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: add: %w", err)
	}
	return ok, nil
}

// Delete implements gocache.Writer, reporting whether the key was present.
func (d *Driver) Delete(ctx context.Context, key string) (bool, error) {
	n, err := d.client.Del(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redisdriver: delete: %w", err)
	}
	return n > 0, nil
}

// DeleteMany implements gocache.Writer. Against a Cluster or Ring the keys
// are regrouped per shard, since one DEL cannot span hash slots. Deletes are
// batched, so a partial failure may leave some keys removed.
func (d *Driver) DeleteMany(ctx context.Context, keys []string) error {
	if err := d.deleteMany(ctx, keys); err != nil {
		return fmt.Errorf("redisdriver: delete many: %w", err)
	}
	return nil
}

func (d *Driver) deleteMany(ctx context.Context, keys []string) error {
	sharded := d.isSharded()
	for chunk := range slices.Chunk(keys, deleteBatch) {
		if sharded {
			if _, err := d.client.Pipelined(ctx, func(p redis.Pipeliner) error {
				for _, key := range chunk {
					p.Del(ctx, key)
				}
				return nil
			}); err != nil {
				return err
			}
			continue
		}
		if err := d.client.Del(ctx, chunk...).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) isSharded() bool {
	switch d.client.(type) {
	case clusterClient, ringClient:
		return true
	default:
		return false
	}
}

// DeleteIfEquals implements gocache.Atomic. The compare and delete run in a
// Lua script so they are atomic against a concurrent expiry or overwrite.
func (d *Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	n, err := deleteIfEquals.Run(ctx, d.client, []string{key}, value).Int64()
	if err != nil {
		return false, fmt.Errorf("redisdriver: delete if equals: %w", err)
	}
	return n > 0, nil
}

// ClearPrefix implements gocache.Writer. It scans and deletes in batches
// rather than blocking Redis with KEYS, so it is not atomic: entries written
// while it runs may survive. Against a Cluster or Ring it visits every master.
//
// The prefix is matched literally; glob metacharacters in it are escaped.
func (d *Driver) ClearPrefix(ctx context.Context, prefix string) error {
	match := escapeGlob(prefix) + "*"
	sweep := func(ctx context.Context, node *redis.Client) error {
		return d.clearNode(ctx, node, match)
	}
	switch c := d.client.(type) {
	case clusterClient:
		if err := c.ForEachMaster(ctx, sweep); err != nil {
			return fmt.Errorf("redisdriver: cluster clear prefix: %w", err)
		}
	case ringClient:
		if err := c.ForEachShard(ctx, sweep); err != nil {
			return fmt.Errorf("redisdriver: ring clear prefix: %w", err)
		}
	default:
		if err := d.clearNode(ctx, d.client, match); err != nil {
			return fmt.Errorf("redisdriver: clear prefix: %w", err)
		}
	}
	return nil
}

func (d *Driver) clearNode(ctx context.Context, node redis.Cmdable, match string) error {
	var cursor uint64
	for {
		keys, next, err := node.Scan(ctx, cursor, match, scanCount).Result()
		if err != nil {
			return err
		}
		if err := d.deleteMany(ctx, keys); err != nil {
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

// Close implements io.Closer. It does not close the underlying client, which
// remains the caller's to manage.
func (d *Driver) Close() error {
	return nil
}
