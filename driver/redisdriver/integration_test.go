//go:build integration

package redisdriver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/drivertest"
)

func TestRedisConformance(t *testing.T) {
	addr := os.Getenv("GOCACHE_TEST_REDIS")
	if addr == "" {
		t.Skip("GOCACHE_TEST_REDIS is not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			if err := client.FlushDB(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			return New(client)
		},
	})
}

func newClusterClient(t *testing.T) *redis.ClusterClient {
	t.Helper()
	addrs := os.Getenv("GOCACHE_TEST_REDIS_CLUSTER")
	if addrs == "" {
		t.Skip("GOCACHE_TEST_REDIS_CLUSTER is not set")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func flushCluster(t *testing.T, client *redis.ClusterClient) {
	t.Helper()
	err := client.ForEachMaster(context.Background(), func(ctx context.Context, node *redis.Client) error {
		return node.FlushDB(ctx).Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mastersHoldingKeys(t *testing.T, client *redis.ClusterClient) int {
	t.Helper()
	var loaded atomic.Int64
	err := client.ForEachMaster(context.Background(), func(ctx context.Context, node *redis.Client) error {
		n, err := node.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		if n > 0 {
			loaded.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return int(loaded.Load())
}

func writeClusterKeys(t *testing.T, d *Driver, prefix string, n int) []string {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
		if err := d.Set(ctx, keys[i], []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestRedisClusterConformance(t *testing.T) {
	client := newClusterClient(t)
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			flushCluster(t, client)
			return New(client)
		},
	})
}

func TestRedisClusterDeleteManySpansSlots(t *testing.T) {
	client := newClusterClient(t)
	flushCluster(t, client)
	d := New(client)
	ctx := context.Background()
	keys := writeClusterKeys(t, d, "delete:", 400)

	if err := d.DeleteMany(ctx, keys); err != nil {
		t.Fatalf("delete many across slots: %v", err)
	}
	for _, key := range keys {
		if _, ok, err := d.Get(ctx, key); err != nil || ok {
			t.Fatalf("%s survived: ok=%v err=%v", key, ok, err)
		}
	}
}

func TestRedisClusterClearPrefixSpansMasters(t *testing.T) {
	client := newClusterClient(t)
	flushCluster(t, client)
	d := New(client)
	ctx := context.Background()
	keys := writeClusterKeys(t, d, "clear:", 400)
	if loaded := mastersHoldingKeys(t, client); loaded < 2 {
		t.Skipf("the cluster spread 400 keys over %d master(s), so a single-node clear would pass", loaded)
	}
	if err := d.Set(ctx, "keep:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	if err := d.ClearPrefix(ctx, "clear:"); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, ok, err := d.Get(ctx, key); err != nil || ok {
			t.Fatalf("%s survived the clear: ok=%v err=%v", key, ok, err)
		}
	}
	if _, ok, _ := d.Get(ctx, "keep:1"); !ok {
		t.Fatal("keep:1 should remain")
	}
}
