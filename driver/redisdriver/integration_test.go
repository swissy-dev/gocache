//go:build integration

package redisdriver

import (
	"context"
	"os"
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
