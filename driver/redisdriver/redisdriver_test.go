package redisdriver

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/drivertest"
)

func TestConformance(t *testing.T) {
	var server *miniredis.Miniredis
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			server = miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			return New(client)
		},
		Advance: func(t *testing.T, d time.Duration) { server.FastForward(d) },
	})
}

func TestClearPrefixEscapesGlobMetacharacters(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	d := New(client)
	ctx := context.Background()
	if err := d.Set(ctx, "a*b:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "axb:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.ClearPrefix(ctx, "a*b:"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(ctx, "a*b:1"); ok {
		t.Fatal("literal prefix not cleared")
	}
	if _, ok, _ := d.Get(ctx, "axb:1"); !ok {
		t.Fatal("glob leaked into the match pattern")
	}
}
