package redisdriver

import (
	"context"
	"fmt"
	"sync"
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
			t.Helper()
			server = miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			return New(client)
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			server.FastForward(d)
		},
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

func newRing(t *testing.T, shards int) (*redis.Ring, []*miniredis.Miniredis) {
	t.Helper()
	servers := make([]*miniredis.Miniredis, shards)
	addrs := make(map[string]string, shards)
	for i := range servers {
		servers[i] = miniredis.RunT(t)
		addrs[fmt.Sprintf("shard%d", i)] = servers[i].Addr()
	}
	client := redis.NewRing(&redis.RingOptions{Addrs: addrs})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, servers
}

func TestRingConformance(t *testing.T) {
	var servers []*miniredis.Miniredis
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			t.Helper()
			var client *redis.Ring
			client, servers = newRing(t, 3)
			return New(client)
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			for _, server := range servers {
				server.FastForward(d)
			}
		},
	})
}

func spread(t *testing.T, d *Driver, prefix string, n int) []string {
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

func assertSpanned(t *testing.T, servers []*miniredis.Miniredis) {
	t.Helper()
	var loaded int
	for _, server := range servers {
		if len(server.Keys()) > 0 {
			loaded++
		}
	}
	if loaded < 2 {
		t.Fatalf("keys landed on %d shard(s), the test proves nothing", loaded)
	}
}

func TestDeleteManySpansRingShards(t *testing.T) {
	client, servers := newRing(t, 3)
	d := New(client)
	ctx := context.Background()
	keys := spread(t, d, "delete:", 300)
	assertSpanned(t, servers)

	if err := d.DeleteMany(ctx, keys); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, ok, err := d.Get(ctx, key); err != nil || ok {
			t.Fatalf("%s survived: ok=%v err=%v", key, ok, err)
		}
	}
}

func TestClearPrefixSpansRingShards(t *testing.T) {
	client, servers := newRing(t, 3)
	d := New(client)
	ctx := context.Background()
	keys := spread(t, d, "clear:", 300)
	assertSpanned(t, servers)
	if err := d.Set(ctx, "keep:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	if err := d.ClearPrefix(ctx, "clear:"); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, ok, err := d.Get(ctx, key); err != nil || ok {
			t.Fatalf("%s survived: ok=%v err=%v", key, ok, err)
		}
	}
	if _, ok, _ := d.Get(ctx, "keep:1"); !ok {
		t.Fatal("keep:1 should remain")
	}
}

type recorder struct {
	mu   sync.Mutex
	cmds []string
}

func (r *recorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *recorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r.add(cmd)
		return next(ctx, cmd)
	}
}

func (r *recorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			r.add(cmd)
		}
		return next(ctx, cmds)
	}
}

func (r *recorder) add(cmd redis.Cmder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, fmt.Sprint(cmd.Args()...))
}

func (r *recorder) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	for _, cmd := range r.cmds {
		if len(cmd) >= len(name) && cmd[:len(name)] == name {
			n++
		}
	}
	return n
}

func TestDeleteManyBatchesOnASingleNode(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	rec := &recorder{}
	client.AddHook(rec)
	d := New(client)

	if err := d.DeleteMany(context.Background(), []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	if got := rec.count("del"); got != 1 {
		t.Fatalf("want one batched DEL, got %d commands", got)
	}
}
