package gocache

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swissy-dev/gocache/driver/memory"
)

type panicWatcher struct {
	saw atomic.Bool
}

func (w *panicWatcher) Enabled(context.Context, slog.Level) bool { return true }

func (w *panicWatcher) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "bus handler panic") {
		w.saw.Store(true)
	}
	return nil
}

func (w *panicWatcher) WithAttrs([]slog.Attr) slog.Handler { return w }

func (w *panicWatcher) WithGroup(string) slog.Handler { return w }

func namespaceChain(names ...string) []string {
	chain := make([]string, 0, len(names))
	for _, n := range names {
		if n != "" {
			chain = append(chain, n)
		}
	}
	return chain
}

func FuzzKeyInjectivity(f *testing.F) {
	f.Add("a", "b", "k", "a:b", "", "k")
	f.Add("a", "", "b:c", "a:b", "", "c")
	f.Add("", "", "a", "a", "", "")
	f.Add(`a\`, "", "b", "a", "", `\b`)
	f.Add("a:", "", "", "a", "", ":")
	f.Add("a", "", "", "", "", "a:")

	base, err := New(WithL1(memory.New()))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = base.Close() })

	f.Fuzz(func(t *testing.T, ns1a, ns1b, key1, ns2a, ns2b, key2 string) {
		c1 := base.Namespace(ns1a).Namespace(ns1b)
		c2 := base.Namespace(ns2a).Namespace(ns2b)

		sameInput := slices.Equal(namespaceChain(ns1a, ns1b), namespaceChain(ns2a, ns2b)) && key1 == key2
		got1, got2 := c1.key(key1), c2.key(key2)

		if sameInput && got1 != got2 {
			t.Fatalf("same input produced different keys:\n  ns=%q/%q key=%q -> %q\n  ns=%q/%q key=%q -> %q",
				ns1a, ns1b, key1, got1, ns2a, ns2b, key2, got2)
		}
		if !sameInput && got1 == got2 {
			t.Fatalf("distinct inputs collided on %q:\n  ns=%q/%q key=%q\n  ns=%q/%q key=%q",
				got1, ns1a, ns1b, key1, ns2a, ns2b, key2)
		}
		if got1 == c1.tagKey(key1) || got1 == c1.lockKey(key1) {
			t.Fatalf("data key %q collided with another domain for ns=%q/%q key=%q", got1, ns1a, ns1b, key1)
		}
		if c1.tagKey(key1) == c1.lockKey(key1) {
			t.Fatalf("tag and lock keys collided for key %q", key1)
		}
	})
}

func FuzzHandleBusMsg(f *testing.F) {
	f.Add([]byte(`{"o":"other","op":"delete","k":["a","b"]}`))
	f.Add([]byte(`{"o":"other","op":"clear","p":"users:"}`))
	f.Add([]byte(`{"o":"other","op":"tag","t":"users"}`))
	f.Add([]byte(`{"op":"delete","k":null}`))
	f.Add([]byte(`{"op":"unknown"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))

	watcher := &panicWatcher{}
	c, err := New(WithL1(memory.New()), WithLogger(slog.New(watcher)))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = c.Close() })

	f.Fuzz(func(t *testing.T, msg []byte) {
		watcher.saw.Store(false)
		c.handleBusMsg(context.Background(), msg)
		if watcher.saw.Load() {
			t.Fatalf("handleBusMsg panicked on %q", msg)
		}
	})
}

func FuzzDecodeEnvelope(f *testing.F) {
	f.Add([]byte(`{"v":{"name":"ana"},"c":1700000000000,"x":1700000060000,"t":["users"]}`))
	f.Add([]byte(`{"v":null,"c":0,"x":0}`))
	f.Add([]byte(`{"x":-1}`))
	f.Add([]byte(`{"t":[]}`))
	f.Add([]byte(`{"x":9223372036854775807}`))
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		env, err := decodeEnvelope(data)
		if err != nil {
			return
		}

		now := time.Now()
		fresh := env.isFresh(now)

		if _, err := env.encode(); err != nil {
			t.Fatalf("re-encoding a decoded envelope failed: %v", err)
		}

		ttl := physicalTTL(env, now, time.Minute)
		if env.ExpiresAt != 0 && fresh && ttl <= 0 {
			t.Fatalf("envelope is fresh but physical ttl is %v (expiresAt=%d)", ttl, env.ExpiresAt)
		}
	})
}
