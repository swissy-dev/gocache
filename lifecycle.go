package gocache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type runtime struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	trackMu   sync.RWMutex
	closeOnce sync.Once
	closed    atomic.Bool
	closeErr  error
	sf        singleflight.Group
	origin    string
	retryQ    chan []byte
	fence     invalidationFence
}

func (rt *runtime) track() bool {
	rt.trackMu.RLock()
	defer rt.trackMu.RUnlock()
	if rt.closed.Load() {
		return false
	}
	rt.wg.Add(1)
	return true
}

func newOrigin() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (c *Cache) Close() error {
	c.rt.closeOnce.Do(func() {
		c.rt.trackMu.Lock()
		c.rt.closed.Store(true)
		c.rt.trackMu.Unlock()
		c.rt.cancel()
		c.rt.wg.Wait()
		var errs []error
		if c.cfg.bus != nil {
			errs = append(errs, c.cfg.bus.Close())
		}
		if c.cfg.l2 != nil {
			errs = append(errs, c.cfg.l2.Close())
		}
		if c.cfg.l1 != nil {
			errs = append(errs, c.cfg.l1.Close())
		}
		c.rt.closeErr = errors.Join(errs...)
	})
	return c.rt.closeErr
}

func (c *Cache) handleBusMsg(ctx context.Context, msg []byte) {
	defer func() {
		if p := recover(); p != nil {
			c.logf("bus handler panic", "panic", p)
		}
	}()
	if c.rt.closed.Load() {
		return
	}
	var m busMsg
	if err := json.Unmarshal(msg, &m); err != nil {
		c.logf("bus message decode failed", "err", err)
		return
	}
	if m.Origin == c.rt.origin {
		return
	}
	c.emit(EventBusMessageReceived{Op: m.Op})
	if c.cfg.l1 == nil {
		return
	}
	switch m.Op {
	case "delete":
		c.rt.fence.invalidateMany(m.Keys)
		if err := c.cfg.l1.DeleteMany(ctx, m.Keys); err != nil {
			c.logf("bus delete failed", "err", err)
		}
	case "clear":
		c.rt.fence.invalidateEverything()
		if err := c.cfg.l1.ClearPrefix(ctx, m.Prefix); err != nil {
			c.logf("bus clear failed", "err", err)
		}
	case "tag":
		markerKey := c.tagKey(m.Tag)
		c.rt.fence.invalidate(markerKey)
		if _, err := c.cfg.l1.Delete(ctx, markerKey); err != nil {
			c.logf("bus tag evict failed", "err", err)
		}
	}
}

func (c *Cache) enqueueRetry(msg []byte) {
	for {
		select {
		case c.rt.retryQ <- msg:
			return
		default:
		}
		select {
		case <-c.rt.retryQ:
			c.emit(EventBusPublishFailed{Dropped: true})
			c.logf("bus retry queue full, dropped the oldest message")
		default:
		}
	}
}

func (c *Cache) drainRetries() {
	defer c.rt.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			c.logf("retry drainer panic", "panic", p)
		}
	}()
	delay := 100 * time.Millisecond
	for {
		var msg []byte
		select {
		case <-c.rt.ctx.Done():
			return
		case msg = <-c.rt.retryQ:
		}
		for {
			ctx, cancel := context.WithTimeout(c.rt.ctx, 5*time.Second)
			err := c.cfg.bus.Publish(ctx, msg)
			cancel()
			if err == nil {
				delay = 100 * time.Millisecond
				break
			}
			c.emit(EventBusPublishFailed{Err: err})
			c.logf("bus publish retry failed", "err", err, "retry_in", delay)
			delay = min(delay*2, 5*time.Second)
			t := time.NewTimer(delay)
			select {
			case <-c.rt.ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}
