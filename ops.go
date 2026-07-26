package gocache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

type readResult struct {
	env     envelope
	isFound bool
	isFresh bool
	tier    Tier
}

func (c *Cache) readTier(ctx context.Context, d Driver, fullKey string) (envelope, bool, error) {
	raw, ok, err := d.Get(ctx, fullKey)
	if err != nil || !ok {
		return envelope{}, false, err
	}
	env, err := decodeEnvelope(raw)
	if err != nil {
		return envelope{}, false, fmt.Errorf("gocache: decode envelope: %w", err)
	}
	valid, err := c.tagsValid(ctx, env)
	if err != nil {
		return envelope{}, false, err
	}
	if !valid {
		return envelope{}, false, nil
	}
	return env, true, nil
}

func (c *Cache) read(ctx context.Context, fullKey string) (readResult, error) {
	now := c.cfg.clock()
	var staleL1 readResult
	if c.cfg.l1 != nil {
		env, ok, err := c.readTier(ctx, c.cfg.l1, fullKey)
		if err != nil {
			return readResult{}, err
		}
		if ok && env.isFresh(now) {
			return readResult{env: env, isFound: true, isFresh: true, tier: TierL1}, nil
		}
		if c.cfg.l2 == nil {
			return readResult{env: env, isFound: ok, tier: TierL1}, nil
		}
		if ok {
			staleL1 = readResult{env: env, isFound: true, tier: TierL1}
		}
	}
	if c.cfg.l2 != nil {
		gen := c.rt.fence.generation(fullKey)
		env, ok, err := c.readTier(ctx, c.cfg.l2, fullKey)
		if err != nil {
			return readResult{}, err
		}
		if !ok {
			return staleL1, nil
		}
		if env.isFresh(now) {
			c.backfillL1(ctx, fullKey, env, now, gen)
			return readResult{env: env, isFound: true, isFresh: true, tier: TierL2}, nil
		}
		return readResult{env: env, isFound: true, tier: TierL2}, nil
	}
	return readResult{}, nil
}

func (c *Cache) backfillL1(ctx context.Context, fullKey string, env envelope, now time.Time, gen uint64) {
	if c.cfg.l1 == nil {
		return
	}
	raw, err := env.encode()
	if err != nil {
		c.logf("l1 backfill encode failed", "key", fullKey, "err", err)
		return
	}
	ttl := physicalTTL(env, now, c.cfg.grace)
	if ttl < 0 {
		return
	}
	if err := c.fencedL1Set(ctx, fullKey, raw, ttl, gen); err != nil {
		c.logf("l1 backfill failed", "key", fullKey, "err", err)
	}
}

func physicalTTL(env envelope, now time.Time, grace time.Duration) time.Duration {
	if env.ExpiresAt == 0 {
		return 0
	}
	remaining := time.UnixMilli(env.ExpiresAt).Sub(now)
	return remaining + grace
}

func (c *Cache) writeEnvelope(ctx context.Context, fullKey string, env envelope, o callOpts) error {
	if c.rt.isClosed.Load() {
		return ErrClosed
	}
	raw, err := env.encode()
	if err != nil {
		return fmt.Errorf("gocache: encode envelope: %w", err)
	}
	c.rt.fence.invalidate(fullKey)
	ttl := physicalTTL(env, c.cfg.clock(), o.grace)
	if env.ExpiresAt != 0 && ttl <= 0 {
		ttl = time.Millisecond
	}
	if c.cfg.l2 != nil {
		if err := c.cfg.l2.Set(ctx, fullKey, raw, ttl); err != nil {
			return fmt.Errorf("gocache: l2 set: %w", err)
		}
		c.emit(EventWritten{Key: fullKey, Tier: TierL2})
	}
	authOK, l1err := c.writeL1(ctx, fullKey, raw, ttl, o)
	if authOK && !o.skipBus {
		c.publish(opDelete, []string{fullKey}, "", "")
	}
	return l1err
}

func (c *Cache) writeL1(ctx context.Context, fullKey string, raw []byte, ttl time.Duration, o callOpts) (bool, error) {
	if c.cfg.l1 == nil {
		return true, nil
	}
	if o.skipL1 {
		if _, err := c.cfg.l1.Delete(ctx, fullKey); err != nil {
			c.logf("l1 skip delete failed", "key", fullKey, "err", err)
		}
		return true, nil
	}
	if err := c.cfg.l1.Set(ctx, fullKey, raw, ttl); err != nil {
		return c.cfg.l2 != nil, fmt.Errorf("gocache: l1 set: %w", err)
	}
	c.emit(EventWritten{Key: fullKey, Tier: TierL1})
	return true, nil
}

const (
	opDelete = "delete"
	opClear  = "clear"
	opTag    = "tag"
)

type busMsg struct {
	Origin string   `json:"o"`
	Op     string   `json:"op"`
	Keys   []string `json:"k,omitempty"`
	Prefix string   `json:"p,omitempty"`
	Tag    string   `json:"t,omitempty"`
}

func (c *Cache) publish(op string, keys []string, prefix, tag string) {
	if c.cfg.bus == nil {
		return
	}
	msg, err := json.Marshal(busMsg{Origin: c.rt.origin, Op: op, Keys: keys, Prefix: prefix, Tag: tag})
	if err != nil {
		c.logf("bus message encode failed", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(c.rt.ctx, 5*time.Second)
	defer cancel()
	if err := c.cfg.bus.Publish(ctx, msg); err != nil {
		c.emit(EventBusPublishFailed{Err: err})
		c.logf("bus publish failed", "err", err)
		c.enqueueRetry(msg)
	}
}

func decodeValue[T any](c *Cache, env envelope) (T, error) {
	var v T
	if err := c.cfg.codec.Unmarshal(env.Value, &v); err != nil {
		var zero T
		return zero, fmt.Errorf("gocache: decode value: %w", err)
	}
	return v, nil
}

func Get[T any](ctx context.Context, c *Cache, key string) (T, bool, error) {
	var zero T
	if c.rt.isClosed.Load() {
		return zero, false, ErrClosed
	}
	k := c.key(key)
	res, err := c.read(ctx, k)
	if err != nil {
		return zero, false, err
	}
	if !res.isFound || !res.isFresh {
		c.emit(EventMiss{Key: k})
		return zero, false, nil
	}
	c.emit(EventHit{Key: k, Tier: res.tier})
	v, err := decodeValue[T](c, res.env)
	if err != nil {
		return zero, false, err
	}
	return v, true, nil
}

func Set(ctx context.Context, c *Cache, key string, value any, opts ...CallOption) error {
	if c.rt.isClosed.Load() {
		return ErrClosed
	}
	o := c.callOpts(opts)
	raw, err := c.cfg.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("gocache: encode value: %w", err)
	}
	env := newEnvelope(raw, c.cfg.clock(), o.ttl, o.tags)
	return c.writeEnvelope(ctx, c.key(key), env, o)
}

func withForever(opts []CallOption) []CallOption {
	return slices.Concat(opts, []CallOption{WithTTL(0)})
}

func SetForever(ctx context.Context, c *Cache, key string, value any, opts ...CallOption) error {
	return Set(ctx, c, key, value, withForever(opts)...)
}

func (c *Cache) withinGrace(res readResult, o callOpts) bool {
	return res.isFound && o.grace > 0 && res.env.ExpiresAt != 0 &&
		c.cfg.clock().Before(time.UnixMilli(res.env.ExpiresAt).Add(o.grace))
}

func factoryError(err error) error {
	if errors.Is(err, ErrFactoryLimit) || errors.Is(err, ErrClosed) {
		return err
	}
	return fmt.Errorf("gocache: factory: %w", err)
}

func GetOrSet[T any](ctx context.Context, c *Cache, key string, factory func(context.Context) (T, error), opts ...CallOption) (T, error) {
	var zero T
	if c.rt.isClosed.Load() {
		return zero, ErrClosed
	}
	o := c.callOpts(opts)
	k := c.key(key)
	res, err := c.read(ctx, k)
	if err != nil {
		return zero, err
	}
	if res.isFound && res.isFresh {
		c.emit(EventHit{Key: k, Tier: res.tier})
		return decodeValue[T](c, res.env)
	}
	c.emit(EventMiss{Key: k})
	graced := c.withinGrace(res, o)
	ch := c.rt.sf.DoChan(k, func() (any, error) {
		return c.runFlight(ctx, k, func(fctx context.Context) (any, error) {
			return factory(fctx)
		}, o)
	})
	var softC <-chan time.Time
	if o.soft > 0 && graced {
		timer := time.NewTimer(o.soft)
		defer timer.Stop()
		softC = timer.C
	}
	select {
	case r := <-ch:
		if r.Err != nil {
			if graced {
				c.emit(EventGraceHit{Key: k, Err: r.Err})
				c.logf("grace hit", "key", k, "err", r.Err)
				return decodeValue[T](c, res.env)
			}
			return zero, factoryError(r.Err)
		}
		v, ok := r.Val.(T)
		if !ok {
			return zero, fmt.Errorf("gocache: type mismatch for key %q", k)
		}
		return v, nil
	case <-softC:
		c.emit(EventGraceHit{Key: k})
		return decodeValue[T](c, res.env)
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func GetOrSetForever[T any](ctx context.Context, c *Cache, key string, factory func(context.Context) (T, error), opts ...CallOption) (T, error) {
	return GetOrSet(ctx, c, key, factory, withForever(opts)...)
}

func (c *Cache) runFlight(callerCtx context.Context, fullKey string, factory func(context.Context) (any, error), o callOpts) (v any, err error) {
	if !c.rt.track() {
		return nil, ErrClosed
	}
	defer c.rt.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("gocache: factory panic: %v", p)
			c.emit(EventFactoryError{Key: fullKey, Err: err})
			c.logf("factory panic", "key", fullKey, "panic", p)
		}
		if err != nil {
			c.rt.sf.Forget(fullKey)
		}
	}()
	fctx, cancel := context.WithCancel(context.WithoutCancel(callerCtx))
	defer cancel()
	stop := context.AfterFunc(c.rt.ctx, cancel)
	defer stop()
	if o.hard > 0 {
		var tcancel context.CancelFunc
		fctx, tcancel = context.WithTimeout(fctx, o.hard)
		defer tcancel()
	}
	if aerr := c.rt.flights.Acquire(fctx, 1); aerr != nil {
		if c.rt.isClosed.Load() {
			return nil, ErrClosed
		}
		serr := fmt.Errorf("%w: %w", ErrFactoryLimit, aerr)
		c.emit(EventFactoryError{Key: fullKey, Err: serr})
		return nil, serr
	}
	defer c.rt.flights.Release(1)
	val, ferr := factory(fctx)
	if ferr != nil {
		c.emit(EventFactoryError{Key: fullKey, Err: ferr})
		return nil, ferr
	}
	raw, merr := c.cfg.codec.Marshal(val)
	if merr != nil {
		return nil, fmt.Errorf("gocache: encode value: %w", merr)
	}
	env := newEnvelope(raw, c.cfg.clock(), o.ttl, o.tags)
	if werr := c.writeEnvelope(fctx, fullKey, env, o); werr != nil {
		c.emit(EventWriteFailed{Key: fullKey, Err: werr})
		c.logf("write failed", "key", fullKey, "err", werr)
	}
	return val, nil
}

func Pull[T any](ctx context.Context, c *Cache, key string) (T, bool, error) {
	v, ok, err := Get[T](ctx, c, key)
	if err != nil || !ok {
		var zero T
		return zero, ok, err
	}
	if _, err := c.Delete(ctx, key); err != nil {
		var zero T
		return zero, false, err
	}
	return v, true, nil
}
