package gocache

import (
	"context"
	"errors"
	"fmt"
)

type Cache struct {
	cfg    *config
	prefix string
	rt     *runtime
}

func New(opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &runtime{
		ctx:    ctx,
		cancel: cancel,
		origin: newOrigin(),
		retryQ: make(chan []byte, cfg.busQueue),
	}
	c := &Cache{cfg: cfg, rt: rt}
	if cfg.bus != nil {
		if err := cfg.bus.Subscribe(ctx, c.handleBusMsg); err != nil {
			cancel()
			return nil, err
		}
		rt.track()
		go c.drainRetries()
	}
	return c, nil
}

func (c *Cache) Namespace(name string) *Cache {
	nc := *c
	nc.prefix = c.prefix + name + ":"
	return &nc
}

func (c *Cache) key(k string) string {
	return c.prefix + k
}

func (c *Cache) callOpts(opts []CallOption) callOpts {
	o := callOpts{
		ttl:   c.cfg.defaultTTL,
		grace: c.cfg.grace,
		soft:  c.cfg.soft,
		hard:  c.cfg.hard,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.ttl < 0 {
		o.ttl = 0
	}
	if o.grace < 0 {
		o.grace = 0
	}
	return o
}

func (c *Cache) emit(e Event) {
	if c.cfg.hook == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			c.logf("event hook panic", "panic", p)
		}
	}()
	c.cfg.hook(e)
}

func (c *Cache) logf(msg string, args ...any) {
	if c.cfg.logger == nil {
		return
	}
	c.cfg.logger.Warn("gocache: "+msg, args...)
}

func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	if c.rt.closed.Load() {
		return false, ErrClosed
	}
	res, err := c.read(ctx, c.key(key))
	if err != nil {
		return false, err
	}
	return res.found && res.fresh, nil
}

func (c *Cache) Delete(ctx context.Context, key string) (bool, error) {
	if c.rt.closed.Load() {
		return false, ErrClosed
	}
	k := c.key(key)
	var existed bool
	var errs []error
	authOK := true
	if c.cfg.l2 != nil {
		ok, err := c.cfg.l2.Delete(ctx, k)
		existed = existed || ok
		if err != nil {
			errs = append(errs, fmt.Errorf("gocache: l2 delete: %w", err))
			authOK = false
		}
	}
	if c.cfg.l1 != nil {
		ok, err := c.cfg.l1.Delete(ctx, k)
		existed = existed || ok
		if err != nil {
			errs = append(errs, fmt.Errorf("gocache: l1 delete: %w", err))
			if c.cfg.l2 == nil {
				authOK = false
			}
		}
	}
	err := errors.Join(errs...)
	if err == nil {
		c.emit(EventDeleted{Key: k})
	}
	if authOK {
		c.publish("delete", []string{k}, "", "")
	}
	if err != nil {
		return false, err
	}
	return existed, nil
}

func (c *Cache) DeleteMany(ctx context.Context, keys []string) error {
	if c.rt.closed.Load() {
		return ErrClosed
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.key(k)
	}
	var errs []error
	authOK := true
	if c.cfg.l2 != nil {
		if err := c.cfg.l2.DeleteMany(ctx, full); err != nil {
			errs = append(errs, fmt.Errorf("gocache: l2 delete many: %w", err))
			authOK = false
		}
	}
	if c.cfg.l1 != nil {
		if err := c.cfg.l1.DeleteMany(ctx, full); err != nil {
			errs = append(errs, fmt.Errorf("gocache: l1 delete many: %w", err))
			if c.cfg.l2 == nil {
				authOK = false
			}
		}
	}
	err := errors.Join(errs...)
	if err == nil {
		for _, k := range full {
			c.emit(EventDeleted{Key: k})
		}
	}
	if authOK {
		c.publish("delete", full, "", "")
	}
	return err
}

func (c *Cache) Clear(ctx context.Context) error {
	if c.rt.closed.Load() {
		return ErrClosed
	}
	var errs []error
	authOK := true
	if c.cfg.l2 != nil {
		if err := c.cfg.l2.ClearPrefix(ctx, c.prefix); err != nil {
			errs = append(errs, fmt.Errorf("gocache: l2 clear: %w", err))
			authOK = false
		}
	}
	if c.cfg.l1 != nil {
		if err := c.cfg.l1.ClearPrefix(ctx, c.prefix); err != nil {
			errs = append(errs, fmt.Errorf("gocache: l1 clear: %w", err))
			if c.cfg.l2 == nil {
				authOK = false
			}
		}
	}
	err := errors.Join(errs...)
	if err == nil {
		c.emit(EventCleared{Prefix: c.prefix})
	}
	if authOK {
		c.publish("clear", nil, c.prefix, "")
	}
	return err
}
