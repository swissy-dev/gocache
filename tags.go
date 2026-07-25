package gocache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

func (c *Cache) parseTagTS(tag string, raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	ts, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		c.logf("unparseable tag timestamp, treating the tag as never invalidated", "tag", tag, "err", err)
		return 0
	}
	return ts
}

func (c *Cache) authoritative() Driver {
	if c.cfg.l2 != nil {
		return c.cfg.l2
	}
	return c.cfg.l1
}

func (c *Cache) tagInvalidatedAt(ctx context.Context, tag string) (int64, error) {
	k := c.tagKey(tag)
	if c.cfg.l1 != nil {
		raw, ok, err := c.cfg.l1.Get(ctx, k)
		if err != nil {
			return 0, fmt.Errorf("gocache: l1 tag read: %w", err)
		}
		if ok {
			return c.parseTagTS(tag, raw), nil
		}
	}
	if c.cfg.l2 == nil {
		return 0, nil
	}
	gen := c.rt.fence.generation(k)
	raw, ok, err := c.cfg.l2.Get(ctx, k)
	if err != nil {
		return 0, fmt.Errorf("gocache: l2 tag read: %w", err)
	}
	if !ok {
		raw = []byte("0")
	}
	if c.cfg.l1 != nil {
		if err := c.fencedL1Set(ctx, k, raw, c.cfg.tagCacheTTL, gen); err != nil {
			c.logf("l1 tag cache failed", "tag", tag, "err", err)
		}
	}
	return c.parseTagTS(tag, raw), nil
}

func (c *Cache) tagsValid(ctx context.Context, env envelope) (bool, error) {
	for _, tag := range env.Tags {
		ts, err := c.tagInvalidatedAt(ctx, tag)
		if err != nil {
			return false, err
		}
		if ts != 0 && env.CreatedAt <= ts {
			return false, nil
		}
	}
	return true, nil
}

func (c *Cache) DeleteByTag(ctx context.Context, tags ...string) error {
	if c.rt.closed.Load() {
		return ErrClosed
	}
	now := c.cfg.clock().UnixMilli()
	raw := strconv.AppendInt(nil, now, 10)
	auth := c.authoritative()
	var errs []error
	for _, tag := range tags {
		k := c.tagKey(tag)
		c.rt.fence.invalidate(k)
		if err := auth.Set(ctx, k, raw, 0); err != nil {
			errs = append(errs, fmt.Errorf("gocache: tag write %q: %w", tag, err))
		} else {
			if c.cfg.l1 != nil && c.cfg.l2 != nil {
				if err := c.cfg.l1.Set(ctx, k, raw, c.cfg.tagCacheTTL); err != nil {
					c.logf("l1 tag cache failed", "tag", tag, "err", err)
				}
			}
			c.emit(EventTagInvalidated{Tag: tag})
		}
		c.publish("tag", nil, "", tag)
	}
	return errors.Join(errs...)
}
