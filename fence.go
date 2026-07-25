package gocache

import (
	"context"
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"time"
)

const invalidationStripes = 4096

var invalidationSeed = maphash.MakeSeed()

type invalidationFence struct {
	stripes [invalidationStripes]atomic.Uint64
}

func (f *invalidationFence) stripe(fullKey string) *atomic.Uint64 {
	return &f.stripes[maphash.String(invalidationSeed, fullKey)%invalidationStripes]
}

func (f *invalidationFence) generation(fullKey string) uint64 {
	return f.stripe(fullKey).Load()
}

func (f *invalidationFence) unchangedSince(fullKey string, gen uint64) bool {
	return f.stripe(fullKey).Load() == gen
}

func (f *invalidationFence) invalidate(fullKey string) {
	f.stripe(fullKey).Add(1)
}

func (f *invalidationFence) invalidateMany(fullKeys []string) {
	for _, fullKey := range fullKeys {
		f.invalidate(fullKey)
	}
}

func (f *invalidationFence) invalidateEverything() {
	for i := range f.stripes {
		f.stripes[i].Add(1)
	}
}

func (c *Cache) fencedL1Set(ctx context.Context, fullKey string, raw []byte, ttl time.Duration, gen uint64) error {
	if !c.rt.fence.unchangedSince(fullKey, gen) {
		return nil
	}
	if err := c.cfg.l1.Set(ctx, fullKey, raw, ttl); err != nil {
		return err
	}
	if c.rt.fence.unchangedSince(fullKey, gen) {
		return nil
	}
	if _, err := c.cfg.l1.Delete(ctx, fullKey); err != nil {
		return fmt.Errorf("gocache: l1 fence rollback: %w", err)
	}
	return nil
}
