// Package memory provides an in-process cache driver backed by a map with LRU
// eviction.
//
// It is the usual choice for the L1 tier. Entries live only in this process, so
// with more than one instance running, a gocache.Bus is what keeps them from
// drifting apart.
//
// Expiry is lazy: an entry past its TTL is reported as absent and removed when
// it is next touched, rather than by a background sweeper. Memory is therefore
// bounded by the entry limit rather than by TTLs — see [WithMaxEntries].
package memory

import (
	"bytes"
	"container/list"
	"context"
	"strings"
	"sync"
	"time"
)

const expiryScanWindow = 8

// Option configures a [Driver].
type Option func(*Driver)

// WithMaxEntries caps how many entries the driver holds, defaulting to 10,000.
// Once full, each insert evicts the least recently used entry. Values below one
// are raised to one.
//
// The limit counts entries, not bytes, so size the cache against the size of
// the values being stored.
func WithMaxEntries(n int) Option {
	return func(d *Driver) { d.maxEntries = n }
}

// Driver is an in-process cache with LRU eviction. Use [New] to create one. It
// is safe for concurrent use, guarded by a single mutex, so it serialises
// access across all keys.
type Driver struct {
	mu         sync.Mutex
	maxEntries int
	items      map[string]*list.Element
	lru        *list.List
}

type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

func (e *entry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// New returns an in-process driver holding up to 10,000 entries unless
// [WithMaxEntries] says otherwise.
func New(opts ...Option) *Driver {
	d := &Driver{
		maxEntries: 10_000,
		items:      make(map[string]*list.Element),
		lru:        list.New(),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.maxEntries < 1 {
		d.maxEntries = 1
	}
	return d
}

// Get implements gocache.Reader. An entry past its TTL is reported as absent
// and dropped.
//
// The returned slice is the stored value itself, not a copy; modifying it would
// corrupt the cached entry, so treat it as read-only.
func (d *Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return nil, false, nil
	}
	en := el.Value.(*entry)
	if en.isExpired(time.Now()) {
		d.remove(el)
		return nil, false, nil
	}
	d.lru.MoveToFront(el)
	return en.value, true, nil
}

// Set implements gocache.Writer. It copies value, so the caller may reuse the
// slice afterwards. A ttl of zero or less stores the entry with no expiry.
func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.set(key, value, ttl)
	return nil
}

func (d *Driver) set(key string, value []byte, ttl time.Duration) {
	v := make([]byte, len(value))
	copy(v, value)
	now := time.Now()
	var exp time.Time
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	if el, ok := d.items[key]; ok {
		en := el.Value.(*entry)
		en.value = v
		en.expiresAt = exp
		d.lru.MoveToFront(el)
		return
	}
	el := d.lru.PushFront(&entry{key: key, value: v, expiresAt: exp})
	d.items[key] = el
	if len(d.items) > d.maxEntries {
		d.reclaimExpiredFromColdEnd(now, expiryScanWindow)
	}
	for len(d.items) > d.maxEntries {
		back := d.lru.Back()
		if back == nil {
			break
		}
		d.remove(back)
	}
}

func (d *Driver) reclaimExpiredFromColdEnd(now time.Time, window int) {
	el := d.lru.Back()
	for i := 0; i < window && el != nil; i++ {
		prev := el.Prev()
		if el.Value.(*entry).isExpired(now) {
			d.remove(el)
		}
		el = prev
	}
}

// Add implements gocache.Atomic. An entry present but expired counts as
// absent, so the add succeeds and replaces it.
func (d *Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[key]; ok {
		en := el.Value.(*entry)
		if !en.isExpired(time.Now()) {
			return false, nil
		}
		d.remove(el)
	}
	d.set(key, value, ttl)
	return true, nil
}

// Delete implements gocache.Writer. It reports false for an entry that was
// present but already expired.
func (d *Driver) Delete(ctx context.Context, key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return false, nil
	}
	expired := el.Value.(*entry).isExpired(time.Now())
	d.remove(el)
	return !expired, nil
}

// DeleteMany implements gocache.Writer. Keys that are absent are ignored.
func (d *Driver) DeleteMany(ctx context.Context, keys []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, k := range keys {
		if el, ok := d.items[k]; ok {
			d.remove(el)
		}
	}
	return nil
}

// DeleteIfEquals implements gocache.Atomic. An expired entry never matches.
func (d *Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return false, nil
	}
	en := el.Value.(*entry)
	if en.isExpired(time.Now()) || !bytes.Equal(en.value, value) {
		return false, nil
	}
	d.remove(el)
	return true, nil
}

// ClearPrefix implements gocache.Writer. An empty prefix removes everything.
func (d *Driver) ClearPrefix(ctx context.Context, prefix string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, el := range d.items {
		if strings.HasPrefix(k, prefix) {
			d.remove(el)
		}
	}
	return nil
}

// Close implements io.Closer. It always returns nil; the driver holds no
// resources beyond memory, which is released when it becomes unreachable.
func (d *Driver) Close() error {
	return nil
}

func (d *Driver) remove(el *list.Element) {
	en := el.Value.(*entry)
	d.lru.Remove(el)
	delete(d.items, en.key)
}
