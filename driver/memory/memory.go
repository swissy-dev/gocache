package memory

import (
	"bytes"
	"container/list"
	"context"
	"strings"
	"sync"
	"time"
)

type Option func(*Driver)

func WithMaxEntries(n int) Option {
	return func(d *Driver) { d.maxEntries = n }
}

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

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

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

func (d *Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return nil, false, nil
	}
	en := el.Value.(*entry)
	if en.expired(time.Now()) {
		d.remove(el)
		return nil, false, nil
	}
	d.lru.MoveToFront(el)
	return en.value, true, nil
}

func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.set(key, value, ttl)
	return nil
}

func (d *Driver) set(key string, value []byte, ttl time.Duration) {
	v := make([]byte, len(value))
	copy(v, value)
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
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
	for len(d.items) > d.maxEntries {
		back := d.lru.Back()
		if back == nil {
			break
		}
		d.remove(back)
	}
}

func (d *Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[key]; ok {
		en := el.Value.(*entry)
		if !en.expired(time.Now()) {
			return false, nil
		}
		d.remove(el)
	}
	d.set(key, value, ttl)
	return true, nil
}

func (d *Driver) Delete(ctx context.Context, key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return false, nil
	}
	expired := el.Value.(*entry).expired(time.Now())
	d.remove(el)
	return !expired, nil
}

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

func (d *Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[key]
	if !ok {
		return false, nil
	}
	en := el.Value.(*entry)
	if en.expired(time.Now()) || !bytes.Equal(en.value, value) {
		return false, nil
	}
	d.remove(el)
	return true, nil
}

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

func (d *Driver) Close() error {
	return nil
}

func (d *Driver) remove(el *list.Element) {
	en := el.Value.(*entry)
	d.lru.Remove(el)
	delete(d.items, en.key)
}
