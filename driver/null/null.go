// Package null provides a driver that stores nothing.
//
// Every read misses and every write succeeds without retaining anything. It is
// meant for disabling caching in a specific environment without changing the
// code that uses the cache, and for isolating whether a bug involves caching at
// all.
//
// It is not a conformance-passing driver: Add always reports success, because
// there is no state in which to detect a conflict. Do not use it as the
// authoritative tier behind gocache.Cache.Lock, which relies on Add being
// genuinely atomic.
package null

import (
	"context"
	"time"
)

// Driver discards everything written to it. It is safe for concurrent use.
type Driver struct{}

// New returns a driver that stores nothing.
func New() *Driver {
	return &Driver{}
}

// Get implements gocache.Reader. It always reports a miss.
func (*Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return nil, false, nil
}

// Set implements gocache.Writer.
func (*Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}

// Add implements gocache.Atomic. It always reports success, since there is
// no stored state in which to detect a conflict.
func (*Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return true, nil
}

// Delete implements gocache.Writer. It always reports that nothing existed.
func (*Driver) Delete(ctx context.Context, key string) (bool, error) {
	return false, nil
}

// DeleteMany implements gocache.Writer.
func (*Driver) DeleteMany(ctx context.Context, keys []string) error {
	return nil
}

// DeleteIfEquals implements gocache.Atomic. It always reports no match.
func (*Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	return false, nil
}

// ClearPrefix implements gocache.Writer.
func (*Driver) ClearPrefix(ctx context.Context, prefix string) error {
	return nil
}

// Close implements io.Closer.
func (*Driver) Close() error {
	return nil
}
