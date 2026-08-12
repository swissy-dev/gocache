package gocache

import "context"

// Bus broadcasts invalidations between instances so that a write on one does
// not leave stale L1 copies on the others. It is a best-effort channel, not a
// queue: messages may be dropped, and correctness must not depend on delivery.
// Missing one costs staleness bounded by the TTL, not incorrect behaviour.
//
// Publish is called on the write path and should not block for long. Subscribe
// registers a handler invoked for every message, including those this instance
// published — the cache filters its own out. Close must stop delivery before
// returning.
type Bus interface {
	Publish(ctx context.Context, msg []byte) error
	Subscribe(ctx context.Context, handler func(ctx context.Context, msg []byte)) error
	Close() error
}
