package gocache

// Tier identifies which cache tier an event refers to.
type Tier string

// The configured cache tiers: L1 is in-process, L2 is shared.
const (
	TierL1 Tier = "l1"
	TierL2 Tier = "l2"
)

// Event is emitted by the cache and delivered to the callback registered with
// [WithEventHook]. Use a type switch to select the ones you care about.
//
// The set is closed: Event cannot be implemented outside this package, so a
// type switch over the Event types below is exhaustive as of this version.
//
// Key fields carry the full stored key — prefixed, namespaced and escaped —
// not the key passed by the caller.
type Event interface {
	event()
}

// EventHit reports a read served from cache, and which tier answered it.
type EventHit struct {
	Key  string
	Tier Tier
}

// EventMiss reports a read that found nothing fresh. An entry that exists but
// is logically expired is a miss, even if grace later serves it.
type EventMiss struct {
	Key string
}

// EventWritten reports a value stored in a tier. A write to both tiers emits
// one event per tier.
type EventWritten struct {
	Key  string
	Tier Tier
}

// EventDeleted reports a key removed from every tier. It is not emitted when
// the delete failed.
type EventDeleted struct {
	Key string
}

// EventCleared reports a [Cache.Clear], carrying the key prefix that was
// removed.
type EventCleared struct {
	Prefix string
}

// EventTagInvalidated reports a tag marked invalid by Cache.DeleteByTag. No
// entries are visited, so the number affected is not known.
type EventTagInvalidated struct {
	Tag string
}

// EventGraceHit reports a stale value served instead of a fresh one.
//
// Err holds the factory's error when grace covered a failure, and is nil when
// the value was served early because the factory exceeded its soft timeout and
// was left running.
type EventGraceHit struct {
	Key string
	Err error
}

// EventFactoryError reports a [GetOrSet] factory that failed or panicked. The
// call may still have succeeded, if grace served a stale value.
type EventFactoryError struct {
	Key string
	Err error
}

// EventWriteFailed reports a factory result that could not be stored. The
// value is still returned to the caller, so this is the only signal that the
// next call will have to compute it again.
type EventWriteFailed struct {
	Key string
	Err error
}

// EventBusPublishFailed reports an invalidation that could not be broadcast.
// The operation itself succeeded; peers may hold stale L1 copies until those
// expire.
//
// Dropped distinguishes a message queued for retry from one discarded because
// the retry queue was full — see [WithBusRetryQueueSize].
type EventBusPublishFailed struct {
	Err     error
	Dropped bool
}

// EventBusMessageReceived reports an invalidation received from another
// instance, carrying its operation: "delete", "clear" or "tag". Messages this
// instance published are filtered out before this is emitted.
type EventBusMessageReceived struct {
	Op string
}

// EventLockAcquired reports a lock lease taken.
type EventLockAcquired struct {
	Key string
}

// EventLockReleased reports a lock lease given up by its owner. It is not
// emitted when the lease had already expired and been taken by someone else, or
// for Lock.ForceRelease.
type EventLockReleased struct {
	Key string
}

func (EventHit) event()                {}
func (EventMiss) event()               {}
func (EventWritten) event()            {}
func (EventDeleted) event()            {}
func (EventCleared) event()            {}
func (EventTagInvalidated) event()     {}
func (EventGraceHit) event()           {}
func (EventFactoryError) event()       {}
func (EventWriteFailed) event()        {}
func (EventBusPublishFailed) event()   {}
func (EventBusMessageReceived) event() {}
func (EventLockAcquired) event()       {}
func (EventLockReleased) event()       {}
