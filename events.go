package gocache

type Tier string

const (
	TierL1 Tier = "l1"
	TierL2 Tier = "l2"
)

type Event interface {
	event()
}

type EventHit struct {
	Key  string
	Tier Tier
}

type EventMiss struct {
	Key string
}

type EventWritten struct {
	Key  string
	Tier Tier
}

type EventDeleted struct {
	Key string
}

type EventCleared struct {
	Prefix string
}

type EventTagInvalidated struct {
	Tag string
}

type EventGraceHit struct {
	Key string
	Err error
}

type EventFactoryError struct {
	Key string
	Err error
}

type EventWriteFailed struct {
	Key string
	Err error
}

type EventBusPublishFailed struct {
	Err     error
	Dropped bool
}

type EventBusMessageReceived struct {
	Op string
}

type EventLockAcquired struct {
	Key string
}

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
