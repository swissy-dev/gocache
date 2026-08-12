package gocache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Codec encodes cached values. The default is encoding/json.
//
// A replacement must produce valid JSON, because values are embedded in a JSON
// envelope alongside their metadata. [New] verifies this with a probe value and
// fails if the codec does not round-trip through JSON. A faster JSON library is
// a drop-in; a binary format is not — encode to bytes yourself and cache a
// struct with a []byte field, which encoding/json renders as base64.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

const (
	defaultHardTimeout  = 30 * time.Second
	defaultMaxFactories = 1024
)

type config struct {
	l1               Driver
	l2               Driver
	bus              Bus
	escapedKeyPrefix string

	defaultTTL   time.Duration
	grace        time.Duration
	soft         time.Duration
	hard         time.Duration
	tagCacheTTL  time.Duration
	codec        Codec
	clock        func() time.Time
	hook         func(Event)
	logger       *slog.Logger
	busQueue     int
	maxFactories int
}

func defaultConfig() *config {
	return &config{
		escapedKeyPrefix: escapeSegment(defaultKeyPrefix),

		defaultTTL:   30 * time.Minute,
		hard:         defaultHardTimeout,
		tagCacheTTL:  10 * time.Second,
		codec:        jsonCodec{},
		clock:        time.Now,
		logger:       slog.Default(),
		busQueue:     1024,
		maxFactories: defaultMaxFactories,
	}
}

func (cfg *config) validate() error {
	if cfg.l1 == nil && cfg.l2 == nil {
		return errors.New("gocache: at least one tier is required")
	}
	if cfg.bus != nil && (cfg.l1 == nil || cfg.l2 == nil) {
		return errors.New("gocache: a bus requires both l1 and l2")
	}
	return cfg.validateCodec()
}

type codecProbe struct {
	Value string `json:"v"`
	Count int    `json:"n"`
}

func (cfg *config) validateCodec() error {
	raw, err := cfg.codec.Marshal(codecProbe{Value: "gocache", Count: 1})
	if err != nil {
		return fmt.Errorf("gocache: codec rejected the probe value: %w", err)
	}
	if !json.Valid(raw) {
		return errors.New("gocache: codec must produce valid json because values are embedded in a json envelope")
	}
	return nil
}

// Option configures a [Cache] at construction. Invalid values are reported as
// an error from [New] rather than panicking or being silently ignored.
type Option func(*config) error

// WithL1 sets the in-process first tier, normally the memory driver. At least
// one tier is required. The driver must not be nil.
func WithL1(d Driver) Option {
	return func(cfg *config) error {
		if d == nil {
			return errors.New("gocache: nil l1 driver")
		}
		cfg.l1 = d
		return nil
	}
}

// WithL2 sets the shared second tier, such as Redis or SQL.
//
// When both tiers are configured L2 is authoritative: writes land there first
// and a failure there fails the call, while an L1 failure is reported without
// losing the value. Locks and tag markers also live in the authoritative tier.
// The driver must not be nil.
func WithL2(d Driver) Option {
	return func(cfg *config) error {
		if d == nil {
			return errors.New("gocache: nil l2 driver")
		}
		cfg.l2 = d
		return nil
	}
}

// WithBus sets the invalidation channel used to evict stale L1 copies on other
// instances after a write.
//
// A bus requires both tiers — it exists to reconcile them — so [New] fails if
// either is missing. Publish failures never fail the calling operation; they
// are retried from a bounded queue and surface as [EventBusPublishFailed]. The
// bus must not be nil.
func WithBus(b Bus) Option {
	return func(cfg *config) error {
		if b == nil {
			return errors.New("gocache: nil bus")
		}
		cfg.bus = b
		return nil
	}
}

// WithKeyPrefix sets the leading segment of every key this cache writes,
// defaulting to "gocache". Give unrelated applications sharing one store
// different prefixes so that [Cache.Clear] cannot reach across them. The prefix
// must not be empty.
func WithKeyPrefix(prefix string) Option {
	return func(cfg *config) error {
		if strings.TrimSpace(prefix) == "" {
			return errors.New("gocache: key prefix must not be empty")
		}
		cfg.escapedKeyPrefix = escapeSegment(prefix)
		return nil
	}
}

func nonNegative(name string, d time.Duration, assign func(*config, time.Duration)) Option {
	return func(cfg *config) error {
		if d < 0 {
			return errors.New("gocache: negative " + name)
		}
		assign(cfg, d)
		return nil
	}
}

// WithDefaultTTL sets how long values stay fresh when a call does not specify
// otherwise. The default is 30 minutes; zero means no expiry. It must not be
// negative — see [WithTTL] to override per call.
func WithDefaultTTL(d time.Duration) Option {
	return nonNegative("default ttl", d, func(cfg *config, v time.Duration) { cfg.defaultTTL = v })
}

// WithDefaultGrace sets how long past its logical expiry a value remains
// physically available to serve when a factory fails. The default is zero,
// which disables grace entirely. It must not be negative.
//
// Grace makes entries outlive their TTL in the store, so it trades storage and
// worst-case staleness for resilience when an origin is down. See [WithGrace]
// to override per call.
func WithDefaultGrace(d time.Duration) Option {
	return nonNegative("grace", d, func(cfg *config, v time.Duration) { cfg.grace = v })
}

// WithSoftTimeout sets how long [GetOrSet] waits for a factory before serving
// a stale value instead, leaving the factory running in the background. The
// default is zero, which disables it.
//
// It only applies when a stale value is within its grace window; with no grace
// there is nothing to serve early.
func WithSoftTimeout(d time.Duration) Option {
	return nonNegative("soft timeout", d, func(cfg *config, v time.Duration) { cfg.soft = v })
}

// WithHardTimeout bounds how long a factory may run before its context is
// cancelled, defaulting to 30 seconds. Zero removes the bound, leaving the
// factory limited only by the cache's lifetime.
func WithHardTimeout(d time.Duration) Option {
	return nonNegative("hard timeout", d, func(cfg *config, v time.Duration) { cfg.hard = v })
}

// WithTagCacheTTL sets how long tag invalidation markers are cached in L1,
// defaulting to 10 seconds. This bounds how long a read can miss a tag
// invalidation issued by another instance, and is the price of not reading
// every tag marker from L2 on every tagged read. It must be positive.
func WithTagCacheTTL(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return errors.New("gocache: tag cache ttl must be positive")
		}
		cfg.tagCacheTTL = d
		return nil
	}
}

// WithCodec replaces the default encoding/json codec. The codec must not be
// nil and must produce valid JSON; see [Codec].
func WithCodec(c Codec) Option {
	return func(cfg *config) error {
		if c == nil {
			return errors.New("gocache: nil codec")
		}
		cfg.codec = c
		return nil
	}
}

// WithClock replaces time.Now, which is how tests drive expiry without
// sleeping. The function must not be nil and must be safe for concurrent use.
func WithClock(clock func() time.Time) Option {
	return func(cfg *config) error {
		if clock == nil {
			return errors.New("gocache: nil clock")
		}
		cfg.clock = clock
		return nil
	}
}

// WithEventHook registers a callback for every [Event] the cache emits, which
// is how hits, misses and background failures are surfaced for metrics.
//
// The hook is called synchronously on the operation's goroutine, so it must not
// block or call back into the cache.
func WithEventHook(hook func(Event)) Option {
	return func(cfg *config) error {
		cfg.hook = hook
		return nil
	}
}

// WithLogger sets where the cache reports failures that do not fail an
// operation, such as a bus publish that had to be queued for retry. It defaults
// to slog.Default(); pass a logger discarding output to silence it.
func WithLogger(l *slog.Logger) Option {
	return func(cfg *config) error {
		cfg.logger = l
		return nil
	}
}

// WithMaxConcurrentFactories caps how many [GetOrSet] factories may run at
// once across the cache, defaulting to 1024. It is a backstop against a cold
// cache spawning unbounded work, not a tuning knob. Calls beyond the limit fail
// with [ErrFactoryLimit]. It must be positive.
func WithMaxConcurrentFactories(n int) Option {
	return func(cfg *config) error {
		if n < 1 {
			return errors.New("gocache: max concurrent factories must be positive")
		}
		cfg.maxFactories = n
		return nil
	}
}

// WithBusRetryQueueSize caps how many failed bus publishes are held for retry,
// defaulting to 1024. The queue is bounded so a prolonged bus outage cannot
// grow memory without limit; messages beyond it are dropped, costing staleness
// on peers until their copies expire. It must be positive.
func WithBusRetryQueueSize(n int) Option {
	return func(cfg *config) error {
		if n < 1 {
			return errors.New("gocache: bus retry queue size must be positive")
		}
		cfg.busQueue = n
		return nil
	}
}

type callOpts struct {
	ttl     time.Duration
	grace   time.Duration
	soft    time.Duration
	hard    time.Duration
	tags    []string
	skipL1  bool
	skipBus bool
}

// CallOption overrides cache-level configuration for a single operation.
type CallOption func(*callOpts)

// WithTTL overrides the default TTL for this call. Zero means no expiry, and a
// negative value is clamped to zero — so a negative TTL stores a permanent
// entry rather than an already-expired one.
func WithTTL(d time.Duration) CallOption {
	return func(o *callOpts) { o.ttl = d }
}

// WithTags attaches tags to the value being written, so it can later be
// invalidated in bulk by [Cache.DeleteByTag] without knowing its key.
//
// Tags are recorded on write only; adding a tag to an existing entry means
// writing it again. Every tagged read checks each of its tags, so tags are not
// free — see [WithTagCacheTTL].
func WithTags(tags ...string) CallOption {
	return func(o *callOpts) { o.tags = tags }
}

// WithGrace overrides the default grace window for this call. Zero disables
// grace, and a negative value is clamped to zero.
func WithGrace(d time.Duration) CallOption {
	return func(o *callOpts) { o.grace = d }
}

// WithCallSoftTimeout overrides [WithSoftTimeout] for this call.
func WithCallSoftTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.soft = d }
}

// WithCallHardTimeout overrides [WithHardTimeout] for this call.
func WithCallHardTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.hard = d }
}

// WithSkipL1 writes to L2 only and removes any L1 copy, so this instance does
// not hold a value it just wrote. Useful when the writer will not read the
// value back and caching it locally would only waste space.
func WithSkipL1() CallOption {
	return func(o *callOpts) { o.skipL1 = true }
}

// WithSkipBus suppresses the invalidation broadcast for this write, leaving
// peers to serve their existing L1 copies until those expire. Use it for writes
// whose staleness elsewhere does not matter, to avoid the bus traffic.
func WithSkipBus() CallOption {
	return func(o *callOpts) { o.skipBus = true }
}
