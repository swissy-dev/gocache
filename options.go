package gocache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

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

type Option func(*config) error

func WithL1(d Driver) Option {
	return func(cfg *config) error {
		if d == nil {
			return errors.New("gocache: nil l1 driver")
		}
		cfg.l1 = d
		return nil
	}
}

func WithL2(d Driver) Option {
	return func(cfg *config) error {
		if d == nil {
			return errors.New("gocache: nil l2 driver")
		}
		cfg.l2 = d
		return nil
	}
}

func WithBus(b Bus) Option {
	return func(cfg *config) error {
		if b == nil {
			return errors.New("gocache: nil bus")
		}
		cfg.bus = b
		return nil
	}
}

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

func WithDefaultTTL(d time.Duration) Option {
	return nonNegative("default ttl", d, func(cfg *config, v time.Duration) { cfg.defaultTTL = v })
}

func WithDefaultGrace(d time.Duration) Option {
	return nonNegative("grace", d, func(cfg *config, v time.Duration) { cfg.grace = v })
}

func WithSoftTimeout(d time.Duration) Option {
	return nonNegative("soft timeout", d, func(cfg *config, v time.Duration) { cfg.soft = v })
}

func WithHardTimeout(d time.Duration) Option {
	return nonNegative("hard timeout", d, func(cfg *config, v time.Duration) { cfg.hard = v })
}

func WithTagCacheTTL(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return errors.New("gocache: tag cache ttl must be positive")
		}
		cfg.tagCacheTTL = d
		return nil
	}
}

func WithCodec(c Codec) Option {
	return func(cfg *config) error {
		if c == nil {
			return errors.New("gocache: nil codec")
		}
		cfg.codec = c
		return nil
	}
}

func WithClock(clock func() time.Time) Option {
	return func(cfg *config) error {
		if clock == nil {
			return errors.New("gocache: nil clock")
		}
		cfg.clock = clock
		return nil
	}
}

func WithEventHook(hook func(Event)) Option {
	return func(cfg *config) error {
		cfg.hook = hook
		return nil
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(cfg *config) error {
		cfg.logger = l
		return nil
	}
}

func WithMaxConcurrentFactories(n int) Option {
	return func(cfg *config) error {
		if n < 1 {
			return errors.New("gocache: max concurrent factories must be positive")
		}
		cfg.maxFactories = n
		return nil
	}
}

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

type CallOption func(*callOpts)

func WithTTL(d time.Duration) CallOption {
	return func(o *callOpts) { o.ttl = d }
}

func WithTags(tags ...string) CallOption {
	return func(o *callOpts) { o.tags = tags }
}

func WithGrace(d time.Duration) CallOption {
	return func(o *callOpts) { o.grace = d }
}

func WithCallSoftTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.soft = d }
}

func WithCallHardTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.hard = d }
}

func WithSkipL1() CallOption {
	return func(o *callOpts) { o.skipL1 = true }
}

func WithSkipBus() CallOption {
	return func(o *callOpts) { o.skipBus = true }
}
