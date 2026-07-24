package gocache

import (
	"encoding/json"
	"time"
)

type envelope struct {
	Value     json.RawMessage `json:"v"`
	CreatedAt int64           `json:"c"`
	ExpiresAt int64           `json:"x"`
	Tags      []string        `json:"t,omitempty"`
}

func newEnvelope(value []byte, now time.Time, ttl time.Duration, tags []string) envelope {
	e := envelope{Value: value, CreatedAt: now.UnixMilli(), Tags: tags}
	if ttl > 0 {
		e.ExpiresAt = now.Add(ttl).UnixMilli()
	}
	return e
}

func (e envelope) fresh(now time.Time) bool {
	return e.ExpiresAt == 0 || now.UnixMilli() < e.ExpiresAt
}

func (e envelope) encode() ([]byte, error) {
	return json.Marshal(e)
}

func decodeEnvelope(data []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(data, &e)
	return e, err
}
