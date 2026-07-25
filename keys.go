package gocache

import "strings"

const (
	defaultKeyPrefix = "gocache"
	keyVersion       = "1"
	domainData       = "d"
	domainTag        = "t"
	domainLock       = "l"
)

var segmentEscaper = strings.NewReplacer(`\`, `\\`, `:`, `\:`)

func escapeSegment(s string) string {
	return segmentEscaper.Replace(s)
}

func joinSegments(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	return a + ":" + b
}

func (c *Cache) domainRoot(domain string) string {
	return c.cfg.escapedKeyPrefix + ":" + keyVersion + ":" + domain
}

func (c *Cache) scopedPrefix(domain string) string {
	return joinSegments(c.domainRoot(domain), c.ns) + ":"
}

func (c *Cache) key(k string) string {
	return c.scopedPrefix(domainData) + escapeSegment(k)
}

func (c *Cache) tagKey(tag string) string {
	return c.domainRoot(domainTag) + ":" + escapeSegment(tag)
}

func (c *Cache) lockKey(name string) string {
	return c.scopedPrefix(domainLock) + escapeSegment(name)
}
