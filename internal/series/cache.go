package series

import "sync"

// Cache remembers which series lookup rows (series + day) have already been written, so that
// a series is written to the lookup table about once per day instead of with every data point.
//
// The cache is purely an optimisation. Series rows are idempotent (the lookup table is a
// ReplacingMergeTree keyed by the series), so a cache miss only costs a duplicate row that
// ClickHouse merges away. This is what makes it safe to keep the cache in process memory, to
// lose it on restart and to run several replicas, each with its own cache.
//
// Memory is bounded with two generations: keys are added to the current generation, and when it
// is full it becomes the previous generation and the oldest generation is dropped. Lookups that
// hit the previous generation move the key forward, so active series stay cached.
type Cache struct {
	mu        sync.Mutex
	current   map[Key]struct{}
	previous  map[Key]struct{}
	perGenCap int
}

// NewCache returns a cache holding up to about capacity keys. A capacity of 0 or less disables
// the cache: every key is reported as missing.
func NewCache(capacity int) *Cache {
	perGen := capacity / 2
	if capacity > 0 && perGen == 0 {
		perGen = 1
	}
	return &Cache{
		current:   make(map[Key]struct{}),
		previous:  make(map[Key]struct{}),
		perGenCap: perGen,
	}
}

// Missing returns the keys that are not in the cache, in their original order. It takes the
// lock once for all keys, so callers should pass all keys of a request together.
func (c *Cache) Missing(keys []Key) []Key {
	if c == nil || c.perGenCap <= 0 {
		return keys
	}
	var missing []Key
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		if _, ok := c.current[k]; ok {
			continue
		}
		if _, ok := c.previous[k]; ok {
			c.addLocked(k)
			continue
		}
		missing = append(missing, k)
	}
	return missing
}

// Add records keys whose series rows have been written successfully.
func (c *Cache) Add(keys []Key) {
	if c == nil || c.perGenCap <= 0 || len(keys) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		c.addLocked(k)
	}
}

// addLocked moves or adds k to the current generation. A key lives in exactly one generation.
func (c *Cache) addLocked(k Key) {
	if _, ok := c.current[k]; ok {
		return
	}
	delete(c.previous, k)
	if len(c.current) >= c.perGenCap {
		c.previous = c.current
		c.current = make(map[Key]struct{}, c.perGenCap)
	}
	c.current[k] = struct{}{}
}

// Len returns the number of cached keys.
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.current) + len(c.previous)
}
