package accountmeta

import (
	"sync"
	"time"
)

type entry struct {
	meta      Meta
	expiresAt time.Time
}

// Cache is a TTL in-memory cache for AccountMeta keyed by account_id.
// Account config does not change during a strategy run; TTL prevents long-term staleness.
type Cache struct {
	mu  sync.RWMutex
	m   map[int64]entry
	ttl time.Duration
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{m: make(map[int64]entry), ttl: ttl}
}

// Get returns the cached Meta and true if found and not expired.
func (c *Cache) Get(accountID int64) (Meta, bool) {
	c.mu.RLock()
	e, ok := c.m[accountID]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expiresAt) {
		return Meta{}, false
	}
	return e.meta, true
}

// Set stores Meta with the configured TTL.
func (c *Cache) Set(accountID int64, meta Meta) {
	c.mu.Lock()
	c.m[accountID] = entry{meta: meta, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
