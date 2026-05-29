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
	m   map[cacheKey]entry
	ttl time.Duration
}

type cacheKey struct {
	accountID int64
	exchange  int32
	market    int32
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{m: make(map[cacheKey]entry), ttl: ttl}
}

// Get returns the cached Meta and true if found and not expired.
func (c *Cache) Get(accountID int64, exchange int32, market int32) (Meta, bool) {
	c.mu.RLock()
	e, ok := c.m[cacheKey{accountID: accountID, exchange: exchange, market: market}]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expiresAt) {
		return Meta{}, false
	}
	return e.meta, true
}

// Set stores Meta with the configured TTL.
func (c *Cache) Set(accountID int64, exchange int32, market int32, meta Meta) {
	c.mu.Lock()
	c.m[cacheKey{accountID: accountID, exchange: exchange, market: market}] = entry{meta: meta, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
