package notification

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu  sync.Mutex
	now func() time.Time
	hit map[string][]time.Time
}

func NewRateLimiter(now func() time.Time) *RateLimiter {
	if now == nil {
		now = time.Now
	}
	return &RateLimiter{
		now: now,
		hit: make(map[string][]time.Time),
	}
}

func (r *RateLimiter) Allow(key string, limit int, window time.Duration) bool {
	if limit <= 0 || window <= 0 {
		return false
	}
	now := r.now()
	cutoff := now.Add(-window)

	r.mu.Lock()
	defer r.mu.Unlock()

	hits := r.hit[key]
	keep := hits[:0]
	for _, t := range hits {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	if len(keep) >= limit {
		r.hit[key] = keep
		return false
	}
	keep = append(keep, now)
	r.hit[key] = keep
	return true
}
