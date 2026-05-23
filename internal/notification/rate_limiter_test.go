package notification

import (
	"testing"
	"time"
)

func TestRateLimiterAllowsBurstThenRefillsByWindow(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	limiter := NewRateLimiter(func() time.Time { return now })

	if !limiter.Allow("u:42", 2, time.Minute) {
		t.Fatalf("first event rejected")
	}
	if !limiter.Allow("u:42", 2, time.Minute) {
		t.Fatalf("second event rejected")
	}
	if limiter.Allow("u:42", 2, time.Minute) {
		t.Fatalf("third event accepted, want rate limited")
	}

	now = now.Add(time.Minute + time.Second)
	if !limiter.Allow("u:42", 2, time.Minute) {
		t.Fatalf("event after window rejected")
	}
}

func TestRateLimiterTreatsNonPositiveLimitAsDisabled(t *testing.T) {
	limiter := NewRateLimiter(time.Now)
	if limiter.Allow("u:42", 0, time.Minute) {
		t.Fatalf("non-positive limit accepted, want disabled")
	}
}
