package accountmeta

import (
	"testing"
	"time"
)

func TestCache_hitAndMiss(t *testing.T) {
	c := NewCache(time.Minute)

	// miss before set
	if _, ok := c.Get(1, 1, 2); ok {
		t.Fatal("expected miss before set")
	}

	meta := Meta{AccountID: 1, Exchange: 1, Market: 2, Environment: 0, SlippageBps: 5.0}
	c.Set(1, 1, 2, meta)

	got, ok := c.Get(1, 1, 2)
	if !ok {
		t.Fatal("expected hit after set")
	}
	if got.SlippageBps != 5.0 {
		t.Errorf("slippage_bps: got %v", got.SlippageBps)
	}
}

func TestCache_expiry(t *testing.T) {
	c := NewCache(10 * time.Millisecond)
	c.Set(2, 1, 2, Meta{AccountID: 2})

	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get(2, 1, 2); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestCache_multipleAccounts(t *testing.T) {
	c := NewCache(time.Minute)
	c.Set(10, 1, 2, Meta{AccountID: 10, Environment: 0})
	c.Set(20, 1, 2, Meta{AccountID: 20, Environment: 1})

	a, ok := c.Get(10, 1, 2)
	if !ok || a.Environment != 0 {
		t.Errorf("a: got %+v, ok=%v", a, ok)
	}
	b, ok := c.Get(20, 1, 2)
	if !ok || b.Environment != 1 {
		t.Errorf("b: got %+v, ok=%v", b, ok)
	}
}
