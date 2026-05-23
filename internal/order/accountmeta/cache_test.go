package accountmeta

import (
	"testing"
	"time"
)

func TestCache_hitAndMiss(t *testing.T) {
	c := NewCache(time.Minute)

	// miss before set
	if _, ok := c.Get(1); ok {
		t.Fatal("expected miss before set")
	}

	meta := Meta{AccountID: 1, Mode: 0, SlippageBps: 5.0}
	c.Set(1, meta)

	got, ok := c.Get(1)
	if !ok {
		t.Fatal("expected hit after set")
	}
	if got.SlippageBps != 5.0 {
		t.Errorf("slippage_bps: got %v", got.SlippageBps)
	}
}

func TestCache_expiry(t *testing.T) {
	c := NewCache(10 * time.Millisecond)
	c.Set(2, Meta{AccountID: 2})

	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get(2); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestCache_multipleAccounts(t *testing.T) {
	c := NewCache(time.Minute)
	c.Set(10, Meta{AccountID: 10, Mode: 0})
	c.Set(20, Meta{AccountID: 20, Mode: 1})

	a, ok := c.Get(10)
	if !ok || a.Mode != 0 {
		t.Errorf("a: got %+v, ok=%v", a, ok)
	}
	b, ok := c.Get(20)
	if !ok || b.Mode != 1 {
		t.Errorf("b: got %+v, ok=%v", b, ok)
	}
}
