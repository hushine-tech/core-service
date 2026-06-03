package venuekeys

import (
	"strings"
	"testing"
)

func TestNewBacktestAPIKeyGeneratesSyntheticNonEmptyKey(t *testing.T) {
	key, err := NewBacktestAPIKey()
	if err != nil {
		t.Fatalf("NewBacktestAPIKey: %v", err)
	}
	if !strings.HasPrefix(key, BacktestPrefix) {
		t.Fatalf("key = %q, want prefix %q", key, BacktestPrefix)
	}
	if len(key) != len(BacktestPrefix)+32 {
		t.Fatalf("key length = %d, want %d", len(key), len(BacktestPrefix)+32)
	}
	if !IsBacktestAPIKey(key) {
		t.Fatalf("IsBacktestAPIKey(%q) = false, want true", key)
	}
}

func TestNewBacktestAPIKeyGeneratesDifferentValues(t *testing.T) {
	first, err := NewBacktestAPIKey()
	if err != nil {
		t.Fatalf("first key: %v", err)
	}
	second, err := NewBacktestAPIKey()
	if err != nil {
		t.Fatalf("second key: %v", err)
	}
	if first == second {
		t.Fatalf("generated duplicate keys: %q", first)
	}
}

func TestIsBacktestAPIKeyRejectsRealOrMalformedKeys(t *testing.T) {
	for _, input := range []string{
		"",
		"api-key",
		"sim_btv_",
		"sim_btv_not-hex",
		"sim_btv_1234",
		"sim_btv_9f0b2d4c9a6e4b7c8d1e2f3a4b5c6d7z",
	} {
		if IsBacktestAPIKey(input) {
			t.Fatalf("IsBacktestAPIKey(%q) = true, want false", input)
		}
	}
}
