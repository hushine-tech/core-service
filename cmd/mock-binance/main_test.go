package main

import (
	"testing"
	"time"
)

func TestMockConfigFromEnvParsesSceneDelays(t *testing.T) {
	cfg := mockConfigFromEnv(func(key string) string {
		switch key {
		case "MOCK_BINANCE_SCENE3_DELAY_SECONDS":
			return "7"
		case "MOCK_BINANCE_SCENE9_DELAY_SECONDS":
			return "11"
		default:
			return ""
		}
	})
	if cfg.Scene3Delay != 7*time.Second {
		t.Fatalf("Scene3Delay = %s, want 7s", cfg.Scene3Delay)
	}
	if cfg.Scene9Delay != 11*time.Second {
		t.Fatalf("Scene9Delay = %s, want 11s", cfg.Scene9Delay)
	}
}

func TestDisplayHostPortNormalizesListenAddr(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{addr: ":19000", want: "127.0.0.1:19000"},
		{addr: "127.0.0.1:19091", want: "127.0.0.1:19091"},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if got := displayHostPort(tt.addr); got != tt.want {
				t.Fatalf("displayHostPort(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}
