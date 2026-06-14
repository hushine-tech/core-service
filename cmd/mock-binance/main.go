package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/exchange/binance/mockserver"
)

func main() {
	addr := flag.String("addr", ":19000", "HTTP/WebSocket listen address")
	flag.Parse()

	mock := mockserver.NewWithConfig(mockConfigFromEnv(os.Getenv))
	hostPort := displayHostPort(*addr)
	log.Printf("mock Binance REST/WS listening on %s", *addr)
	log.Printf("REST base URL: http://%s", hostPort)
	log.Printf("WS base URL: ws://%s", hostPort)
	if err := http.ListenAndServe(*addr, mock.Handler()); err != nil {
		log.Fatal(err)
	}
}

func mockConfigFromEnv(lookup func(string) string) mockserver.Config {
	return mockserver.Config{
		Scene3Delay: parseSecondsEnv(lookup("MOCK_BINANCE_SCENE3_DELAY_SECONDS")),
		Scene9Delay: parseSecondsEnv(lookup("MOCK_BINANCE_SCENE9_DELAY_SECONDS")),
	}
}

func parseSecondsEnv(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func displayHostPort(addr string) string {
	addr = strings.TrimSpace(addr)
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}
