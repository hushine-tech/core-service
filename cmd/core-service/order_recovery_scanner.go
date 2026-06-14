package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hushine-tech/core-service/internal/logger"
)

type orderRecoveryScanner interface {
	ScanOnce(ctx context.Context, now time.Time) (int, error)
}

func runOrderRecoveryScanner(ctx context.Context, scanner orderRecoveryScanner, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	scan := func(now time.Time) {
		written, err := scanner.ScanOnce(ctx, now.UTC())
		if err != nil {
			logger.Error(context.Background(), "system", fmt.Sprintf("order recovery scanner failed: %v", err))
			return
		}
		if written > 0 {
			logger.Info(context.Background(), "system", fmt.Sprintf("order recovery scanner wrote lifecycle events: count=%d", written))
		}
	}

	scan(time.Now().UTC())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			scan(now)
		}
	}
}
