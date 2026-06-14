package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hushine-tech/core-service/internal/logger"
)

type orderUserDataStreamManager interface {
	SyncOnce(ctx context.Context) (int, error)
}

func runOrderUserDataStreamManager(ctx context.Context, manager orderUserDataStreamManager, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	sync := func() {
		started, err := manager.SyncOnce(ctx)
		if err != nil {
			logger.Error(context.Background(), "system", fmt.Sprintf("order user data stream sync failed: %v", err))
			return
		}
		if started > 0 {
			logger.Info(context.Background(), "system", fmt.Sprintf("order user data stream started: count=%d", started))
		}
	}

	sync()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sync()
		}
	}
}
