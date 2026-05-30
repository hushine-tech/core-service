package lifecycle

import (
	"context"
	"fmt"
)

type CancelTarget struct {
	SessionID       string
	AccountID       int64
	VenueID         int64
	OrderID         string
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
}

type CancelResult struct {
	ExchangeOrderID string
	ClientOrderID   string
	Status          string
}

type OrderCanceller interface {
	CancelOrder(ctx context.Context, target CancelTarget) (CancelResult, error)
}

type StopCancelResult struct {
	Cancelled        int
	StoppingFailed   bool
	ErrorMessage     string
	AttemptsPerOrder map[string]int
}

func CancelOpenOrders(ctx context.Context, targets []CancelTarget, canceller OrderCanceller, maxAttempts int) StopCancelResult {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	result := StopCancelResult{AttemptsPerOrder: make(map[string]int, len(targets))}
	for _, target := range targets {
		key := cancelTargetKey(target)
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			result.AttemptsPerOrder[key] = attempt
			_, err := canceller.CancelOrder(ctx, target)
			if err == nil {
				result.Cancelled++
				lastErr = nil
				break
			}
			lastErr = err
		}
		if lastErr != nil {
			result.StoppingFailed = true
			result.ErrorMessage = fmt.Sprintf("cancel open order failed after %d attempts: %s: %v", maxAttempts, key, lastErr)
			return result
		}
	}
	return result
}

func cancelTargetKey(target CancelTarget) string {
	if target.OrderID != "" {
		return target.OrderID
	}
	if target.ExchangeOrderID != "" {
		return target.ExchangeOrderID
	}
	if target.ClientOrderID != "" {
		return target.ClientOrderID
	}
	return target.Symbol
}
