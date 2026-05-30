package lifecycle

import (
	"context"
	"errors"
	"testing"
)

type failingCanceller struct {
	calls int
	err   error
}

func (c *failingCanceller) CancelOrder(_ context.Context, _ CancelTarget) (CancelResult, error) {
	c.calls++
	return CancelResult{}, c.err
}

func TestStopCancelOpenOrdersRetriesThreeTimesThenStoppingFailed(t *testing.T) {
	canceller := &failingCanceller{err: errors.New("exchange timeout")}
	result := CancelOpenOrders(context.Background(), []CancelTarget{{
		OrderID: "order-1",
		Symbol:  "ETHUSDT",
	}}, canceller, 3)

	if !result.StoppingFailed {
		t.Fatal("expected stopping_failed result")
	}
	if canceller.calls != 3 {
		t.Fatalf("cancel calls = %d, want 3", canceller.calls)
	}
	if result.AttemptsPerOrder["order-1"] != 3 {
		t.Fatalf("attempts = %+v, want order-1=3", result.AttemptsPerOrder)
	}
	if result.ErrorMessage == "" {
		t.Fatal("expected manual-check error message")
	}
}
