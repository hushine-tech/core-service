package notification

import (
	"context"
	"time"
)

const (
	SchemaVersion = 1

	CategoryStrategy = "strategy"

	SeverityInfo  = "info"
	SeverityWarn  = "warn"
	SeverityError = "error"

	EventOrderAccepted       = "order.accepted"
	EventOrderFailed         = "order.failed"
	EventOrderRecovering     = "order.recovering"
	EventOrderRecovered      = "order.recovered"
	EventOrderRecoveryFailed = "order.recovery_failed"
)

type Event struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id,omitempty"`
	CreatedAt     time.Time         `json:"created_at,omitempty"`
	UserID        int64             `json:"user_id"`
	Category      string            `json:"category"`
	EventType     string            `json:"event_type"`
	Severity      string            `json:"severity,omitempty"`
	SourceService string            `json:"source_service,omitempty"`
	AccountID     int64             `json:"account_id,omitempty"`
	StrategyID    int64             `json:"strategy_id,omitempty"`
	SessionID     string            `json:"session_id,omitempty"`
	OrderID       string            `json:"order_id,omitempty"`
	AttemptID     string            `json:"attempt_id,omitempty"`
	Title         string            `json:"title,omitempty"`
	Message       string            `json:"message"`
	DedupeKey     string            `json:"dedupe_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}
