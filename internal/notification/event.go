package notification

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const (
	SchemaVersion = 1

	CategorySystem   = "system"
	CategoryStrategy = "strategy"
	CategoryCustom   = "custom"

	SeverityInfo  = "info"
	SeverityWarn  = "warn"
	SeverityError = "error"

	EventRuntimeStarted            = "runtime.started"
	EventRuntimeEnded              = "runtime.ended"
	EventRuntimeUnhealthy          = "runtime.unhealthy"
	EventRuntimeRecovered          = "runtime.recovered"
	EventRuntimeHeartbeatLost      = "runtime.heartbeat_lost"
	EventRuntimeHeartbeatRecovered = "runtime.heartbeat_recovered"
	EventSessionStarted            = "session.started"
	EventSessionStopped            = "session.stopped"
	EventSessionFailed             = "session.failed"
	EventOrderAccepted             = "order.accepted"
	EventOrderFailed               = "order.failed"
	EventOrderRecovering           = "order.recovering"
	EventOrderRecovered            = "order.recovered"
	EventOrderRecoveryFailed       = "order.recovery_failed"
	EventCustomInfo                = "custom.info"
	EventCustomWarn                = "custom.warn"
	EventCustomError               = "custom.error"
	EventTestMessage               = "test.message"
)

var (
	ErrInvalidEvent = errors.New("invalid notification event")

	knownCategories = map[string]bool{
		CategorySystem:   true,
		CategoryStrategy: true,
		CategoryCustom:   true,
	}
	knownSeverities = map[string]bool{
		SeverityInfo:  true,
		SeverityWarn:  true,
		SeverityError: true,
	}
	knownEventTypes = map[string]bool{
		EventRuntimeStarted:            true,
		EventRuntimeEnded:              true,
		EventRuntimeUnhealthy:          true,
		EventRuntimeRecovered:          true,
		EventRuntimeHeartbeatLost:      true,
		EventRuntimeHeartbeatRecovered: true,
		EventSessionStarted:            true,
		EventSessionStopped:            true,
		EventSessionFailed:             true,
		EventOrderAccepted:             true,
		EventOrderFailed:               true,
		EventOrderRecovering:           true,
		EventOrderRecovered:            true,
		EventOrderRecoveryFailed:       true,
		EventCustomInfo:                true,
		EventCustomWarn:                true,
		EventCustomError:               true,
		EventTestMessage:               true,
	}
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
	RuntimeID     string            `json:"runtime_id,omitempty"`
	RuntimeName   string            `json:"runtime_name,omitempty"`
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

func ParseEvent(raw []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return Event{}, err
	}
	if err := ValidateEvent(event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func ValidateEvent(event Event) error {
	if event.SchemaVersion != SchemaVersion {
		return ErrInvalidEvent
	}
	if event.UserID <= 0 {
		return ErrInvalidEvent
	}
	event.Category = strings.TrimSpace(event.Category)
	if !knownCategories[event.Category] {
		return ErrInvalidEvent
	}
	event.EventType = strings.TrimSpace(event.EventType)
	if !knownEventTypes[event.EventType] {
		return ErrInvalidEvent
	}
	severity := strings.TrimSpace(event.Severity)
	if severity != "" && !knownSeverities[severity] {
		return ErrInvalidEvent
	}
	if strings.TrimSpace(event.Message) == "" {
		return ErrInvalidEvent
	}
	return nil
}
