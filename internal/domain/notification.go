package domain

import "time"

const (
	NotificationChannelTelegram = "telegram"

	NotificationChannelStatusUnbound = "unbound"
	NotificationChannelStatusPending = "pending"
	NotificationChannelStatusBound   = "bound"
	NotificationChannelStatusRevoked = "revoked"

	NotificationDeliveryOK           = "ok"
	NotificationDeliveryFailed       = "failed"
	NotificationDeliveryUnbound      = "unbound"
	NotificationDeliveryDisabled     = "disabled"
	NotificationDeliveryPlanDisabled = "plan_disabled"
	NotificationDeliveryRateLimited  = "rate_limited"
	NotificationDeliveryInvalidEvent = "invalid_event"
)

type NotificationSettings struct {
	UserID             int64
	SystemEnabled      bool
	StrategyEnabled    bool
	CustomEnabled      bool
	LastDeliveryStatus string
	LastDeliveryError  string
	LastDeliveryAt     *time.Time
	LastTestMessageAt  *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type NotificationChannel struct {
	ID                 int64
	UserID             int64
	Channel            string
	Status             string
	TargetID           string
	TargetType         string
	TargetLabel        string
	BindCodeHash       string
	BindCodeExpiresAt  *time.Time
	BoundAt            *time.Time
	RevokedAt          *time.Time
	LastDeliveryStatus string
	LastDeliveryError  string
	LastDeliveryAt     *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type NotificationPlan struct {
	PlanCode                 string
	NotificationEnabled      bool
	AllowSystem              bool
	AllowStrategy            bool
	AllowCustom              bool
	CustomRateLimitPerMinute int
	CustomRateLimitBurst     int
	UpdatedAt                time.Time
}
