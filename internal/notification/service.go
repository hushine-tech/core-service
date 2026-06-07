package notification

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

type Repository interface {
	GetUser(ctx context.Context, userID int64) (domain.User, error)
	GetNotificationSettings(ctx context.Context, userID int64) (domain.NotificationSettings, error)
	UpsertNotificationSettings(ctx context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error)
	GetNotificationChannel(ctx context.Context, userID int64, channel string) (domain.NotificationChannel, error)
	FindNotificationChannelByBindCodeHash(ctx context.Context, codeHash string, at time.Time) (domain.NotificationChannel, error)
	UpsertNotificationBindCode(ctx context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error)
	BindNotificationChannel(ctx context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error)
	RevokeNotificationChannel(ctx context.Context, userID int64, channel string, now time.Time) error
	UpdateNotificationDeliveryStatus(ctx context.Context, userID int64, channel string, status string, errText string, at time.Time) error
	GetNotificationPlan(ctx context.Context, planCode string) (domain.NotificationPlan, error)
}

type TelegramSender interface {
	SendMessage(ctx context.Context, chatID string, text string) error
}

type Config struct {
	BotUsername      string
	BindCodeTTL      time.Duration
	SendTimeout      time.Duration
	CustomRateWindow time.Duration
	DeliveryEnabled  *bool
}

type Service struct {
	repo    Repository
	sender  TelegramSender
	cfg     Config
	now     func() time.Time
	limiter *RateLimiter
}

func NewService(repo Repository, sender TelegramSender, cfg Config, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	if cfg.BindCodeTTL <= 0 {
		cfg.BindCodeTTL = 10 * time.Minute
	}
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = 5 * time.Second
	}
	if cfg.CustomRateWindow <= 0 {
		cfg.CustomRateWindow = time.Minute
	}
	return &Service{
		repo:    repo,
		sender:  sender,
		cfg:     cfg,
		now:     now,
		limiter: NewRateLimiter(now),
	}
}

func (s *Service) BotUsername() string {
	if s == nil {
		return ""
	}
	return s.cfg.BotUsername
}

func (s *Service) DeliverEvent(ctx context.Context, event Event) error {
	if err := ValidateEvent(event); err != nil {
		if event.UserID > 0 {
			_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryInvalidEvent, "invalid notification event", s.now())
		}
		return err
	}
	if !s.deliveryEnabled() {
		_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryDisabled, "notification delivery disabled", s.now())
		return nil
	}
	user, err := s.repo.GetUser(ctx, event.UserID)
	if err != nil {
		return err
	}
	settings, err := s.repo.GetNotificationSettings(ctx, event.UserID)
	if err != nil {
		return err
	}
	plan, err := s.repo.GetNotificationPlan(ctx, user.PlanCode)
	if err != nil {
		return err
	}
	channel, err := s.repo.GetNotificationChannel(ctx, event.UserID, domain.NotificationChannelTelegram)
	if err != nil {
		return err
	}

	if status, ok := s.blockedStatus(event, settings, plan, channel); !ok {
		_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, status, "", s.now())
		return nil
	}
	if event.Category == CategoryCustom {
		limit := plan.CustomRateLimitPerMinute
		if plan.CustomRateLimitBurst > 0 && plan.CustomRateLimitBurst < limit {
			limit = plan.CustomRateLimitBurst
		}
		key := fmt.Sprintf("user:%d", event.UserID)
		if !s.limiter.Allow(key, limit, s.cfg.CustomRateWindow) {
			_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryRateLimited, "", s.now())
			return nil
		}
	}
	if s.sender == nil {
		_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryFailed, "telegram sender is not configured", s.now())
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()
	if err := s.sender.SendMessage(sendCtx, channel.TargetID, formatMessage(event)); err != nil {
		_ = s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryFailed, err.Error(), s.now())
		return nil
	}
	return s.repo.UpdateNotificationDeliveryStatus(ctx, event.UserID, domain.NotificationChannelTelegram, domain.NotificationDeliveryOK, "", s.now())
}

func (s *Service) deliveryEnabled() bool {
	if s == nil || s.cfg.DeliveryEnabled == nil {
		return true
	}
	return *s.cfg.DeliveryEnabled
}

func (s *Service) blockedStatus(event Event, settings domain.NotificationSettings, plan domain.NotificationPlan, channel domain.NotificationChannel) (string, bool) {
	if !plan.NotificationEnabled {
		return domain.NotificationDeliveryPlanDisabled, false
	}
	if !settings.Enabled {
		return domain.NotificationDeliveryDisabled, false
	}
	switch event.Category {
	case CategorySystem:
		if !plan.AllowSystem {
			return domain.NotificationDeliveryPlanDisabled, false
		}
		if !settings.SystemEnabled {
			return domain.NotificationDeliveryDisabled, false
		}
	case CategoryStrategy:
		if !plan.AllowStrategy {
			return domain.NotificationDeliveryPlanDisabled, false
		}
		if !settings.StrategyEnabled {
			return domain.NotificationDeliveryDisabled, false
		}
	case CategoryCustom:
		if !plan.AllowCustom {
			return domain.NotificationDeliveryPlanDisabled, false
		}
		if !settings.CustomEnabled {
			return domain.NotificationDeliveryDisabled, false
		}
	}
	if channel.Status != domain.NotificationChannelStatusBound || strings.TrimSpace(channel.TargetID) == "" {
		return domain.NotificationDeliveryUnbound, false
	}
	return "", true
}

func formatMessage(event Event) string {
	title := strings.TrimSpace(event.Title)
	msg := strings.TrimSpace(event.Message)
	if title == "" {
		return msg
	}
	if msg == "" {
		return title
	}
	return title + "\n" + msg
}

func (s *Service) GetSettings(ctx context.Context, userID int64) (domain.NotificationSettings, domain.NotificationPlan, domain.NotificationChannel, error) {
	user, err := s.repo.GetUser(ctx, userID)
	if err != nil {
		return domain.NotificationSettings{}, domain.NotificationPlan{}, domain.NotificationChannel{}, err
	}
	settings, err := s.repo.GetNotificationSettings(ctx, userID)
	if err != nil {
		return domain.NotificationSettings{}, domain.NotificationPlan{}, domain.NotificationChannel{}, err
	}
	plan, err := s.repo.GetNotificationPlan(ctx, user.PlanCode)
	if err != nil {
		return domain.NotificationSettings{}, domain.NotificationPlan{}, domain.NotificationChannel{}, err
	}
	channel, err := s.repo.GetNotificationChannel(ctx, userID, domain.NotificationChannelTelegram)
	if err != nil {
		return domain.NotificationSettings{}, domain.NotificationPlan{}, domain.NotificationChannel{}, err
	}
	return settings, plan, channel, nil
}

func (s *Service) UpdatePreferences(ctx context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error) {
	if settings.UserID <= 0 {
		return domain.NotificationSettings{}, ErrInvalidEvent
	}
	return s.repo.UpsertNotificationSettings(ctx, settings)
}

func (s *Service) CreateBindCode(ctx context.Context, userID int64) (string, time.Time, error) {
	if userID <= 0 {
		return "", time.Time{}, ErrInvalidEvent
	}
	code, err := randomBindCode()
	if err != nil {
		return "", time.Time{}, err
	}
	expiresAt := s.now().UTC().Add(s.cfg.BindCodeTTL)
	if _, err := s.repo.UpsertNotificationBindCode(ctx, userID, domain.NotificationChannelTelegram, HashBindCode(code), expiresAt); err != nil {
		return "", time.Time{}, err
	}
	return code, expiresAt, nil
}

func (s *Service) ConfirmBinding(ctx context.Context, userID int64) (domain.NotificationChannel, error) {
	return s.repo.GetNotificationChannel(ctx, userID, domain.NotificationChannelTelegram)
}

func (s *Service) HandleTelegramUpdate(ctx context.Context, update TelegramUpdate) error {
	code := strings.TrimSpace(update.Message.Text)
	if code == "" {
		return nil
	}
	pending, err := s.repo.FindNotificationChannelByBindCodeHash(ctx, HashBindCode(code), s.now().UTC())
	if err != nil {
		return nil
	}
	chat := update.Message.Chat
	targetID := fmt.Sprintf("%d", chat.ID)
	targetType := strings.TrimSpace(chat.Type)
	targetLabel := telegramChatLabel(chat)
	_, err = s.repo.BindNotificationChannel(ctx, pending.UserID, pending.Channel, targetID, targetType, targetLabel, s.now().UTC())
	return err
}

func (s *Service) Unbind(ctx context.Context, userID int64) error {
	return s.repo.RevokeNotificationChannel(ctx, userID, domain.NotificationChannelTelegram, s.now().UTC())
}

func (s *Service) SendTest(ctx context.Context, userID int64) error {
	return s.DeliverEvent(ctx, Event{
		SchemaVersion: SchemaVersion,
		UserID:        userID,
		Category:      CategorySystem,
		EventType:     EventTestMessage,
		Severity:      SeverityInfo,
		Message:       "Hushine test notification.",
	})
}

func HashBindCode(code string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(code)))
	return hex.EncodeToString(sum[:])
}

func telegramChatLabel(chat TelegramChat) string {
	if username := strings.TrimSpace(chat.Username); username != "" {
		if strings.HasPrefix(username, "@") {
			return username
		}
		return "@" + username
	}
	name := strings.TrimSpace(strings.TrimSpace(chat.FirstName) + " " + strings.TrimSpace(chat.LastName))
	if name != "" {
		return name
	}
	return strings.TrimSpace(chat.Title)
}

func randomBindCode() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	out := make([]byte, 10)
	for i := range out {
		out[i] = alphabet[int(b[i%len(b)])%len(alphabet)]
	}
	return string(out), nil
}
