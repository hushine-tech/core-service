package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
)

type fakeRepo struct {
	user      domain.User
	settings  domain.NotificationSettings
	channel   domain.NotificationChannel
	plan      domain.NotificationPlan
	status    string
	statusErr string
}

func (f *fakeRepo) GetUser(_ context.Context, userID int64) (domain.User, error) {
	if f.user.ID == userID {
		return f.user, nil
	}
	return domain.User{}, repository.ErrNotFound
}

func (f *fakeRepo) GetNotificationSettings(_ context.Context, userID int64) (domain.NotificationSettings, error) {
	f.settings.UserID = userID
	return f.settings, nil
}

func (f *fakeRepo) UpsertNotificationSettings(_ context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error) {
	f.settings = settings
	return settings, nil
}

func (f *fakeRepo) GetNotificationChannel(_ context.Context, userID int64, channel string) (domain.NotificationChannel, error) {
	f.channel.UserID = userID
	f.channel.Channel = channel
	return f.channel, nil
}

func (f *fakeRepo) FindNotificationChannelByBindCodeHash(_ context.Context, codeHash string, _ time.Time) (domain.NotificationChannel, error) {
	if f.channel.BindCodeHash == codeHash && f.channel.Status == domain.NotificationChannelStatusPending {
		return f.channel, nil
	}
	return domain.NotificationChannel{}, repository.ErrNotFound
}

func (f *fakeRepo) UpsertNotificationBindCode(_ context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error) {
	f.channel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusPending, BindCodeHash: codeHash, BindCodeExpiresAt: &expiresAt}
	return f.channel, nil
}

func (f *fakeRepo) BindNotificationChannel(_ context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error) {
	f.channel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusBound, TargetID: targetID, TargetType: targetType, TargetLabel: targetLabel, BoundAt: &now}
	return f.channel, nil
}

func (f *fakeRepo) RevokeNotificationChannel(_ context.Context, userID int64, channel string, now time.Time) error {
	f.channel = domain.NotificationChannel{UserID: userID, Channel: channel, Status: domain.NotificationChannelStatusRevoked, RevokedAt: &now}
	return nil
}

func (f *fakeRepo) UpdateNotificationDeliveryStatus(_ context.Context, _ int64, _ string, status string, errText string, _ time.Time) error {
	f.status = status
	f.statusErr = errText
	return nil
}

func (f *fakeRepo) GetNotificationPlan(_ context.Context, planCode string) (domain.NotificationPlan, error) {
	f.plan.PlanCode = planCode
	return f.plan, nil
}

type fakeSender struct {
	sent []string
	err  error
}

func (f *fakeSender) SendMessage(_ context.Context, chatID string, text string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, chatID+":"+text)
	return nil
}

func TestParseEventRejectsInvalidSchemaAndMissingUser(t *testing.T) {
	if _, err := ParseEvent([]byte(`{"schema_version":2,"user_id":1,"category":"system","event_type":"runtime.started","message":"x"}`)); err == nil {
		t.Fatalf("ParseEvent accepted unsupported schema")
	}
	if _, err := ParseEvent([]byte(`{"schema_version":1,"category":"system","event_type":"runtime.started","message":"x"}`)); err == nil {
		t.Fatalf("ParseEvent accepted missing user_id")
	}
}

func TestServiceDeliverEventSendsBoundAllowedTelegram(t *testing.T) {
	repo := &fakeRepo{
		user:     domain.User{ID: 42, PlanCode: "pro"},
		settings: domain.NotificationSettings{UserID: 42, Enabled: true, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true},
		channel:  domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true},
	}
	sender := &fakeSender{}
	svc := NewService(repo, sender, Config{BotUsername: "hushine_bot"}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.DeliverEvent(context.Background(), Event{
		SchemaVersion: 1,
		UserID:        42,
		Category:      CategorySystem,
		EventType:     EventRuntimeStarted,
		Severity:      SeverityInfo,
		Message:       "Runtime started",
	})
	if err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if len(sender.sent) != 1 || sender.sent[0] != "chat-1:Runtime started" {
		t.Fatalf("sent = %#v, want one Telegram message", sender.sent)
	}
	if repo.status != domain.NotificationDeliveryOK || repo.statusErr != "" {
		t.Fatalf("status = %q/%q, want ok", repo.status, repo.statusErr)
	}
}

func TestServiceDeliverEventSkipsSendWhenDeliveryDisabled(t *testing.T) {
	repo := &fakeRepo{}
	sender := &fakeSender{}
	enabled := false
	svc := NewService(repo, sender, Config{
		BotUsername:     "hushine_bot",
		DeliveryEnabled: &enabled,
	}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.DeliverEvent(context.Background(), Event{
		SchemaVersion: 1,
		UserID:        42,
		Category:      CategoryStrategy,
		EventType:     EventOrderAccepted,
		Severity:      SeverityInfo,
		Message:       "Order accepted",
	})
	if err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("sent = %#v, want no message while delivery disabled", sender.sent)
	}
	if repo.status != domain.NotificationDeliveryDisabled {
		t.Fatalf("status = %q, want disabled", repo.status)
	}
}

func TestServiceDeliverEventSkipsSendWhenUserDisabled(t *testing.T) {
	repo := &fakeRepo{
		user: domain.User{ID: 42, PlanCode: "pro"},
		settings: domain.NotificationSettings{
			UserID:          42,
			Enabled:         false,
			SystemEnabled:   true,
			StrategyEnabled: true,
			CustomEnabled:   true,
		},
		channel: domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:    domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true},
	}
	sender := &fakeSender{}
	svc := NewService(repo, sender, Config{BotUsername: "hushine_bot"}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.DeliverEvent(context.Background(), Event{
		SchemaVersion: 1,
		UserID:        42,
		Category:      CategoryStrategy,
		EventType:     EventOrderAccepted,
		Severity:      SeverityInfo,
		Message:       "Order accepted",
	})
	if err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("sent = %#v, want no message while user notifications disabled", sender.sent)
	}
	if repo.status != domain.NotificationDeliveryDisabled {
		t.Fatalf("status = %q, want disabled", repo.status)
	}
}

func TestServiceDeliverEventRecordsDisabledWithoutSending(t *testing.T) {
	repo := &fakeRepo{
		user:     domain.User{ID: 42, PlanCode: "developer"},
		settings: domain.NotificationSettings{UserID: 42, Enabled: true, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: false},
		channel:  domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:     domain.NotificationPlan{PlanCode: "developer", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: false},
	}
	sender := &fakeSender{}
	svc := NewService(repo, sender, Config{BotUsername: "hushine_bot"}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.DeliverEvent(context.Background(), Event{
		SchemaVersion: 1,
		UserID:        42,
		Category:      CategoryCustom,
		EventType:     EventCustomInfo,
		Severity:      SeverityInfo,
		Message:       "custom",
	})
	if err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("sent = %#v, want no message", sender.sent)
	}
	if repo.status != domain.NotificationDeliveryPlanDisabled {
		t.Fatalf("status = %q, want plan_disabled", repo.status)
	}
}

func TestServiceDeliverEventRecordsTelegramFailureWithoutReturningError(t *testing.T) {
	repo := &fakeRepo{
		user:     domain.User{ID: 42, PlanCode: "pro"},
		settings: domain.NotificationSettings{UserID: 42, Enabled: true, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true},
		channel:  domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true},
	}
	svc := NewService(repo, &fakeSender{err: errors.New("telegram timeout")}, Config{}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.DeliverEvent(context.Background(), Event{
		SchemaVersion: 1,
		UserID:        42,
		Category:      CategoryStrategy,
		EventType:     EventOrderFailed,
		Severity:      SeverityError,
		Message:       "Order failed",
	})
	if err != nil {
		t.Fatalf("deliver event returned error: %v", err)
	}
	if repo.status != domain.NotificationDeliveryFailed || repo.statusErr != "telegram timeout" {
		t.Fatalf("status = %q/%q, want failed diagnostic", repo.status, repo.statusErr)
	}
}

func TestHandleTelegramUpdateBindsPendingCode(t *testing.T) {
	repo := &fakeRepo{
		channel: domain.NotificationChannel{
			UserID:       42,
			Channel:      domain.NotificationChannelTelegram,
			Status:       domain.NotificationChannelStatusPending,
			BindCodeHash: HashBindCode("ABC123"),
		},
	}
	svc := NewService(repo, &fakeSender{}, Config{}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := svc.HandleTelegramUpdate(context.Background(), TelegramUpdate{
		Message: TelegramMessage{
			Text: "ABC123",
			Chat: TelegramChat{
				ID:        12345,
				Type:      "private",
				Username:  "xdy",
				FirstName: "X",
				LastName:  "DY",
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleTelegramUpdate: %v", err)
	}
	if repo.channel.Status != domain.NotificationChannelStatusBound || repo.channel.TargetID != "12345" || repo.channel.TargetLabel != "@xdy" {
		t.Fatalf("channel = %+v, want bound Telegram target", repo.channel)
	}
}
