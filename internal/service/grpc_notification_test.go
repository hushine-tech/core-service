package service

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/notification"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serviceNotificationSender struct {
	sent []string
}

func (s *serviceNotificationSender) SendMessage(_ context.Context, chatID string, text string) error {
	s.sent = append(s.sent, chatID+":"+text)
	return nil
}

func TestGetNotificationSettings(t *testing.T) {
	repo := &stubRepo{
		users: map[string]domain.User{
			"alice": {ID: 7, Username: "alice", PlanCode: "pro", CreatedAt: time.Now().UTC()},
		},
		notificationSettings: domain.NotificationSettings{UserID: 7, SystemEnabled: true, StrategyEnabled: false, CustomEnabled: true},
		notificationChannel:  domain.NotificationChannel{UserID: 7, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetLabel: "@alice"},
		notificationPlan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true, CustomRateLimitPerMinute: 30},
	}
	notifier := notification.NewService(repo, &serviceNotificationSender{}, notification.Config{BotUsername: "hushine_bot"}, time.Now)
	svc := NewAccountGRPCService(repo, nil, nil, nil, notifier)

	resp, err := svc.GetNotificationSettings(context.Background(), &accountv1.GetNotificationSettingsRequest{UserId: 7})
	if err != nil {
		t.Fatalf("GetNotificationSettings: %v", err)
	}
	if resp.GetBotUsername() != "hushine_bot" {
		t.Fatalf("bot username = %q, want hushine_bot", resp.GetBotUsername())
	}
	if resp.GetPreferences().GetStrategyEnabled() {
		t.Fatalf("strategy_enabled = true, want false")
	}
	if resp.GetTelegram().GetStatus() != domain.NotificationChannelStatusBound {
		t.Fatalf("telegram status = %q, want bound", resp.GetTelegram().GetStatus())
	}
	if !resp.GetPlan().GetAllowCustom() {
		t.Fatalf("plan allow_custom = false, want true")
	}
}

func TestSendTestNotificationUsesNotifier(t *testing.T) {
	sender := &serviceNotificationSender{}
	repo := &stubRepo{
		users: map[string]domain.User{
			"alice": {ID: 7, Username: "alice", PlanCode: "pro", CreatedAt: time.Now().UTC()},
		},
		notificationSettings: domain.NotificationSettings{UserID: 7, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true},
		notificationChannel:  domain.NotificationChannel{UserID: 7, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		notificationPlan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true, CustomRateLimitPerMinute: 30},
	}
	notifier := notification.NewService(repo, sender, notification.Config{BotUsername: "hushine_bot"}, func() time.Time { return time.Unix(100, 0).UTC() })
	svc := NewAccountGRPCService(repo, nil, nil, nil, notifier)

	resp, err := svc.SendTestNotification(context.Background(), &accountv1.SendTestNotificationRequest{UserId: 7})
	if err != nil {
		t.Fatalf("SendTestNotification: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("accepted = false, want true")
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent = %#v, want one test message", sender.sent)
	}
	if repo.deliveryStatus != domain.NotificationDeliveryOK {
		t.Fatalf("delivery status = %q, want ok", repo.deliveryStatus)
	}
}

func TestNotificationRPCRequiresConfiguredService(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)
	_, err := svc.GetNotificationSettings(context.Background(), &accountv1.GetNotificationSettingsRequest{UserId: 7})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
	}
}
