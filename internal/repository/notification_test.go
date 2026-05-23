package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func notificationTestRepo(t *testing.T) (*TimescaleRepository, context.Context) {
	t.Helper()
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		dsn = "host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable"
	}
	repo, err := NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}
	return repo, context.Background()
}

func createNotificationTestUser(t *testing.T, ctx context.Context, repo *TimescaleRepository) domain.User {
	t.Helper()
	user, err := repo.CreateUser(ctx, domain.User{
		Username:     fmt.Sprintf("notification-test-%d", time.Now().UnixNano()),
		PasswordHash: "test-hash",
		CreatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM users WHERE id = $1`, user.ID)
	})
	return user
}

func TestNotificationSettingsDefaultAndUpsert(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	settings, err := repo.GetNotificationSettings(ctx, user.ID)
	if err != nil {
		t.Fatalf("get default settings: %v", err)
	}
	if settings.UserID != user.ID || !settings.SystemEnabled || !settings.StrategyEnabled || !settings.CustomEnabled {
		t.Fatalf("default settings = %+v, want all categories enabled for user", settings)
	}

	settings.SystemEnabled = false
	settings.CustomEnabled = false
	updated, err := repo.UpsertNotificationSettings(ctx, settings)
	if err != nil {
		t.Fatalf("upsert settings: %v", err)
	}
	if updated.SystemEnabled || !updated.StrategyEnabled || updated.CustomEnabled {
		t.Fatalf("updated settings = %+v, want system/custom disabled and strategy enabled", updated)
	}
}

func TestNotificationBindCodeAndChannelBinding(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	expires := time.Now().UTC().Add(10 * time.Minute)
	pending, err := repo.UpsertNotificationBindCode(ctx, user.ID, "telegram", "hash-one", expires)
	if err != nil {
		t.Fatalf("upsert bind code: %v", err)
	}
	if pending.Status != domain.NotificationChannelStatusPending || pending.BindCodeHash != "hash-one" {
		t.Fatalf("pending channel = %+v, want pending with hash-one", pending)
	}

	pending, err = repo.UpsertNotificationBindCode(ctx, user.ID, "telegram", "hash-two", expires)
	if err != nil {
		t.Fatalf("replace bind code: %v", err)
	}
	if pending.BindCodeHash != "hash-two" {
		t.Fatalf("bind code hash = %q, want replacement hash-two", pending.BindCodeHash)
	}
	found, err := repo.FindNotificationChannelByBindCodeHash(ctx, "hash-two", time.Now().UTC())
	if err != nil {
		t.Fatalf("find bind code: %v", err)
	}
	if found.UserID != user.ID || found.Channel != "telegram" {
		t.Fatalf("found channel = %+v, want user/channel for hash-two", found)
	}

	bound, err := repo.BindNotificationChannel(ctx, user.ID, "telegram", "12345", "private", "@xdy", time.Now().UTC())
	if err != nil {
		t.Fatalf("bind channel: %v", err)
	}
	if bound.Status != domain.NotificationChannelStatusBound || bound.TargetID != "12345" || bound.TargetType != "private" || bound.TargetLabel != "@xdy" {
		t.Fatalf("bound channel = %+v, want generic target fields stored", bound)
	}
	if bound.BindCodeHash != "" || bound.BindCodeExpiresAt != nil || bound.BoundAt == nil {
		t.Fatalf("bound channel bind fields = hash:%q expires:%v bound:%v, want code cleared and bound_at set", bound.BindCodeHash, bound.BindCodeExpiresAt, bound.BoundAt)
	}

	rebinding, err := repo.UpsertNotificationBindCode(ctx, user.ID, "telegram", "hash-rebind", expires)
	if err != nil {
		t.Fatalf("upsert rebind code: %v", err)
	}
	if rebinding.Status != domain.NotificationChannelStatusBound || rebinding.TargetID != "12345" {
		t.Fatalf("rebinding channel = %+v, want existing bound target kept while code is pending", rebinding)
	}
	if rebinding.BindCodeHash != "hash-rebind" || rebinding.BindCodeExpiresAt == nil {
		t.Fatalf("rebinding code fields = hash:%q expires:%v, want new code retained", rebinding.BindCodeHash, rebinding.BindCodeExpiresAt)
	}
	found, err = repo.FindNotificationChannelByBindCodeHash(ctx, "hash-rebind", time.Now().UTC())
	if err != nil {
		t.Fatalf("find rebind code for bound channel: %v", err)
	}
	if found.UserID != user.ID || found.Status != domain.NotificationChannelStatusBound {
		t.Fatalf("found rebind channel = %+v, want existing bound channel", found)
	}
}

func TestNotificationPlanFallbackAndDeliveryStatus(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	plan, err := repo.GetNotificationPlan(ctx, "plan-that-does-not-exist")
	if err != nil {
		t.Fatalf("get fallback plan: %v", err)
	}
	if plan.PlanCode != "free" || plan.NotificationEnabled {
		t.Fatalf("fallback plan = %+v, want disabled free plan", plan)
	}

	if _, err := repo.UpsertNotificationBindCode(ctx, user.ID, "telegram", "hash", time.Now().UTC().Add(10*time.Minute)); err != nil {
		t.Fatalf("upsert bind code: %v", err)
	}
	at := time.Now().UTC()
	if err := repo.UpdateNotificationDeliveryStatus(ctx, user.ID, "telegram", "failed", "telegram timeout", at); err != nil {
		t.Fatalf("update delivery status: %v", err)
	}
	channel, err := repo.GetNotificationChannel(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("get channel: %v", err)
	}
	if channel.LastDeliveryStatus != "failed" || channel.LastDeliveryError != "telegram timeout" || channel.LastDeliveryAt == nil {
		t.Fatalf("channel delivery status = %+v, want failed diagnostic", channel)
	}
	settings, err := repo.GetNotificationSettings(ctx, user.ID)
	if err != nil {
		t.Fatalf("get settings: %v", err)
	}
	if settings.LastDeliveryStatus != "failed" || settings.LastDeliveryError != "telegram timeout" || settings.LastDeliveryAt == nil {
		t.Fatalf("settings delivery status = %+v, want user-level failed diagnostic", settings)
	}
}
