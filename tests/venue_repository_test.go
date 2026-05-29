package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
)

func venueTestRepo(t *testing.T) (*repository.TimescaleRepository, context.Context) {
	t.Helper()
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		t.Skip("skip: TIMESCALEDB_DSN is required for destructive venue repository migration tests")
	}
	repo, err := repository.NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}
	return repo, context.Background()
}

func createVenueTestUser(t *testing.T, ctx context.Context, repo *repository.TimescaleRepository) domain.User {
	t.Helper()
	user, err := repo.CreateUser(ctx, domain.User{
		Username:     fmt.Sprintf("venue-test-%d", time.Now().UnixNano()),
		PasswordHash: "test-hash",
		CreatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	return user
}

func createVenueTestAccount(t *testing.T, ctx context.Context, repo *repository.TimescaleRepository, userID int64, env domain.Environment) int64 {
	t.Helper()
	id, err := repo.CreateAccount(ctx, domain.Account{
		UserID:      userID,
		Name:        fmt.Sprintf("venue-account-%d", time.Now().UnixNano()),
		Environment: env,
		Status:      domain.AccountStatusActive,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return id
}

func createVenueFixture(t *testing.T, ctx context.Context, repo *repository.TimescaleRepository, userID int64, env domain.Environment) domain.Venue {
	t.Helper()
	venue, err := repo.CreateVenue(ctx, domain.Venue{
		UserID:                userID,
		Exchange:              domain.ExchangeBinance,
		Market:                domain.MarketPerpetualFutures,
		Environment:           env,
		Status:                domain.VenueStatusActive,
		DisplayName:           fmt.Sprintf("binance-testnet-%d", time.Now().UnixNano()),
		Description:           "repository venue fixture",
		APIKey:                fmt.Sprintf("encrypted-key-%d", time.Now().UnixNano()),
		CredentialInfo:        "encrypted-secret",
		CredentialKeyVersion:  "v1",
		CredentialFingerprint: fmt.Sprintf("fp-%d", time.Now().UnixNano()),
		MarginMode:            domain.MarginModeCross,
		PositionMode:          domain.PositionModeOneWay,
	})
	if err != nil {
		t.Fatalf("create venue: %v", err)
	}
	return venue
}

func TestVenueRepositoryCreateGetList(t *testing.T) {
	repo, ctx := venueTestRepo(t)
	user := createVenueTestUser(t, ctx, repo)
	accountID := createVenueTestAccount(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	venue := createVenueFixture(t, ctx, repo, user.ID, domain.EnvironmentDemo)

	got, err := repo.GetVenue(ctx, venue.VenueID, user.ID)
	if err != nil {
		t.Fatalf("get venue: %v", err)
	}
	if got.VenueID != venue.VenueID || got.UserID != user.ID || got.Environment != domain.EnvironmentDemo {
		t.Fatalf("venue = %+v, want created venue for user/env", got)
	}

	bound, err := repo.BindVenue(ctx, user.ID, accountID, venue.VenueID, "test bind")
	if err != nil {
		t.Fatalf("bind venue: %v", err)
	}
	if bound.AccountID == nil || *bound.AccountID != accountID {
		t.Fatalf("bound venue account_id = %v, want %d", bound.AccountID, accountID)
	}

	items, meta, err := repo.ListVenues(ctx, user.ID, accountID, false, false, 20, 0)
	if err != nil {
		t.Fatalf("list venues: %v", err)
	}
	if meta.Total == 0 || len(items) == 0 {
		t.Fatalf("list venues returned items=%d meta=%+v, want bound venue", len(items), meta)
	}
}

func TestVenueRepositoryBindRejectsEnvironmentMismatch(t *testing.T) {
	repo, ctx := venueTestRepo(t)
	user := createVenueTestUser(t, ctx, repo)
	accountID := createVenueTestAccount(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	venue := createVenueFixture(t, ctx, repo, user.ID, domain.EnvironmentLive)

	if _, err := repo.BindVenue(ctx, user.ID, accountID, venue.VenueID, "wrong env"); err == nil {
		t.Fatal("BindVenue environment mismatch = nil, want error")
	}
}

func TestVenueRepositoryBindRejectsActiveSessionAccount(t *testing.T) {
	repo, ctx := venueTestRepo(t)
	user := createVenueTestUser(t, ctx, repo)
	accountID := createVenueTestAccount(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	venue := createVenueFixture(t, ctx, repo, user.ID, domain.EnvironmentDemo)

	if err := repo.SaveSession(ctx, domain.StrategySession{
		SessionID:   fmt.Sprintf("venue-active-%d", time.Now().UnixNano()),
		AccountID:   accountID,
		UserID:      user.ID,
		Environment: domain.EnvironmentDemo,
		Status:      "running",
		StartedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save active session: %v", err)
	}

	if _, err := repo.BindVenue(ctx, user.ID, accountID, venue.VenueID, "active session"); err != repository.ErrConflict {
		t.Fatalf("BindVenue active session err = %v, want ErrConflict", err)
	}
}

func TestVenueRepositoryReleaseClearsAccountID(t *testing.T) {
	repo, ctx := venueTestRepo(t)
	user := createVenueTestUser(t, ctx, repo)
	accountID := createVenueTestAccount(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	venue := createVenueFixture(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	if _, err := repo.BindVenue(ctx, user.ID, accountID, venue.VenueID, "bind before release"); err != nil {
		t.Fatalf("bind venue: %v", err)
	}

	released, err := repo.ReleaseVenue(ctx, user.ID, venue.VenueID, "release")
	if err != nil {
		t.Fatalf("release venue: %v", err)
	}
	if released.AccountID != nil {
		t.Fatalf("released account_id = %v, want nil", *released.AccountID)
	}
}

func TestVenueRepositoryResolveRouteMeta(t *testing.T) {
	repo, ctx := venueTestRepo(t)
	user := createVenueTestUser(t, ctx, repo)
	accountID := createVenueTestAccount(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	venue := createVenueFixture(t, ctx, repo, user.ID, domain.EnvironmentDemo)
	if _, err := repo.BindVenue(ctx, user.ID, accountID, venue.VenueID, "route bind"); err != nil {
		t.Fatalf("bind venue: %v", err)
	}

	meta, err := repo.ResolveVenueRouteMeta(ctx, accountID, domain.ExchangeBinance, domain.MarketPerpetualFutures)
	if err != nil {
		t.Fatalf("resolve route meta: %v", err)
	}
	if meta.VenueID != venue.VenueID || meta.AccountID != accountID || meta.APIKey != venue.APIKey {
		t.Fatalf("route meta = %+v, want bound venue/account/API key", meta)
	}
	if meta.MarginMode != domain.MarginModeCross || meta.PositionMode != domain.PositionModeOneWay {
		t.Fatalf("route modes = %d/%d, want cross/one_way", meta.MarginMode, meta.PositionMode)
	}
}
