package httpserver

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"github.com/hushine-tech/core-service/internal/venuekeys"
)

type fakeRepo struct {
	repository.Repository
	nextID      int64
	accounts    []domain.Account
	venues      []domain.Venue
	state       domain.OnlineAccountInfo
	venueStates map[int64]domain.OnlineAccountInfo
}

func (f *fakeRepo) CreateAccount(_ context.Context, account domain.Account) (int64, error) {
	if f.nextID == 0 {
		f.nextID = 1
	}
	account.AccountID = f.nextID
	f.nextID++
	f.accounts = append(f.accounts, account)
	return account.AccountID, nil
}

func (f *fakeRepo) CreateVenue(_ context.Context, venue domain.Venue) (domain.Venue, error) {
	if f.nextID == 0 {
		f.nextID = 1
	}
	venue.VenueID = f.nextID
	f.nextID++
	if venue.CreatedAt.IsZero() {
		venue.CreatedAt = time.Now().UTC()
	}
	if venue.UpdatedAt.IsZero() {
		venue.UpdatedAt = time.Now().UTC()
	}
	f.venues = append(f.venues, venue)
	return venue, nil
}

func (f *fakeRepo) UpdateAccountState(_ context.Context, info domain.OnlineAccountInfo) error {
	f.state = info
	return nil
}

func (f *fakeRepo) UpsertVenueWalletState(_ context.Context, venue domain.Venue, info domain.OnlineAccountInfo) error {
	if f.venueStates == nil {
		f.venueStates = map[int64]domain.OnlineAccountInfo{}
	}
	f.venueStates[venue.VenueID] = info
	return nil
}

func (f *fakeRepo) GetVenueWalletState(_ context.Context, venueID int64, _ int64) (domain.OnlineAccountInfo, error) {
	if f.venueStates == nil {
		return domain.OnlineAccountInfo{}, repository.ErrNotFound
	}
	info, ok := f.venueStates[venueID]
	if !ok {
		return domain.OnlineAccountInfo{}, repository.ErrNotFound
	}
	return info, nil
}

func postAccount(t *testing.T, repo *fakeRepo, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/accounts", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	NewHandler(repo).handleAccounts(rec, req)
	return rec
}

func TestCreateAccountRejectsDeprecatedRuntimePayload(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"legacy mode", `{"user_id":7,"name":"legacy","mode":1}`},
		{"credentials", `{"user_id":7,"name":"legacy","environment":1,"api_key":"k","api_secret":"s"}`},
		{"initial balance", `{"user_id":7,"name":"legacy","environment":0,"initial_balance":1000}`},
		{"market modes", `{"user_id":7,"name":"legacy","environment":0,"margin_mode":"cross","position_mode":"one_way"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepo{}
			rec := postAccount(t, repo, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
			if len(repo.accounts) != 0 || len(repo.venues) != 0 {
				t.Fatalf("deprecated payload should not write account or venue: accounts=%+v venues=%+v", repo.accounts, repo.venues)
			}
		})
	}
}

func TestCreateBacktestAccountCreatesDefaultSimulatedVenue(t *testing.T) {
	repo := &fakeRepo{}
	rec := postAccount(t, repo, `{"user_id":7,"name":"bt","environment":0}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.accounts) != 1 {
		t.Fatalf("accounts len = %d, want 1", len(repo.accounts))
	}
	if len(repo.venues) != 1 {
		t.Fatalf("venues len = %d, want 1", len(repo.venues))
	}
	venue := repo.venues[0]
	if venue.AccountID == nil || *venue.AccountID != repo.accounts[0].AccountID {
		t.Fatalf("venue account_id = %v, want %d", venue.AccountID, repo.accounts[0].AccountID)
	}
	if venue.Environment != domain.EnvironmentBacktest || venue.Exchange != domain.ExchangeBinance || venue.Market != domain.MarketPerpetualFutures {
		t.Fatalf("venue route = env:%v exchange:%v market:%v", venue.Environment, venue.Exchange, venue.Market)
	}
	if !venuekeys.IsBacktestAPIKey(venue.APIKey) {
		t.Fatalf("venue api_key = %q, want synthetic key", venue.APIKey)
	}
	if venue.CredentialInfo != "" {
		t.Fatalf("venue credential_info = %q, want empty", venue.CredentialInfo)
	}
	if venue.CredentialKeyVersion != "synthetic" {
		t.Fatalf("venue credential_key_version = %q, want synthetic", venue.CredentialKeyVersion)
	}
	if len(repo.venueStates) != 1 {
		t.Fatalf("venue wallet states len = %d, want 1", len(repo.venueStates))
	}
	state := repo.venueStates[venue.VenueID]
	if state.AccountID != repo.accounts[0].AccountID {
		t.Fatalf("state account_id = %d, want %d", state.AccountID, repo.accounts[0].AccountID)
	}
	if state.Futures.MarginMode != "cross" || state.Futures.PositionMode != "one_way" {
		t.Fatalf("state futures modes = %q/%q, want cross/one_way", state.Futures.MarginMode, state.Futures.PositionMode)
	}
}

func TestCreateBacktestAccountDoesNotPersistWhenSyntheticKeyGenerationFails(t *testing.T) {
	original := newBacktestVenueAPIKey
	newBacktestVenueAPIKey = func() (string, error) {
		return "", errors.New("entropy unavailable")
	}
	t.Cleanup(func() {
		newBacktestVenueAPIKey = original
	})

	repo := &fakeRepo{}
	rec := postAccount(t, repo, `{"user_id":7,"name":"bt","environment":0}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.accounts) != 0 {
		t.Fatalf("accounts len = %d, want 0", len(repo.accounts))
	}
	if len(repo.venues) != 0 {
		t.Fatalf("venues len = %d, want 0", len(repo.venues))
	}
}
