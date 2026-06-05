package httpserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
)

type fakeRepo struct {
	repository.Repository
	nextID      int64
	accounts    []domain.Account
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
			if len(repo.accounts) != 0 {
				t.Fatalf("deprecated payload should not write account: accounts=%+v", repo.accounts)
			}
		})
	}
}

func TestCreateBacktestAccountCreatesPortfolioContextOnly(t *testing.T) {
	repo := &fakeRepo{}
	rec := postAccount(t, repo, `{"user_id":7,"name":"bt","environment":0}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.accounts) != 1 {
		t.Fatalf("accounts len = %d, want 1", len(repo.accounts))
	}
	if repo.state.AccountID != 0 || len(repo.venueStates) != 0 {
		t.Fatalf("account create must not write wallet state: state=%+v venue_states=%+v", repo.state, repo.venueStates)
	}
}

func TestAccountWalletEndpointIsGone(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/accounts/7/wallet", nil)
			rec := httptest.NewRecorder()
			NewHandler(&fakeRepo{}).handleAccountByID(rec, req)
			if rec.Code != http.StatusGone {
				t.Fatalf("status = %d, want 410; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}
