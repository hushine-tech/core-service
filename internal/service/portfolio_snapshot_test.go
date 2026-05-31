package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

func testPortfolioCredentialManager(t *testing.T) *credential.Manager {
	t.Helper()
	mgr, err := credential.NewManager("0123456789abcdef0123456789abcdef", "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

type snapshotFactory struct {
	reader adapter.AccountSnapshotReader
}

func (f snapshotFactory) CredentialValidator() (adapter.CredentialValidator, error) {
	return snapshotCredentialValidator{}, nil
}

func (f snapshotFactory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return f.reader, nil
}

func (f snapshotFactory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	return nil, adapter.CapabilityUnsupported("symbol_rules_reader")
}

func (f snapshotFactory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, adapter.CapabilityUnsupported("order_executor")
}

func (f snapshotFactory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}

func (f snapshotFactory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, adapter.CapabilityUnsupported("order_canceller")
}

type snapshotCredentialValidator struct{}

func (snapshotCredentialValidator) ValidateCredential(_ context.Context, raw json.RawMessage) (adapter.ParsedCredential, error) {
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return adapter.ParsedCredential{}, err
	}
	if payload["api_key"] == "" || payload["api_secret"] == "" {
		return adapter.ParsedCredential{}, errors.New("missing credential")
	}
	return adapter.ParsedCredential{Raw: raw, Metadata: payload}, nil
}

type snapshotReader struct {
	calls int
	req   adapter.PortfolioSnapshotRequest
	resp  adapter.PortfolioSnapshot
}

func (r *snapshotReader) ReadPortfolioSnapshot(_ context.Context, req adapter.PortfolioSnapshotRequest) (adapter.PortfolioSnapshot, error) {
	r.calls++
	r.req = req
	if r.resp.UpdatedAt.IsZero() {
		r.resp.UpdatedAt = time.Unix(100, 0).UTC()
	}
	r.resp.UserID = req.UserID
	r.resp.AccountID = req.AccountID
	r.resp.VenueID = req.VenueID
	return r.resp, nil
}

func TestGetPortfolioSnapshotReturnsVenueArray(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{
		Exchange:         domain.ExchangeBinance,
		Environment:      domain.EnvironmentDemo,
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       1000,
		WalletBalance:    900,
		AvailableBalance: 800,
		Balances: []adapter.BalanceEntry{
			{Asset: "USDT", WalletBalance: 900, AvailableBalance: 800, ValueUSDT: 900},
		},
		Positions: []adapter.PositionEntry{
			{Symbol: "ETHUSDT", PositionSide: "BOTH", Qty: 0.2, MarkPrice: 2000},
		},
		OnlineInfo: &domain.OnlineAccountInfo{
			Mode:             domain.AccountModeBinanceTestnet,
			WalletBalance:    900,
			AvailableBalance: 800,
			Futures: domain.FuturesWallet{
				MarginMode:       "cross",
				PositionMode:     "one_way",
				WalletBalance:    900,
				AvailableBalance: 800,
				MarginBalance:    940,
				RiskMetadata: []domain.FuturesRiskMetadata{{
					Symbol:               "ETHUSDT",
					ConfiguredLeverage:   20,
					ConfiguredMarginMode: "cross",
				}},
				Positions: []domain.FuturesPosition{{
					Symbol:        "ETHUSDT",
					PositionSide:  "BOTH",
					PositionQty:   0.2,
					EntryPrice:    1900,
					MarkPrice:     2000,
					Leverage:      20,
					MarginMode:    "cross",
					MarginType:    "cross",
					InitialMargin: 20,
				}},
			},
		},
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	repo := &stubRepo{
		account: domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo, Mode: domain.AccountModeBinanceTestnet},
		venues: []domain.Venue{{
			VenueID:        venueID,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Exchange:       domain.ExchangeBinance,
			Environment:    domain.EnvironmentDemo,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			APIKey:         "k1",
			CredentialInfo: encrypted,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr), WithExchangeRegistry(registry))

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{AccountId: accountID, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() error = %v", err)
	}
	if got := len(resp.GetSnapshot().GetVenues()); got != 1 {
		t.Fatalf("len(venues) = %d, want 1", got)
	}
	venue := resp.GetSnapshot().GetVenues()[0]
	if venue.GetVenueId() != venueID || venue.GetExchange() != int32(domain.ExchangeBinance) || venue.GetBalances()[0].GetAsset() != "USDT" {
		t.Fatalf("venue snapshot = %+v", venue)
	}
	if venue.GetWallet() == nil || venue.GetWallet().GetFutures().GetPositions()[0].GetLeverage() != 20 {
		t.Fatalf("venue wallet futures = %+v, want full canonical futures wallet", venue.GetWallet().GetFutures())
	}
	if venue.GetWallet().GetFutures().GetRiskMetadata()[0].GetConfiguredLeverage() != 20 {
		t.Fatalf("venue wallet risk metadata = %+v, want leverage 20", venue.GetWallet().GetFutures().GetRiskMetadata())
	}
}

func TestBacktestPortfolioSnapshotIncludesBoundSimulatedVenue(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	repo := &stubRepo{
		account: domain.Account{
			AccountID:   accountID,
			UserID:      serviceTestUserID,
			Environment: domain.EnvironmentBacktest,
			Mode:        domain.AccountModeBacktest,
		},
		state: domain.OnlineAccountInfo{
			AccountID:        accountID,
			Mode:             domain.AccountModeBacktest,
			TotalValue:       1500,
			WalletBalance:    1000,
			AvailableBalance: 900,
			Futures: domain.FuturesWallet{
				WalletBalance:    1000,
				AvailableBalance: 900,
				MarginBalance:    1000,
			},
			Spot:      domain.SpotWallet{Free: 500},
			UpdatedAt: time.Unix(100, 0).UTC(),
		},
		venues: []domain.Venue{{
			VenueID:     venueID,
			UserID:      serviceTestUserID,
			AccountID:   &accountID,
			Exchange:    domain.ExchangeBinance,
			Environment: domain.EnvironmentBacktest,
			Market:      domain.MarketPerpetualFutures,
			Status:      domain.VenueStatusActive,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.GetPortfolioSnapshot(context.Background(), &accountv1.GetPortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    serviceTestUserID,
	})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot() error = %v", err)
	}
	venues := resp.GetSnapshot().GetVenues()
	if len(venues) != 1 {
		t.Fatalf("len(venues) = %d, want 1", len(venues))
	}
	venue := venues[0]
	if venue.GetVenueId() != venueID || venue.GetExchange() != int32(domain.ExchangeBinance) || venue.GetMarket() != int32(domain.MarketPerpetualFutures) {
		t.Fatalf("venue snapshot = %+v", venue)
	}
	if venue.GetWalletBalance() != 1000 || venue.GetAvailableBalance() != 900 {
		t.Fatalf("venue balances = wallet %v available %v, want 1000/900", venue.GetWalletBalance(), venue.GetAvailableBalance())
	}
	if venue.GetWallet() == nil || venue.GetWallet().GetMode() != int32(domain.AccountModeBacktest) {
		t.Fatalf("venue wallet = %+v, want backtest account wallet state", venue.GetWallet())
	}
	if venue.GetWallet().GetSpot().GetFree() != 0 || venue.GetWallet().GetSpot().GetLocked() != 0 {
		t.Fatalf("futures venue snapshot must not carry spot wallet: %+v", venue.GetWallet().GetSpot())
	}
}

func TestUpdatePortfolioSnapshotReadsVenueThroughRegistry(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{
		Exchange:         domain.ExchangeBinance,
		Environment:      domain.EnvironmentDemo,
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       1200,
		WalletBalance:    1100,
		AvailableBalance: 1000,
		Balances:         []adapter.BalanceEntry{{Asset: "USDT", WalletBalance: 1100, AvailableBalance: 1000}},
	}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	repo := &stubRepo{
		account: domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo, Mode: domain.AccountModeBinanceTestnet},
		venues: []domain.Venue{{
			VenueID:        venueID,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Exchange:       domain.ExchangeBinance,
			Environment:    domain.EnvironmentDemo,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			APIKey:         "k1",
			CredentialInfo: encrypted,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
		}},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr), WithExchangeRegistry(registry))

	resp, err := svc.UpdatePortfolioSnapshot(context.Background(), &accountv1.UpdatePortfolioSnapshotRequest{AccountId: accountID, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("UpdatePortfolioSnapshot() error = %v", err)
	}
	if reader.calls != 1 {
		t.Fatalf("snapshot reader calls = %d, want 1", reader.calls)
	}
	if reader.req.VenueID != venueID {
		t.Fatalf("reader venue_id = %d, want %d", reader.req.VenueID, venueID)
	}
	if repo.state.TotalValue != 1200 || resp.GetSnapshot().GetTotalValue() != 1200 {
		t.Fatalf("state=%+v snapshot=%+v, want total value persisted", repo.state, resp.GetSnapshot())
	}
}

func TestOldOnlineAccountInfoMainPathIsRemoved(t *testing.T) {
	accountID := int64(15)
	venueID := int64(88)
	mgr := testPortfolioCredentialManager(t)
	encrypted, err := mgr.Encrypt(`{"api_secret":"s1"}`)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	reader := &snapshotReader{resp: adapter.PortfolioSnapshot{TotalValue: 1300, WalletBalance: 1200, AvailableBalance: 1100}}
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: domain.EnvironmentDemo, Market: domain.MarketPerpetualFutures}, snapshotFactory{reader: reader})
	repo := &stubRepo{
		account: domain.Account{AccountID: accountID, UserID: serviceTestUserID, Environment: domain.EnvironmentDemo, Mode: domain.AccountModeBinanceTestnet},
		venues: []domain.Venue{{
			VenueID:        venueID,
			UserID:         serviceTestUserID,
			AccountID:      &accountID,
			Exchange:       domain.ExchangeBinance,
			Environment:    domain.EnvironmentDemo,
			Market:         domain.MarketPerpetualFutures,
			Status:         domain.VenueStatusActive,
			APIKey:         "k1",
			CredentialInfo: encrypted,
			MarginMode:     domain.MarginModeCross,
			PositionMode:   domain.PositionModeOneWay,
		}},
	}
	// Router is deliberately nil. The legacy RPC must be a compatibility
	// wrapper over the portfolio snapshot path, not the old mode router path.
	svc := NewAccountGRPCService(repo, nil, nil, nil, WithCredentialManager(mgr), WithExchangeRegistry(registry))

	resp, err := svc.GetOnlineAccountInfo(context.Background(), &accountv1.GetOnlineAccountInfoRequest{AccountId: accountID, UserId: serviceTestUserID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo() error = %v", err)
	}
	if resp.GetWallet().GetTotalValue() != 1300 {
		t.Fatalf("wallet total = %v, want 1300", resp.GetWallet().GetTotalValue())
	}
}
