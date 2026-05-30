package tests

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type testPortfolioSnapshotFactory struct {
	reader adapter.AccountSnapshotReader
}

func (f testPortfolioSnapshotFactory) CredentialValidator() (adapter.CredentialValidator, error) {
	return testPortfolioCredentialValidator{}, nil
}

func (f testPortfolioSnapshotFactory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return f.reader, nil
}

func (f testPortfolioSnapshotFactory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	return nil, adapter.CapabilityUnsupported("symbol_rules_reader")
}

func (f testPortfolioSnapshotFactory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, adapter.CapabilityUnsupported("order_executor")
}

func (f testPortfolioSnapshotFactory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, adapter.CapabilityUnsupported("order_state_reader")
}

func (f testPortfolioSnapshotFactory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, adapter.CapabilityUnsupported("order_canceller")
}

type testPortfolioCredentialValidator struct{}

func (testPortfolioCredentialValidator) ValidateCredential(_ context.Context, raw json.RawMessage) (adapter.ParsedCredential, error) {
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return adapter.ParsedCredential{}, err
	}
	if payload["api_key"] == "" || payload["api_secret"] == "" {
		return adapter.ParsedCredential{}, errors.New("missing credential")
	}
	return adapter.ParsedCredential{Raw: raw, Metadata: payload}, nil
}

type testPortfolioSnapshotReader struct {
	info domain.OnlineAccountInfo
	seen adapter.PortfolioSnapshotRequest
}

func (r *testPortfolioSnapshotReader) ReadPortfolioSnapshot(_ context.Context, req adapter.PortfolioSnapshotRequest) (adapter.PortfolioSnapshot, error) {
	r.seen = req
	info := r.info
	info.AccountID = req.AccountID
	if info.UpdatedAt.IsZero() {
		info.UpdatedAt = time.Now().UTC()
	}
	return adapter.PortfolioSnapshot{
		UserID:           req.UserID,
		AccountID:        req.AccountID,
		VenueID:          req.VenueID,
		Exchange:         domain.ExchangeBinance,
		Environment:      infoEnvironment(info.Mode),
		Market:           domain.MarketPerpetualFutures,
		TotalValue:       info.TotalValue,
		WalletBalance:    info.WalletBalance,
		AvailableBalance: info.AvailableBalance,
		OnlineInfo:       &info,
		UpdatedAt:        info.UpdatedAt,
	}, nil
}

func newBinancePerpSnapshotRegistry(reader adapter.AccountSnapshotReader, env domain.Environment) *adapter.Registry {
	registry := adapter.NewRegistry()
	registry.Register(adapter.Route{Exchange: domain.ExchangeBinance, Environment: env, Market: domain.MarketPerpetualFutures}, testPortfolioSnapshotFactory{reader: reader})
	return registry
}

func infoEnvironment(mode domain.AccountMode) domain.Environment {
	switch mode {
	case domain.AccountModeBinanceLive:
		return domain.EnvironmentLive
	case domain.AccountModeBinanceTestnet:
		return domain.EnvironmentDemo
	default:
		return domain.EnvironmentBacktest
	}
}
