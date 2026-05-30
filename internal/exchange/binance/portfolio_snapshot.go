package binance

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type onlineInfoFetcher interface {
	FetchOnlineAccountInfo(ctx context.Context, account domain.Account) (domain.OnlineAccountInfo, error)
}

type portfolioSnapshotReader struct {
	route  adapter.Route
	client onlineInfoFetcher
}

func (r portfolioSnapshotReader) ReadPortfolioSnapshot(ctx context.Context, req adapter.PortfolioSnapshotRequest) (adapter.PortfolioSnapshot, error) {
	info, err := r.client.FetchOnlineAccountInfo(ctx, domain.Account{
		AccountID:   req.AccountID,
		UserID:      req.UserID,
		Environment: r.route.Environment,
		APIKey:      req.Credential.Metadata["api_key"],
		APISecret:   req.Credential.Metadata["api_secret"],
	})
	if err != nil {
		return adapter.PortfolioSnapshot{}, err
	}
	raw, err := json.Marshal(info)
	if err != nil {
		return adapter.PortfolioSnapshot{}, err
	}
	return adapter.PortfolioSnapshot{
		UserID:           req.UserID,
		AccountID:        req.AccountID,
		VenueID:          req.VenueID,
		Exchange:         r.route.Exchange,
		Environment:      r.route.Environment,
		Market:           r.route.Market,
		TotalValue:       info.TotalValue,
		WalletBalance:    info.WalletBalance,
		AvailableBalance: info.AvailableBalance,
		Balances:         balancesFromOnlineInfo(info),
		Positions:        positionsFromOnlineInfo(info),
		OnlineInfo:       &info,
		UpdatedAt:        info.UpdatedAt,
		RawPayload:       raw,
	}, nil
}

type backtestPortfolioSnapshotReader struct{}

func (backtestPortfolioSnapshotReader) ReadPortfolioSnapshot(_ context.Context, req adapter.PortfolioSnapshotRequest) (adapter.PortfolioSnapshot, error) {
	return adapter.PortfolioSnapshot{
		UserID:    req.UserID,
		AccountID: req.AccountID,
		VenueID:   req.VenueID,
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func balancesFromOnlineInfo(info domain.OnlineAccountInfo) []adapter.BalanceEntry {
	balances := []adapter.BalanceEntry{
		{
			Asset:            "USDT",
			WalletBalance:    info.Futures.WalletBalance,
			AvailableBalance: info.Futures.AvailableBalance,
			ValueUSDT:        info.Futures.MarginBalance,
		},
	}
	for _, asset := range info.Spot.Assets {
		value := 0.0
		if asset.Price != nil {
			value = asset.Qty * *asset.Price
		}
		balances = append(balances, adapter.BalanceEntry{
			Asset:            asset.Symbol,
			WalletBalance:    asset.Qty,
			AvailableBalance: asset.Qty - asset.Locked,
			Locked:           asset.Locked,
			ValueUSDT:        value,
		})
	}
	return balances
}

func positionsFromOnlineInfo(info domain.OnlineAccountInfo) []adapter.PositionEntry {
	positions := make([]adapter.PositionEntry, 0, len(info.Futures.Positions))
	for _, position := range info.Futures.Positions {
		positions = append(positions, adapter.PositionEntry{
			Symbol:           position.Symbol,
			PositionSide:     position.PositionSide,
			Qty:              position.PositionQty,
			EntryPrice:       position.EntryPrice,
			MarkPrice:        position.MarkPrice,
			UnrealizedPnl:    position.UnrealizedPnl,
			LiquidationPrice: position.LiquidationPrice,
		})
	}
	return positions
}
