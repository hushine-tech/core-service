package risk

import (
	"context"
	"strings"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

type AdapterCapabilityReader struct {
	registry *adapter.Registry
}

func NewAdapterCapabilityReader(registry *adapter.Registry) AdapterCapabilityReader {
	return AdapterCapabilityReader{registry: registry}
}

func (r AdapterCapabilityReader) ReadOrderCapability(ctx context.Context, route RouteKey) (adapter.OrderCapability, error) {
	provider, err := r.registry.OrderCapabilityProvider(adapter.Route{
		Exchange:    domain.Exchange(route.Exchange),
		Environment: domain.Environment(route.Environment),
		Market:      domain.Market(route.Market),
	})
	if err != nil {
		return adapter.OrderCapability{}, err
	}
	return provider.OrderCapability(ctx, adapter.ParsedCredential{})
}

type AdapterSymbolRulesReader struct {
	registry *adapter.Registry
}

func NewAdapterSymbolRulesReader(registry *adapter.Registry) AdapterSymbolRulesReader {
	return AdapterSymbolRulesReader{registry: registry}
}

func (r AdapterSymbolRulesReader) ReadSymbolRules(ctx context.Context, req SnapshotRequest) ([]FuturesRiskMetadata, error) {
	reader, err := r.registry.SymbolRulesReader(adapter.Route{
		Exchange:    domain.Exchange(req.Exchange),
		Environment: domain.Environment(req.Environment),
		Market:      domain.Market(req.Market),
	})
	if err != nil {
		return nil, err
	}
	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	rules, err := reader.ReadSymbolRules(ctx, adapter.SymbolRulesRequest{Symbols: []string{symbol}})
	if err != nil {
		return nil, err
	}
	out := make([]FuturesRiskMetadata, 0, len(rules.Symbols))
	for _, rule := range rules.Symbols {
		out = append(out, FuturesRiskMetadata{
			Symbol:      strings.ToUpper(strings.TrimSpace(rule.Symbol)),
			MinQty:      rule.MinQty,
			MinNotional: rule.MinNotional,
			StepSize:    rule.StepSize,
			TickSize:    rule.TickSize,
		})
	}
	return out, nil
}

type VenueWalletStateReader interface {
	GetVenueWalletState(ctx context.Context, venueID, userID int64) (domain.OnlineAccountInfo, error)
}

type VenueWalletSnapshotReader struct {
	reader VenueWalletStateReader
}

func NewVenueWalletSnapshotReader(reader VenueWalletStateReader) VenueWalletSnapshotReader {
	return VenueWalletSnapshotReader{reader: reader}
}

func (r VenueWalletSnapshotReader) ReadSnapshot(ctx context.Context, req SnapshotRequest) (Snapshot, error) {
	info, err := r.reader.GetVenueWalletState(ctx, req.VenueID, req.UserID)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotFromOnlineInfo(info), nil
}

func snapshotFromOnlineInfo(info domain.OnlineAccountInfo) Snapshot {
	out := Snapshot{
		AvailableBalance: info.AvailableBalance,
	}
	if out.AvailableBalance == 0 {
		out.AvailableBalance = info.Futures.AvailableBalance
	}
	for _, asset := range info.Spot.Assets {
		out.Balances = append(out.Balances, Balance{
			Asset:     asset.Symbol,
			Available: asset.Qty - asset.Locked,
			Locked:    asset.Locked,
		})
	}
	for _, pos := range info.Futures.Positions {
		out.Positions = append(out.Positions, Position{
			Symbol:       pos.Symbol,
			PositionSide: pos.PositionSide,
			Qty:          pos.PositionQty,
		})
	}
	for _, item := range info.Futures.RiskMetadata {
		out.FuturesRiskMetadata = append(out.FuturesRiskMetadata, FuturesRiskMetadata{
			Symbol:             item.Symbol,
			ConfiguredLeverage: item.ConfiguredLeverage,
			StepSize:           item.StepSize,
			TickSize:           item.TickSize,
		})
	}
	return out
}

type OpenOrderStore interface {
	ListOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error)
}

type OpenOrderPendingReader struct {
	store OpenOrderStore
	limit int
}

func NewOpenOrderPendingReader(store OpenOrderStore) OpenOrderPendingReader {
	return OpenOrderPendingReader{store: store, limit: 500}
}

func (r OpenOrderPendingReader) HasPendingRoute(ctx context.Context, key PendingRouteKey) (bool, error) {
	limit := r.limit
	if limit <= 0 {
		limit = 500
	}
	orders, err := r.store.ListOpenOrders(ctx, limit)
	if err != nil {
		return false, err
	}
	for _, order := range orders {
		if pendingOrderMatches(order, key) {
			return true, nil
		}
	}
	return false, nil
}

func pendingOrderMatches(order lifecycle.OpenOrder, key PendingRouteKey) bool {
	return order.AccountID == key.AccountID &&
		order.VenueID == key.VenueID &&
		order.Environment == key.Environment &&
		order.Exchange == key.Exchange &&
		order.Market == key.Market &&
		order.PositionSide == key.PositionSide &&
		strings.EqualFold(strings.TrimSpace(order.Symbol), strings.TrimSpace(key.Symbol))
}
