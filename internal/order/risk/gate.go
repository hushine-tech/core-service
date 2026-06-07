package risk

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

type DefaultGateConfig struct {
	CapabilityReader  CapabilityReader
	SnapshotReader    SnapshotReader
	PendingReader     PendingReader
	SymbolRulesReader SymbolRulesReader
	Now               func() time.Time
}

type DefaultGate struct {
	capabilityReader  CapabilityReader
	snapshotReader    SnapshotReader
	pendingReader     PendingReader
	symbolRulesReader SymbolRulesReader
	now               func() time.Time
}

func NewDefaultGate(cfg DefaultGateConfig) *DefaultGate {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &DefaultGate{
		capabilityReader:  cfg.CapabilityReader,
		snapshotReader:    cfg.SnapshotReader,
		pendingReader:     cfg.PendingReader,
		symbolRulesReader: cfg.SymbolRulesReader,
		now:               now,
	}
}

func (g *DefaultGate) Review(ctx context.Context, req ReviewRequest) (Decision, error) {
	reviewedAt := g.reviewedAt()
	if req.AccountID <= 0 || req.VenueID <= 0 {
		return reject(reviewedAt, "INVALID_ROUTE", "account_id and venue_id are required"), nil
	}
	req.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	req.Side = strings.ToUpper(strings.TrimSpace(req.Side))
	req.OrderType = strings.ToUpper(strings.TrimSpace(req.OrderType))
	req.TimeInForce = strings.ToUpper(strings.TrimSpace(req.TimeInForce))
	if req.OrderType == "" {
		if req.Price != nil {
			req.OrderType = "LIMIT"
		} else {
			req.OrderType = "MARKET"
		}
	}
	if req.OrderType == "LIMIT" && req.TimeInForce == "" {
		req.TimeInForce = "GTC"
	}

	if g.pendingReader != nil {
		pending, err := g.pendingReader.HasPendingRoute(ctx, pendingKey(req))
		if err != nil {
			return Decision{}, err
		}
		if pending {
			return reject(reviewedAt, "ROUTE_PENDING_EXECUTION", "route has pending execution"), nil
		}
	}

	if g.capabilityReader != nil {
		capability, err := g.capabilityReader.ReadOrderCapability(ctx, routeKey(req))
		if err != nil {
			return Decision{}, err
		}
		if violation := validateCapability(req, capability); violation.Code != "" {
			return rejectViolation(reviewedAt, violation), nil
		}
	}

	snapshot := Snapshot{}
	if g.snapshotReader != nil {
		var err error
		snapshot, err = g.snapshotReader.ReadSnapshot(ctx, SnapshotRequest{RouteKey: routeKey(req), Symbol: req.Symbol})
		if err != nil {
			return Decision{}, err
		}
	}

	if g.symbolRulesReader != nil && domain.Market(req.Market) == domain.MarketPerpetualFutures {
		rules, err := g.symbolRulesReader.ReadSymbolRules(ctx, SnapshotRequest{RouteKey: routeKey(req), Symbol: req.Symbol})
		if err != nil {
			return Decision{}, err
		}
		snapshot.FuturesRiskMetadata = mergeRiskMetadata(snapshot.FuturesRiskMetadata, rules)
	}

	switch domain.Market(req.Market) {
	case domain.MarketSpot:
		if violation := validateSpotBalance(req, snapshot); violation.Code != "" {
			return rejectViolation(reviewedAt, violation), nil
		}
	case domain.MarketPerpetualFutures:
		if violation := validateFuturesBalance(req, snapshot); violation.Code != "" {
			return rejectViolation(reviewedAt, violation), nil
		}
	}

	return Decision{Status: DecisionAllow, ReviewedAt: reviewedAt}, nil
}

func (g *DefaultGate) reviewedAt() time.Time {
	if g == nil || g.now == nil {
		return time.Now().UTC()
	}
	return g.now().UTC()
}

type AllowGate struct{}

func (AllowGate) Review(_ context.Context, _ ReviewRequest) (Decision, error) {
	return Decision{Status: DecisionAllow, ReviewedAt: time.Now().UTC()}, nil
}

func routeKey(req ReviewRequest) RouteKey {
	return RouteKey{
		AccountID:   req.AccountID,
		VenueID:     req.VenueID,
		UserID:      req.UserID,
		Environment: req.Environment,
		Exchange:    req.Exchange,
		Market:      req.Market,
	}
}

func pendingKey(req ReviewRequest) PendingRouteKey {
	return PendingRouteKey{
		AccountID:    req.AccountID,
		VenueID:      req.VenueID,
		Environment:  req.Environment,
		Exchange:     req.Exchange,
		Market:       req.Market,
		PositionSide: req.PositionSide,
		Symbol:       strings.ToUpper(strings.TrimSpace(req.Symbol)),
	}
}

func reject(reviewedAt time.Time, code, message string) Decision {
	return rejectViolation(reviewedAt, Violation{Code: code, Message: message})
}

func rejectViolation(reviewedAt time.Time, violation Violation) Decision {
	return Decision{
		Status:     DecisionReject,
		ReasonCode: violation.Code,
		Violations: []Violation{violation},
		ReviewedAt: reviewedAt,
	}
}

func priceBasis(req ReviewRequest) float64 {
	if req.Price != nil && *req.Price > 0 {
		return *req.Price
	}
	return req.MarkPrice
}

func orderNotional(req ReviewRequest) float64 {
	price := priceBasis(req)
	if price <= 0 {
		return 0
	}
	return math.Abs(req.Qty) * price
}

func mergeRiskMetadata(primary, secondary []FuturesRiskMetadata) []FuturesRiskMetadata {
	if len(secondary) == 0 {
		return primary
	}
	bySymbol := map[string]FuturesRiskMetadata{}
	for _, item := range primary {
		bySymbol[strings.ToUpper(strings.TrimSpace(item.Symbol))] = item
	}
	for _, item := range secondary {
		key := strings.ToUpper(strings.TrimSpace(item.Symbol))
		merged := bySymbol[key]
		merged.Symbol = firstNonEmptyString(merged.Symbol, item.Symbol)
		if item.StepSize > 0 {
			merged.StepSize = item.StepSize
		}
		if item.TickSize > 0 {
			merged.TickSize = item.TickSize
		}
		if item.MinQty > 0 {
			merged.MinQty = item.MinQty
		}
		if item.MinNotional > 0 {
			merged.MinNotional = item.MinNotional
		}
		bySymbol[key] = merged
	}
	out := make([]FuturesRiskMetadata, 0, len(bySymbol))
	for _, item := range bySymbol {
		out = append(out, item)
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
