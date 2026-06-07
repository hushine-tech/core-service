package risk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type staticCapabilityReader struct {
	capability adapter.OrderCapability
}

func (r staticCapabilityReader) ReadOrderCapability(context.Context, RouteKey) (adapter.OrderCapability, error) {
	return r.capability, nil
}

type staticSnapshotReader struct {
	snapshot Snapshot
}

func (r staticSnapshotReader) ReadSnapshot(context.Context, SnapshotRequest) (Snapshot, error) {
	return r.snapshot, nil
}

type staticPendingReader struct {
	pending bool
}

func (r staticPendingReader) HasPendingRoute(context.Context, PendingRouteKey) (bool, error) {
	return r.pending, nil
}

type staticSymbolRulesReader struct {
	rules []FuturesRiskMetadata
}

func (r staticSymbolRulesReader) ReadSymbolRules(context.Context, SnapshotRequest) ([]FuturesRiskMetadata, error) {
	return r.rules, nil
}

type failingSymbolRulesReader struct{}

func (failingSymbolRulesReader) ReadSymbolRules(context.Context, SnapshotRequest) ([]FuturesRiskMetadata, error) {
	return nil, errors.New("symbol rules should not be called")
}

func TestDefaultGateReview(t *testing.T) {
	baseReq := ReviewRequest{
		AccountID:    1,
		VenueID:      10,
		UserID:       77,
		Environment:  int32(domain.EnvironmentDemo),
		Exchange:     int32(domain.ExchangeBinance),
		Market:       int32(domain.MarketPerpetualFutures),
		PositionSide: 0,
		Symbol:       "ETHUSDT",
		Side:         "BUY",
		Qty:          0.2,
		MarkPrice:    2500,
		OrderType:    "LIMIT",
		TimeInForce:  "GTC",
	}
	price := 2499.0
	baseReq.Price = &price

	baseCapability := adapter.OrderCapability{
		Market:             domain.MarketPerpetualFutures,
		OrderTypes:         []string{"MARKET", "LIMIT"},
		TimeInForce:        []string{"GTC", "IOC", "FOK", "GTD"},
		SupportsPostOnly:   true,
		SupportsGTD:        true,
		SupportsReduceOnly: true,
	}
	baseSnapshot := Snapshot{
		AvailableBalance: 1000,
		FuturesRiskMetadata: []FuturesRiskMetadata{{
			Symbol:             "ETHUSDT",
			ConfiguredLeverage: 10,
		}},
	}

	cases := []struct {
		name       string
		req        ReviewRequest
		capability adapter.OrderCapability
		snapshot   Snapshot
		pending    bool
		wantStatus DecisionStatus
		wantCode   string
	}{
		{
			name:       "spot reduce only buy rejected",
			req:        withMarket(baseReq, int32(domain.MarketSpot), "BUY", true),
			capability: spotCapability(),
			snapshot: Snapshot{Balances: []Balance{{
				Asset:     "ETH",
				Available: 1,
			}}},
			wantStatus: DecisionReject,
			wantCode:   "SPOT_REDUCE_ONLY_BUY",
		},
		{
			name:       "spot reduce only sell over unlocked rejected",
			req:        withMarket(baseReq, int32(domain.MarketSpot), "SELL", true),
			capability: spotCapability(),
			snapshot: Snapshot{Balances: []Balance{{
				Asset:     "ETH",
				Available: 0.1,
				Locked:    0.1,
			}}},
			wantStatus: DecisionReject,
			wantCode:   "INSUFFICIENT_UNLOCKED_QTY",
		},
		{
			name:       "spot buy over quote balance rejected",
			req:        withMarket(baseReq, int32(domain.MarketSpot), "BUY", false),
			capability: spotCapability(),
			snapshot: Snapshot{Balances: []Balance{{
				Asset:     "USDT",
				Available: 100,
			}}},
			wantStatus: DecisionReject,
			wantCode:   "INSUFFICIENT_QUOTE_BALANCE",
		},
		{
			name:       "spot market buy without price rejected",
			req:        spotMarketBuyWithoutPrice(baseReq),
			capability: spotCapability(),
			snapshot: Snapshot{Balances: []Balance{{
				Asset:     "USDT",
				Available: 1000,
			}}},
			wantStatus: DecisionReject,
			wantCode:   "PRICE_REQUIRED_FOR_RISK",
		},
		{
			name:       "spot buy within quote balance allowed",
			req:        withMarket(baseReq, int32(domain.MarketSpot), "BUY", false),
			capability: spotCapability(),
			snapshot: Snapshot{Balances: []Balance{{
				Asset:     "USDT",
				Available: 1000,
			}}},
			wantStatus: DecisionAllow,
		},
		{
			name:       "futures missing risk metadata rejected",
			req:        baseReq,
			capability: baseCapability,
			snapshot:   Snapshot{AvailableBalance: 1000},
			wantStatus: DecisionReject,
			wantCode:   "RISK_METADATA_MISSING",
		},
		{
			name:       "pending route blocks open order",
			req:        baseReq,
			capability: baseCapability,
			snapshot:   baseSnapshot,
			pending:    true,
			wantStatus: DecisionReject,
			wantCode:   "ROUTE_PENDING_EXECUTION",
		},
		{
			name:       "futures reduce only without position rejected",
			req:        withReduceOnly(baseReq, "SELL"),
			capability: baseCapability,
			snapshot:   baseSnapshot,
			wantStatus: DecisionReject,
			wantCode:   "REDUCE_ONLY_POSITION_MISSING",
		},
		{
			name:       "futures reduce only wrong side rejected",
			req:        withReduceOnly(baseReq, "BUY"),
			capability: baseCapability,
			snapshot: snapshotWithPosition(baseSnapshot, Position{
				Symbol:       "ETHUSDT",
				PositionSide: "BOTH",
				Qty:          0.5,
			}),
			wantStatus: DecisionReject,
			wantCode:   "REDUCE_ONLY_POSITION_MISMATCH",
		},
		{
			name:       "futures reduce only over position rejected",
			req:        withReduceOnly(baseReq, "SELL"),
			capability: baseCapability,
			snapshot: snapshotWithPosition(baseSnapshot, Position{
				Symbol:       "ETHUSDT",
				PositionSide: "BOTH",
				Qty:          0.1,
			}),
			wantStatus: DecisionReject,
			wantCode:   "REDUCE_ONLY_QTY_EXCEEDS_POSITION",
		},
		{
			name:       "futures reduce only sell long allowed",
			req:        withReduceOnly(baseReq, "SELL"),
			capability: baseCapability,
			snapshot: snapshotWithPosition(baseSnapshot, Position{
				Symbol:       "ETHUSDT",
				PositionSide: "BOTH",
				Qty:          0.5,
			}),
			wantStatus: DecisionAllow,
		},
		{
			name:       "futures reduce only buy short allowed",
			req:        withReduceOnly(baseReq, "BUY"),
			capability: baseCapability,
			snapshot: snapshotWithPosition(baseSnapshot, Position{
				Symbol:       "ETHUSDT",
				PositionSide: "BOTH",
				Qty:          -0.5,
			}),
			wantStatus: DecisionAllow,
		},
		{
			name:       "supported limit gtc allowed",
			req:        baseReq,
			capability: baseCapability,
			snapshot:   baseSnapshot,
			wantStatus: DecisionAllow,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := NewDefaultGate(DefaultGateConfig{
				CapabilityReader: staticCapabilityReader{capability: tc.capability},
				SnapshotReader:   staticSnapshotReader{snapshot: tc.snapshot},
				PendingReader:    staticPendingReader{pending: tc.pending},
				Now: func() time.Time {
					return time.Unix(1893456000, 0).UTC()
				},
			})

			decision, err := gate.Review(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("Review() error = %v", err)
			}
			if decision.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s; decision=%+v", decision.Status, tc.wantStatus, decision)
			}
			if decision.ReasonCode != tc.wantCode {
				t.Fatalf("reason_code = %q, want %q; decision=%+v", decision.ReasonCode, tc.wantCode, decision)
			}
		})
	}
}

func TestDefaultGateMergesSymbolRulesWithoutDroppingConfiguredLeverage(t *testing.T) {
	price := 2499.0
	gate := NewDefaultGate(DefaultGateConfig{
		CapabilityReader: staticCapabilityReader{capability: adapter.OrderCapability{
			Market:             domain.MarketPerpetualFutures,
			OrderTypes:         []string{"LIMIT"},
			TimeInForce:        []string{"GTC"},
			SupportsReduceOnly: true,
		}},
		SnapshotReader: staticSnapshotReader{snapshot: Snapshot{
			AvailableBalance: 1000,
			FuturesRiskMetadata: []FuturesRiskMetadata{{
				Symbol:             "ETHUSDT",
				ConfiguredLeverage: 10,
			}},
		}},
		SymbolRulesReader: staticSymbolRulesReader{rules: []FuturesRiskMetadata{{
			Symbol:      "ETHUSDT",
			MinNotional: 1000,
			MinQty:      0.001,
			StepSize:    0.001,
			TickSize:    0.01,
		}}},
		Now: func() time.Time {
			return time.Unix(1893456000, 0).UTC()
		},
	})

	decision, err := gate.Review(context.Background(), ReviewRequest{
		AccountID:   1,
		VenueID:     10,
		UserID:      77,
		Environment: int32(domain.EnvironmentDemo),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketPerpetualFutures),
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		Qty:         0.2,
		Price:       &price,
		MarkPrice:   2500,
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
	})
	if err != nil {
		t.Fatalf("Review() error = %v", err)
	}
	if decision.Status != DecisionReject || decision.ReasonCode != "MIN_NOTIONAL_VIOLATION" {
		t.Fatalf("decision = %+v, want MIN_NOTIONAL_VIOLATION instead of losing leverage", decision)
	}
}

func TestDefaultGateRejectsSymbolRuleStepAndTickMisalignment(t *testing.T) {
	baseReq := ReviewRequest{
		AccountID:   1,
		VenueID:     10,
		UserID:      77,
		Environment: int32(domain.EnvironmentDemo),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketPerpetualFutures),
		Symbol:      "ETHUSDT",
		Side:        "BUY",
		Qty:         0.2,
		MarkPrice:   2500,
		OrderType:   "LIMIT",
		TimeInForce: "GTC",
	}
	price := 2499.0
	baseReq.Price = &price
	gate := NewDefaultGate(DefaultGateConfig{
		CapabilityReader: staticCapabilityReader{capability: adapter.OrderCapability{
			Market:             domain.MarketPerpetualFutures,
			OrderTypes:         []string{"LIMIT"},
			TimeInForce:        []string{"GTC"},
			SupportsReduceOnly: true,
		}},
		SnapshotReader: staticSnapshotReader{snapshot: Snapshot{
			AvailableBalance: 1000,
			FuturesRiskMetadata: []FuturesRiskMetadata{{
				Symbol:             "ETHUSDT",
				ConfiguredLeverage: 10,
			}},
		}},
		SymbolRulesReader: staticSymbolRulesReader{rules: []FuturesRiskMetadata{{
			Symbol:      "ETHUSDT",
			MinNotional: 1,
			MinQty:      0.001,
			StepSize:    0.001,
			TickSize:    0.01,
		}}},
	})

	stepReq := baseReq
	stepReq.Qty = 0.2005
	decision, err := gate.Review(context.Background(), stepReq)
	if err != nil {
		t.Fatalf("Review(step) error = %v", err)
	}
	if decision.Status != DecisionReject || decision.ReasonCode != "STEP_SIZE_VIOLATION" {
		t.Fatalf("step decision = %+v, want STEP_SIZE_VIOLATION", decision)
	}

	tickReq := baseReq
	tickPrice := 2499.005
	tickReq.Price = &tickPrice
	decision, err = gate.Review(context.Background(), tickReq)
	if err != nil {
		t.Fatalf("Review(tick) error = %v", err)
	}
	if decision.Status != DecisionReject || decision.ReasonCode != "TICK_SIZE_VIOLATION" {
		t.Fatalf("tick decision = %+v, want TICK_SIZE_VIOLATION", decision)
	}
}

func TestMergeRiskMetadataDoesNotOverwriteConfiguredLeverage(t *testing.T) {
	merged := mergeRiskMetadata(
		[]FuturesRiskMetadata{{
			Symbol:             "ETHUSDT",
			ConfiguredLeverage: 10,
		}},
		[]FuturesRiskMetadata{{
			Symbol:             "ETHUSDT",
			ConfiguredLeverage: 2,
			MinQty:             0.001,
			StepSize:           0.001,
		}},
	)
	if len(merged) != 1 {
		t.Fatalf("merged len = %d, want 1", len(merged))
	}
	if merged[0].ConfiguredLeverage != 10 {
		t.Fatalf("configured leverage = %g, want wallet snapshot leverage 10", merged[0].ConfiguredLeverage)
	}
	if merged[0].StepSize != 0.001 || merged[0].MinQty != 0.001 {
		t.Fatalf("symbol rules were not merged: %+v", merged[0])
	}
}

func TestDefaultGateDoesNotRequireFuturesSymbolRulesForSpot(t *testing.T) {
	gate := NewDefaultGate(DefaultGateConfig{
		CapabilityReader: staticCapabilityReader{capability: spotCapability()},
		SnapshotReader: staticSnapshotReader{snapshot: Snapshot{Balances: []Balance{{
			Asset:     "ETH",
			Available: 1,
		}}}},
		SymbolRulesReader: failingSymbolRulesReader{},
	})

	decision, err := gate.Review(context.Background(), ReviewRequest{
		AccountID:   1,
		VenueID:     10,
		UserID:      77,
		Environment: int32(domain.EnvironmentDemo),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketSpot),
		Symbol:      "ETHUSDT",
		Side:        "SELL",
		Qty:         0.2,
		OrderType:   "MARKET",
	})
	if err != nil {
		t.Fatalf("Review() error = %v", err)
	}
	if decision.Status != DecisionAllow {
		t.Fatalf("decision = %+v, want spot sell allowed without futures symbol rules", decision)
	}
}

func withMarket(req ReviewRequest, market int32, side string, reduceOnly bool) ReviewRequest {
	req.Market = market
	req.Side = side
	req.ReduceOnly = reduceOnly
	return req
}

func withReduceOnly(req ReviewRequest, side string) ReviewRequest {
	req.Side = side
	req.ReduceOnly = true
	return req
}

func spotMarketBuyWithoutPrice(req ReviewRequest) ReviewRequest {
	req.Market = int32(domain.MarketSpot)
	req.Side = "BUY"
	req.OrderType = "MARKET"
	req.TimeInForce = ""
	req.Price = nil
	req.MarkPrice = 0
	return req
}

func snapshotWithPosition(snapshot Snapshot, position Position) Snapshot {
	snapshot.Positions = append(append([]Position(nil), snapshot.Positions...), position)
	return snapshot
}

func spotCapability() adapter.OrderCapability {
	return adapter.OrderCapability{
		Market:             domain.MarketSpot,
		OrderTypes:         []string{"MARKET", "LIMIT"},
		TimeInForce:        []string{"GTC", "IOC", "FOK"},
		SupportsPostOnly:   true,
		SupportsReduceOnly: true,
	}
}
