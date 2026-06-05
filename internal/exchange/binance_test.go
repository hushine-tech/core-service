package exchange

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/logger"
)

type routeResponse struct {
	status int
	body   string
	signed bool
}

func TestBinanceAdapter_FetchOnlineAccountInfo_MapsPhaseASnapshot(t *testing.T) {
	server, hits := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"1100.0",
				"totalUnrealizedProfit":"50.5",
				"totalMarginBalance":"1150.5",
				"totalPositionInitialMargin":"300.0",
				"totalOpenOrderInitialMargin":"25.0",
				"totalMaintMargin":"120.0",
				"totalCrossWalletBalance":"1090.0",
				"totalCrossUnPnl":"10.5",
				"availableBalance":"900.0",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":false}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{
					"asset":"USDT",
					"balance":"1100.0",
					"crossWalletBalance":"1090.0",
					"crossUnPnl":"10.5",
					"availableBalance":"900.0",
					"marginAvailable":true
				}
			]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{
					"symbol":"BTCUSDT",
					"positionSide":"LONG",
					"positionAmt":"0.5",
					"entryPrice":"30000",
					"breakEvenPrice":"30010",
					"markPrice":"30100",
					"unRealizedProfit":"50",
					"liquidationPrice":"25000",
					"marginType":"",
					"isolatedMargin":"0",
					"isolatedWallet":"200",
					"notional":"15050",
					"initialMargin":"150.5",
					"positionInitialMargin":"145.5",
					"openOrderInitialMargin":"5",
					"maintMargin":"12"
				},
				{
					"symbol":"ETHUSDT",
					"positionSide":"BOTH",
					"positionAmt":"0",
					"entryPrice":"0",
					"breakEvenPrice":"0",
					"markPrice":"0",
					"unRealizedProfit":"0",
					"liquidationPrice":"0",
					"marginType":"cross",
					"isolatedMargin":"0",
					"isolatedWallet":"0",
					"notional":"0",
					"initialMargin":"0",
					"positionInitialMargin":"0",
					"openOrderInitialMargin":"0",
					"maintMargin":"0"
				}
			]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{"symbol":"BTCUSDT","leverage":25,"marginType":"isolated"}
			]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{
					"symbol":"BTCUSDT",
					"brackets":[
						{
							"bracket":1,
							"initialLeverage":25,
							"notionalCap":50000,
							"notionalFloor":0,
							"maintMarginRatio":0.005,
							"cum":0
						}
					]
				}
			]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body: `{
				"symbols":[
					{
						"symbol":"BTCUSDT",
						"pricePrecision":2,
						"quantityPrecision":3,
						"filters":[
							{"filterType":"PRICE_FILTER","tickSize":"0.10"},
							{"filterType":"LOT_SIZE","stepSize":"0.001"}
						]
					}
				]
			}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"balances":[
					{"asset":"USDT","free":"200","locked":"10"},
					{"asset":"BTC","free":"0.01","locked":"0.002"}
				]
			}`,
		},
		"/api/v3/ticker/price": {
			status: http.StatusOK,
			body:   `{"symbol":"BTCUSDT","price":"30000"}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if got, want := info.TotalValue, 1720.5; got != want {
		t.Fatalf("TotalValue = %v, want %v", got, want)
	}
	if got, want := info.Futures.TotalMarginBalance, 1150.5; got != want {
		t.Fatalf("TotalMarginBalance = %v, want %v", got, want)
	}
	if got, want := info.Futures.TotalPositionInitialMargin, 300.0; got != want {
		t.Fatalf("TotalPositionInitialMargin = %v, want %v", got, want)
	}
	if got, want := len(info.Futures.Positions), 1; got != want {
		t.Fatalf("len(Positions) = %d, want %d", got, want)
	}
	if info.Futures.MultiAssetsMode {
		t.Fatal("MultiAssetsMode = true, want false")
	}
	if info.Futures.PortfolioMargin {
		t.Fatal("PortfolioMargin = true, want false")
	}

	pos := info.Futures.Positions[0]
	if pos.Symbol != "BTCUSDT" {
		t.Fatalf("Position symbol = %q, want BTCUSDT", pos.Symbol)
	}
	if pos.Leverage != 25 {
		t.Fatalf("Position leverage = %v, want 25 (filled from metadata)", pos.Leverage)
	}
	if pos.MarginType != "isolated" {
		t.Fatalf("Position margin_type = %q, want isolated", pos.MarginType)
	}
	if pos.MarginMode != "isolated" {
		t.Fatalf("Position margin_mode = %q, want isolated", pos.MarginMode)
	}
	if pos.BreakEvenPrice != 30010 {
		t.Fatalf("BreakEvenPrice = %v, want 30010", pos.BreakEvenPrice)
	}
	if info.Spot.Free != 200 || info.Spot.Locked != 10 {
		t.Fatalf("Spot USDT balances = (%v,%v), want (200,10)", info.Spot.Free, info.Spot.Locked)
	}
	if len(info.Spot.Assets) != 1 || info.Spot.Assets[0].Symbol != "BTC" {
		t.Fatalf("spot assets = %+v, want BTC position preserved", info.Spot.Assets)
	}

	for _, path := range []string{
		"/fapi/v3/account",
		"/fapi/v1/multiAssetsMargin",
		"/fapi/v3/balance",
		"/fapi/v3/positionRisk",
		"/fapi/v1/symbolConfig",
		"/fapi/v1/leverageBracket",
		"/fapi/v1/exchangeInfo",
		"/api/v3/account",
		"/api/v3/ticker/price",
	} {
		if hits.count(path) != 1 {
			t.Fatalf("expected %s to be hit once, got %d", path, hits.count(path))
		}
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_CountsStablecoinSpotAssets(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"500",
				"totalUnrealizedProfit":"0",
				"totalMarginBalance":"500",
				"totalPositionInitialMargin":"0",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"0",
				"totalCrossWalletBalance":"500",
				"totalCrossUnPnl":"0",
				"availableBalance":"500",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":true}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"asset":"USDT","balance":"500","crossWalletBalance":"500","crossUnPnl":"0","availableBalance":"500","marginAvailable":true}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[]}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"balances":[
					{"asset":"USDT","free":"5000","locked":"0"},
					{"asset":"USDC","free":"5000","locked":"0"}
				]
			}`,
		},
		"/api/v3/ticker/price": {
			status: http.StatusOK,
			body:   `{"symbol":"USDCUSDT","price":"0.99958"}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if math.Abs(info.TotalValue-10497.9) > 1e-9 {
		t.Fatalf("TotalValue = %v, want 10497.9", info.TotalValue)
	}
	if len(info.Spot.Assets) != 1 || info.Spot.Assets[0].Symbol != "USDC" {
		t.Fatalf("spot assets = %+v, want preserved USDC asset", info.Spot.Assets)
	}
	if info.Spot.Assets[0].Price == nil || math.Abs(*info.Spot.Assets[0].Price-0.99958) > 1e-12 {
		t.Fatalf("USDC price = %v, want 0.99958", info.Spot.Assets[0].Price)
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_UnpricedSpotAssetsDoNotBlockFetch(t *testing.T) {
	// Regression for canonical-wallet-display-boundary review #2:
	// a dust / delisted spot asset whose USDT price cannot be resolved MUST
	// NOT fail the whole exchange snapshot call; that would block
	// futures-only strategy startup for any account carrying such an asset.
	// The asset is preserved with Price=nil; canonical spot runtime falls
	// back to free+locked at that point.
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"500",
				"totalUnrealizedProfit":"0",
				"totalMarginBalance":"500",
				"totalPositionInitialMargin":"0",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"0",
				"totalCrossWalletBalance":"500",
				"totalCrossUnPnl":"0",
				"availableBalance":"500",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":true}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"asset":"USDT","balance":"500","crossWalletBalance":"500","crossUnPnl":"0","availableBalance":"500","marginAvailable":true}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[]}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"balances":[
					{"asset":"USDT","free":"5000","locked":"0"},
					{"asset":"USDC","free":"5000","locked":"0"}
				]
			}`,
		},
		"/api/v3/ticker/price": {
			status: http.StatusNotFound,
			body:   `{"code":-1121,"msg":"Invalid symbol."}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Spot wallet still populated; the unpriced asset is preserved as a
	// first-class canonical entry so the runtime can still see the cash leg.
	if len(info.Spot.Assets) != 1 {
		t.Fatalf("expected 1 spot asset, got %+v", info.Spot.Assets)
	}
	asset := info.Spot.Assets[0]
	if asset.Symbol != "USDC" {
		t.Fatalf("expected USDC asset, got %s", asset.Symbol)
	}
	if asset.Price != nil {
		t.Fatalf("expected Price==nil for unpriced asset, got %v", *asset.Price)
	}
	// Futures leg must still be fully populated — it did not depend on spot.
	if info.Futures.WalletBalance != 500 {
		t.Fatalf("futures wallet balance = %v, want 500", info.Futures.WalletBalance)
	}
}

func containsAll(s string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

func TestBinanceAdapter_FetchOnlineAccountInfo_SpotFailureDoesNotBlockFuturesPhaseA(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"900",
				"totalUnrealizedProfit":"25",
				"totalMarginBalance":"925",
				"totalPositionInitialMargin":"100",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"50",
				"totalCrossWalletBalance":"900",
				"totalCrossUnPnl":"25",
				"availableBalance":"800",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":false}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"asset":"USDT","balance":"900","crossWalletBalance":"900","crossUnPnl":"25","availableBalance":"800","marginAvailable":true}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[]}`,
		},
		"/api/v3/account": {
			status: http.StatusForbidden,
			signed: true,
			body:   `{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if info.TotalValue != 925 {
		t.Fatalf("TotalValue = %v, want 925", info.TotalValue)
	}
	if info.Spot.Free != 0 || info.Spot.Locked != 0 || len(info.Spot.Assets) != 0 {
		t.Fatalf("spot wallet should stay empty on non-blocking failure, got %+v", info.Spot)
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_RepeatedReadsRemainStable(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"1000",
				"totalUnrealizedProfit":"40",
				"totalMarginBalance":"1040",
				"totalPositionInitialMargin":"150",
				"totalOpenOrderInitialMargin":"10",
				"totalMaintMargin":"60",
				"totalCrossWalletBalance":"995",
				"totalCrossUnPnl":"45",
				"availableBalance":"820",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":false}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"asset":"USDT","balance":"1000","crossWalletBalance":"995","crossUnPnl":"45","availableBalance":"820","marginAvailable":true}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{
					"symbol":"BTCUSDT",
					"positionSide":"LONG",
					"positionAmt":"0.2",
					"entryPrice":"30000",
					"breakEvenPrice":"30005",
					"markPrice":"30200",
					"unRealizedProfit":"40",
					"liquidationPrice":"26000",
					"marginType":"cross",
					"isolatedWallet":"0",
					"notional":"6040",
					"initialMargin":"120.8",
					"positionInitialMargin":"120.8",
					"openOrderInitialMargin":"0",
					"maintMargin":"30"
				}
			]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"symbol":"BTCUSDT","leverage":10,"marginType":"cross"}]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"symbol":"BTCUSDT","brackets":[{"bracket":1,"initialLeverage":10,"notionalCap":50000,"notionalFloor":0,"maintMarginRatio":0.005,"cum":0}]}]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[{"symbol":"BTCUSDT","pricePrecision":2,"quantityPrecision":3,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body:   `{"balances":[{"asset":"USDT","free":"50","locked":"0"}]}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	account := testExchangeAccount()

	first, err := adapter.FetchOnlineAccountInfo(context.Background(), account)
	if err != nil {
		t.Fatalf("first read error = %v", err)
	}
	second, err := adapter.FetchOnlineAccountInfo(context.Background(), account)
	if err != nil {
		t.Fatalf("second read error = %v", err)
	}

	if first.TotalValue != second.TotalValue {
		t.Fatalf("TotalValue drifted: %v vs %v", first.TotalValue, second.TotalValue)
	}
	if first.Futures.TotalMarginBalance != second.Futures.TotalMarginBalance {
		t.Fatalf("TotalMarginBalance drifted: %v vs %v", first.Futures.TotalMarginBalance, second.Futures.TotalMarginBalance)
	}
	if first.AvailableBalance != second.AvailableBalance {
		t.Fatalf("AvailableBalance drifted: %v vs %v", first.AvailableBalance, second.AvailableBalance)
	}
	if len(first.Futures.Positions) != len(second.Futures.Positions) {
		t.Fatalf("position length drifted: %d vs %d", len(first.Futures.Positions), len(second.Futures.Positions))
	}
	if len(first.Futures.Positions) != 1 {
		t.Fatalf("expected one position, got %d", len(first.Futures.Positions))
	}
	if first.Futures.Positions[0] != second.Futures.Positions[0] {
		t.Fatalf("position core fields drifted: %+v vs %+v", first.Futures.Positions[0], second.Futures.Positions[0])
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_OmitsRiskMetadataWhenNoOpenPositions(t *testing.T) {
	server, hits := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"5000",
				"totalUnrealizedProfit":"0",
				"totalMarginBalance":"5000",
				"totalPositionInitialMargin":"0",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"0",
				"totalCrossWalletBalance":"5000",
				"totalCrossUnPnl":"0",
				"availableBalance":"5000",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":false}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"asset":"USDT","balance":"5000","crossWalletBalance":"5000","crossUnPnl":"0","availableBalance":"5000","marginAvailable":true}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body:   `{"balances":[]}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if len(info.Futures.Positions) != 0 {
		t.Fatalf("expected no open positions, got %+v", info.Futures.Positions)
	}
	if len(info.Futures.RiskMetadata) != 0 {
		t.Fatalf("expected no risk metadata without open positions, got %+v", info.Futures.RiskMetadata)
	}
	if hits.count("/fapi/v1/symbolConfig") != 0 || hits.count("/fapi/v1/leverageBracket") != 0 {
		t.Fatalf("metadata endpoints should not be called without open positions")
	}
}

func TestBinanceAdapter_FetchPhaseAMetadata_ParsesSymbolRulesAndBrackets(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[{"symbol":"BTCUSDT","leverage":20,"marginType":"cross"}]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{
					"symbol":"BTCUSDT",
					"brackets":[
						{"bracket":1,"initialLeverage":20,"notionalCap":50000,"notionalFloor":0,"maintMarginRatio":0.004,"cum":0},
						{"bracket":2,"initialLeverage":10,"notionalCap":250000,"notionalFloor":50000,"maintMarginRatio":0.005,"cum":200}
					]
				}
			]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body: `{
				"symbols":[
					{
						"symbol":"BTCUSDT",
						"pricePrecision":2,
						"quantityPrecision":3,
						"filters":[
							{"filterType":"PRICE_FILTER","tickSize":"0.10"},
							{"filterType":"LOT_SIZE","stepSize":"0.001"}
						]
					}
				]
			}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	meta, err := adapter.fetchPhaseAMetadata(context.Background(), testExchangeAccount(), []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("fetchPhaseAMetadata() error = %v", err)
	}

	got, ok := meta["BTCUSDT"]
	if !ok {
		t.Fatalf("BTCUSDT metadata missing: %+v", meta)
	}
	if got.MarginType != "cross" {
		t.Fatalf("MarginType = %q, want cross", got.MarginType)
	}
	if got.InitialLeverage != 20 {
		t.Fatalf("InitialLeverage = %v, want 20", got.InitialLeverage)
	}
	if got.PricePrecision != 2 || got.QuantityPrecision != 3 {
		t.Fatalf("precision = (%d,%d), want (2,3)", got.PricePrecision, got.QuantityPrecision)
	}
	if got.TickSize != 0.1 || got.StepSize != 0.001 {
		t.Fatalf("tick/step = (%v,%v), want (0.1,0.001)", got.TickSize, got.StepSize)
	}
	if len(got.Brackets) != 2 {
		t.Fatalf("len(Brackets) = %d, want 2", len(got.Brackets))
	}
	if got.Brackets[1].NotionalFloor != 50000 {
		t.Fatalf("second bracket notional floor = %v, want 50000", got.Brackets[1].NotionalFloor)
	}
}

func TestBinanceAdapter_FetchPhaseAMetadata_BulkFallbackFiltersRequestedSymbols(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/symbolConfig":
			if symbol := r.URL.Query().Get("symbol"); symbol != "" {
				t.Fatalf("symbolConfig should bulk fetch above threshold, got symbol=%q", symbol)
			}
			_, _ = io.WriteString(w, `[
				{"symbol":"BTCUSDT","leverage":10,"marginType":"cross"},
				{"symbol":"ETHUSDT","leverage":5,"marginType":"cross"},
				{"symbol":"SOLUSDT","leverage":8,"marginType":"isolated"},
				{"symbol":"XRPUSDT","leverage":3,"marginType":"cross"},
				{"symbol":"DOGEUSDT","leverage":20,"marginType":"cross"}
			]`)
		case "/fapi/v1/leverageBracket":
			if symbol := r.URL.Query().Get("symbol"); symbol != "" {
				t.Fatalf("leverageBracket should bulk fetch above threshold, got symbol=%q", symbol)
			}
			_, _ = io.WriteString(w, `[
				{"symbol":"BTCUSDT","brackets":[{"bracket":1,"initialLeverage":10,"notionalCap":50000,"notionalFloor":0,"maintMarginRatio":0.004,"cum":0}]},
				{"symbol":"ETHUSDT","brackets":[{"bracket":1,"initialLeverage":5,"notionalCap":100000,"notionalFloor":0,"maintMarginRatio":0.005,"cum":0}]},
				{"symbol":"SOLUSDT","brackets":[{"bracket":1,"initialLeverage":8,"notionalCap":25000,"notionalFloor":0,"maintMarginRatio":0.01,"cum":0}]},
				{"symbol":"XRPUSDT","brackets":[{"bracket":1,"initialLeverage":3,"notionalCap":10000,"notionalFloor":0,"maintMarginRatio":0.02,"cum":0}]},
				{"symbol":"DOGEUSDT","brackets":[{"bracket":1,"initialLeverage":20,"notionalCap":5000,"notionalFloor":0,"maintMarginRatio":0.025,"cum":0}]}
			]`)
		case "/fapi/v1/exchangeInfo":
			_, _ = io.WriteString(w, `{"symbols":[
				{"symbol":"BTCUSDT","pricePrecision":2,"quantityPrecision":3,"filters":[]},
				{"symbol":"ETHUSDT","pricePrecision":2,"quantityPrecision":3,"filters":[]},
				{"symbol":"SOLUSDT","pricePrecision":3,"quantityPrecision":1,"filters":[]},
				{"symbol":"XRPUSDT","pricePrecision":4,"quantityPrecision":0,"filters":[]},
				{"symbol":"DOGEUSDT","pricePrecision":5,"quantityPrecision":0,"filters":[]}
			]}`)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	meta, err := adapter.fetchPhaseAMetadata(
		context.Background(),
		testExchangeAccount(),
		[]string{"ETHUSDT", "BTCUSDT", "SOLUSDT", "XRPUSDT"},
	)
	if err != nil {
		t.Fatalf("fetchPhaseAMetadata() error = %v", err)
	}
	if len(meta) != 4 {
		t.Fatalf("metadata len = %d, want 4: %+v", len(meta), meta)
	}
	if _, ok := meta["DOGEUSDT"]; ok {
		t.Fatalf("bulk metadata must filter unrequested DOGEUSDT: %+v", meta)
	}
	if got := meta["ETHUSDT"].InitialLeverage; got != 5 {
		t.Fatalf("ETH leverage = %v, want 5", got)
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_MultiAssetsKeepsCanonicalPrimaryAssetAndUSDDisplayTotals(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"10758.5315",
				"totalUnrealizedProfit":"0",
				"totalMarginBalance":"10758.5315",
				"totalPositionInitialMargin":"0",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"0",
				"totalCrossWalletBalance":"10758.5315",
				"totalCrossUnPnl":"0",
				"availableBalance":"10758.5315",
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":true}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{"asset":"USDT","balance":"5000","crossWalletBalance":"5000","crossUnPnl":"0","availableBalance":"5000","marginAvailable":true},
				{"asset":"USDC","balance":"5000","crossWalletBalance":"5000","crossUnPnl":"0","availableBalance":"5000","marginAvailable":true},
				{"asset":"BTC","balance":"0.01","crossWalletBalance":"0.01","crossUnPnl":"0","availableBalance":"0.01","marginAvailable":true}
			]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[]}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body:   `{"balances":[]}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if !info.Futures.MultiAssetsMode {
		t.Fatal("MultiAssetsMode = false, want true")
	}
	if got, want := info.Futures.WalletBalance, 5000.0; got != want {
		t.Fatalf("WalletBalance = %v, want %v", got, want)
	}
	if got, want := info.Futures.MarginBalance, 5000.0; got != want {
		t.Fatalf("MarginBalance = %v, want %v", got, want)
	}
	if got, want := info.Futures.DisplayWalletBalanceUsd, 10758.5315; got != want {
		t.Fatalf("DisplayWalletBalanceUsd = %v, want %v", got, want)
	}
	if got, want := info.Futures.DisplayMarginBalanceUsd, 10758.5315; got != want {
		t.Fatalf("DisplayMarginBalanceUsd = %v, want %v", got, want)
	}
	if got, want := info.TotalValue, 5000.0; got != want {
		t.Fatalf("TotalValue = %v, want %v", got, want)
	}
}

func TestBinanceAdapter_FetchOnlineAccountInfo_SingleAssetStillPublishesUSDDisplayTotals(t *testing.T) {
	server, _ := newBinanceTestServer(t, map[string]routeResponse{
		"/fapi/v3/account": {
			status: http.StatusOK,
			signed: true,
			body: `{
				"totalWalletBalance":"5000",
				"totalUnrealizedProfit":"0",
				"totalMarginBalance":"5000",
				"totalPositionInitialMargin":"0",
				"totalOpenOrderInitialMargin":"0",
				"totalMaintMargin":"0",
				"totalCrossWalletBalance":"5000",
				"totalCrossUnPnl":"0",
				"availableBalance":"5000",
				"assets":[
					{"asset":"USDT","walletBalance":"5000","unrealizedProfit":"0","marginBalance":"5000"},
					{"asset":"USDC","walletBalance":"5000","unrealizedProfit":"0","marginBalance":"5000"},
					{"asset":"BTC","walletBalance":"0.01","unrealizedProfit":"0","marginBalance":"0.01"}
				],
				"positions":[]
			}`,
		},
		"/fapi/v1/multiAssetsMargin": {
			status: http.StatusOK,
			signed: true,
			body:   `{"multiAssetsMargin":false}`,
		},
		"/fapi/v3/balance": {
			status: http.StatusOK,
			signed: true,
			body: `[
				{"asset":"USDT","balance":"5000","crossWalletBalance":"5000","crossUnPnl":"0","availableBalance":"5000","marginAvailable":true},
				{"asset":"USDC","balance":"5000","crossWalletBalance":"5000","crossUnPnl":"0","availableBalance":"5000","marginAvailable":true},
				{"asset":"BTC","balance":"0.01","crossWalletBalance":"0.01","crossUnPnl":"0","availableBalance":"0.01","marginAvailable":true}
			]`,
		},
		"/fapi/v1/assetIndex": {
			status: http.StatusOK,
			body:   `[{"symbol":"BTCUSD","index":"75853.15"}]`,
		},
		"/fapi/v3/positionRisk": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/symbolConfig": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/leverageBracket": {
			status: http.StatusOK,
			signed: true,
			body:   `[]`,
		},
		"/fapi/v1/exchangeInfo": {
			status: http.StatusOK,
			body:   `{"symbols":[]}`,
		},
		"/api/v3/account": {
			status: http.StatusOK,
			signed: true,
			body:   `{"balances":[]}`,
		},
		// USDC's live spot rate against USDT — typical real-world deviation.
		// canonical-wallet-display-boundary review #5: display-USD aggregation
		// must NOT hardcode stablecoins to exactly 1.0 — it should use the
		// exchange's own market rate so totals match the Binance UI.
		"/api/v3/ticker/price": {
			status: http.StatusOK,
			body:   `{"symbol":"USDCUSDT","price":"0.9996"}`,
		},
	})
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	info, err := adapter.FetchOnlineAccountInfo(context.Background(), testExchangeAccount())
	if err != nil {
		t.Fatalf("FetchOnlineAccountInfo() error = %v", err)
	}

	if info.Futures.MultiAssetsMode {
		t.Fatal("MultiAssetsMode = true, want false")
	}
	if got, want := info.Futures.WalletBalance, 5000.0; got != want {
		t.Fatalf("WalletBalance = %v, want %v", got, want)
	}
	if got, want := info.Futures.MarginBalance, 5000.0; got != want {
		t.Fatalf("MarginBalance = %v, want %v", got, want)
	}
	// Expected = 5000 * 1 (USDT reference) + 5000 * 0.9996 (USDC live)
	//            + 0.01 * 75853.15 (BTC from assetIndex)
	//          = 5000 + 4998 + 758.5315 = 10756.5315
	if got, want := info.Futures.DisplayWalletBalanceUsd, 10756.5315; got != want {
		t.Fatalf("DisplayWalletBalanceUsd = %v, want %v", got, want)
	}
	if got, want := info.Futures.DisplayMarginBalanceUsd, 10756.5315; got != want {
		t.Fatalf("DisplayMarginBalanceUsd = %v, want %v", got, want)
	}
	if got, want := info.Futures.DisplayUnrealizedPnlUsd, 0.0; got != want {
		t.Fatalf("DisplayUnrealizedPnlUsd = %v, want %v", got, want)
	}
}

func TestBinanceAdapter_signedGet_AppendsSignatureLast(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.RawQuery
		if !strings.Contains(raw, "timestamp=") {
			t.Fatalf("raw query missing timestamp: %q", raw)
		}
		parts := strings.Split(raw, "&signature=")
		if len(parts) != 2 {
			t.Fatalf("expected signature to be appended last, raw query = %q", raw)
		}
		if strings.Contains(parts[1], "&") {
			t.Fatalf("signature should be the final query param, raw query = %q", raw)
		}
		if got, want := parts[1], signHMAC(testExchangeAccount().APISecret, parts[0]); got != want {
			t.Fatalf("signature = %q, want %q for payload %q", got, want, parts[0])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()

	adapter := NewBinanceAdapter(EnvDemo, server.URL, server.URL, logger.Instance(), "binance_test")
	if _, err := adapter.signedGet(context.Background(), testExchangeAccount(), server.URL+"/fapi/v3/account", nil); err != nil {
		t.Fatalf("signedGet() error = %v", err)
	}
}

func testExchangeAccount() domain.Account {
	return domain.Account{
		AccountID:   42,
		Environment: domain.EnvironmentDemo,
		APIKey:      "acct-key",
		APISecret:   "acct-secret",
	}
}

type hitCounter struct {
	mu   sync.Mutex
	data map[string]int
}

func (c *hitCounter) inc(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[path]++
}

func (c *hitCounter) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[path]
}

func newBinanceTestServer(t *testing.T, routes map[string]routeResponse) (*httptest.Server, *hitCounter) {
	t.Helper()

	hits := &hitCounter{data: make(map[string]int)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, ok := routes[r.URL.Path]
		if !ok {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		hits.inc(r.URL.Path)
		if resp.signed {
			if got := r.Header.Get("X-MBX-APIKEY"); got == "" {
				t.Fatalf("signed request %s missing X-MBX-APIKEY", r.URL.Path)
			}
			if q := r.URL.Query().Get("timestamp"); q == "" {
				t.Fatalf("signed request %s missing timestamp", r.URL.Path)
			}
			if q := r.URL.Query().Get("signature"); q == "" {
				t.Fatalf("signed request %s missing signature", r.URL.Path)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.status)
		_, _ = io.WriteString(w, resp.body)
	}))
	return server, hits
}
