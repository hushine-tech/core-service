//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/catalog"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/httpserver"
	"github.com/hushine-tech/core-service/internal/repository"
	"github.com/hushine-tech/core-service/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestTimescaleHTTPCreateAndGRPC exercises the same paths as scripts/integration_e2e.sh:
// POST /accounts creates a backtest account context, then gRPC reads/updates
// backtest wallet state against TimescaleDB.
func TestTimescaleHTTPCreateAndGRPC(t *testing.T) {
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		dsn = "host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable"
	}
	repo, err := repository.NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	btName := "int-bt-" + suffix
	username := "int-user-" + suffix

	h := httpserver.NewHandler(repo)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// --- gRPC server in-process (mock Binance) ---
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	mock := exchange.NewIntegrationMockFetcher()
	router := exchange.NewAdapterRouter(map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvLive}:    mock,
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: mock,
	}, repo.GetAccountState)
	symCat := catalog.NewWithFetchers(0,
		func(context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
		func(context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
	)
	accountv1.RegisterAccountServiceServer(grpcSrv, service.NewAccountGRPCService(repo, router, symCat, nil))
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop() })

	grpcAddr := lis.Addr().String()
	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := accountv1.NewAccountServiceClient(conn)
	userID := createUser(t, cli, username)

	// --- Backtest account context; default simulated venue is created by the service. ---
	btBody := fmt.Sprintf(`{"user_id":%d,"name":%q,"environment":0}`, userID, btName)
	btID := mustPostAccount(t, ts.URL, btBody)

	ls, err := cli.ListSymbols(ctx, &accountv1.ListSymbolsRequest{Market: "spot", Query: "ETH", Limit: 5})
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(ls.GetSymbols()) != 1 || ls.GetSymbols()[0] != "ETHUSDT" {
		t.Fatalf("ListSymbols: %v", ls.GetSymbols())
	}

	// Backtest: initial portfolio read
	g1, err := cli.GetPortfolioSnapshot(ctx, &accountv1.GetPortfolioSnapshotRequest{AccountId: btID, UserId: userID})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot backtest: %v", err)
	}
	if g1.GetSnapshot().GetWallet().GetFutures().GetWalletBalance() != 0 {
		t.Fatalf("backtest initial futures.wallet_balance want 0 got %f", g1.GetSnapshot().GetWallet().GetFutures().GetWalletBalance())
	}
}

// TestRegistryGRPC exercises CreateAccount / ListAccounts / GetAccount over gRPC.
func TestRegistryGRPC(t *testing.T) {
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		dsn = "host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable"
	}
	repo, err := repository.NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	name := "int-grpc-reg-" + suffix
	username := "int-user-" + suffix

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	mock := exchange.NewIntegrationMockFetcher()
	router := exchange.NewAdapterRouter(map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvLive}:    mock,
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: mock,
	}, repo.GetAccountState)
	symCat := catalog.NewWithFetchers(0,
		func(context.Context) ([]string, error) { return []string{"BTCUSDT"}, nil },
		func(context.Context) ([]string, error) { return []string{"BTCUSDT"}, nil },
	)
	accountv1.RegisterAccountServiceServer(grpcSrv, service.NewAccountGRPCService(repo, router, symCat, nil))
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop() })

	conn, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := accountv1.NewAccountServiceClient(conn)
	userID := createUser(t, cli, username)

	created, err := cli.CreateAccount(ctx, &accountv1.CreateAccountRequest{
		Name:        name,
		Environment: int32(domain.EnvironmentBacktest),
		UserId:      userID,
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if created.GetAccountId() == 0 || created.GetName() != name {
		t.Fatalf("CreateAccount response: %+v", created)
	}

	list, err := cli.ListAccounts(ctx, &accountv1.ListAccountsRequest{UserId: userID})
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}
	var found bool
	for _, a := range list.GetAccounts() {
		if a.GetAccountId() == created.GetAccountId() {
			found = true
			if a.GetName() != name {
				t.Fatalf("list name want %q got %q", name, a.GetName())
			}
		}
	}
	if !found {
		t.Fatalf("created account %d not in list", created.GetAccountId())
	}

	got, err := cli.GetAccount(ctx, &accountv1.GetAccountRequest{AccountId: created.GetAccountId(), UserId: userID})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if got.GetAccount().GetName() != name {
		t.Fatalf("GetAccount name want %q got %q", name, got.GetAccount().GetName())
	}
}

// TestBacktestMultiSymbolVenueWalletBootstrap persists spot+futures via venue creation.
func TestBacktestMultiSymbolVenueWalletBootstrap(t *testing.T) {
	dsn := os.Getenv("TIMESCALEDB_DSN")
	if dsn == "" {
		dsn = "host=192.168.88.10 port=5432 user=postgres password=postgres dbname=account sslmode=disable"
	}
	repo, err := repository.NewTimescaleRepository(dsn, nil)
	if err != nil {
		t.Skipf("skip: cannot connect to TimescaleDB (%v). Set TIMESCALEDB_DSN or ensure DB is up.", err)
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	name := "int-wb-" + suffix
	username := "int-user-" + suffix

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	mock := exchange.NewIntegrationMockFetcher()
	router := exchange.NewAdapterRouter(map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvLive}:    mock,
		{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}: mock,
	}, repo.GetAccountState)
	symCat := catalog.NewWithFetchers(0,
		func(context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
		func(context.Context) ([]string, error) { return []string{"BTCUSDT", "ETHUSDT"}, nil },
	)
	accountv1.RegisterAccountServiceServer(grpcSrv, service.NewAccountGRPCService(repo, router, symCat, nil))
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop() })

	conn, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := accountv1.NewAccountServiceClient(conn)
	userID := createUser(t, cli, username)

	created, err := cli.CreateAccount(ctx, &accountv1.CreateAccountRequest{Name: name, Environment: int32(domain.EnvironmentBacktest), UserId: userID})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	price := 41000.0
	_, err = cli.CreateVenue(ctx, &accountv1.CreateVenueRequest{
		UserId:      userID,
		AccountId:   created.GetAccountId(),
		Exchange:    int32(domain.ExchangeBinance),
		Market:      int32(domain.MarketPerpetualFutures),
		Environment: int32(domain.EnvironmentBacktest),
		Status:      int32(domain.VenueStatusActive),
		DisplayName: "backtest-bootstrap",
		ApiKey:      "backtest-" + suffix,
		Spot: &accountv1.SpotWallet{
			Free: 5000, Locked: 0,
			Assets: []*accountv1.SpotAsset{
				{Symbol: "BTCUSDT", Qty: 0.1, AvgEntryPrice: 40000, Price: &price},
				{Symbol: "ETHUSDT", Qty: 1, AvgEntryPrice: 2000, Price: f64Ptr(2100)},
			},
		},
		Futures: &accountv1.FuturesWallet{
			MarginMode: "isolated", PositionMode: "one_way",
			Positions: []*accountv1.FuturesPosition{
				{Symbol: "BTCUSDT", InitialBalance: 2000, Leverage: 10, FeeRate: 0.0004},
				{Symbol: "ETHUSDT", InitialBalance: 1500, Leverage: 10, FeeRate: 0.0004},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateVenue: %v", err)
	}

	g, err := cli.GetPortfolioSnapshot(ctx, &accountv1.GetPortfolioSnapshotRequest{AccountId: created.GetAccountId(), UserId: userID})
	if err != nil {
		t.Fatalf("GetPortfolioSnapshot: %v", err)
	}
	if g.GetSnapshot().GetWallet().GetTotalValue() <= 0 {
		t.Fatalf("expected positive total_value, got %f", g.GetSnapshot().GetWallet().GetTotalValue())
	}
	if len(g.GetSnapshot().GetWallet().GetSpot().GetAssets()) != 2 {
		t.Fatalf("spot assets: %d", len(g.GetSnapshot().GetWallet().GetSpot().GetAssets()))
	}
	if len(g.GetSnapshot().GetWallet().GetFutures().GetPositions()) != 2 {
		t.Fatalf("futures positions: %d", len(g.GetSnapshot().GetWallet().GetFutures().GetPositions()))
	}
}

func f64Ptr(v float64) *float64 { return &v }

func createUser(t *testing.T, cli accountv1.AccountServiceClient, username string) int64 {
	t.Helper()
	resp, err := cli.CreateUser(context.Background(), &accountv1.CreateUserRequest{
		Username: username,
		Password: "integration-pass-123",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if resp.GetUser() == nil || resp.GetUser().GetId() == 0 {
		t.Fatal("CreateUser returned empty user")
	}
	return resp.GetUser().GetId()
}

func mustPostAccount(t *testing.T, baseURL, jsonBody string) int64 {
	t.Helper()
	resp, err := http.Post(baseURL+"/accounts", "application/json", bytes.NewReader([]byte(jsonBody)))
	if err != nil {
		t.Fatalf("POST /accounts: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /accounts: status %d body %s", resp.StatusCode, string(body))
	}
	var out struct {
		AccountID int64 `json:"account_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode response: %v body %s", err, string(body))
	}
	if out.AccountID == 0 {
		t.Fatalf("missing account_id in %s", string(body))
	}
	return out.AccountID
}
