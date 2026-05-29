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
// POST /accounts (backtest with initial_balance + live), then gRPC Get/Update against TimescaleDB.
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
	lvName := "int-lv-" + suffix
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

	// --- Backtest account + wallet seed (same as curl POST /accounts) ---
	btBody := fmt.Sprintf(`{"user_id":%d,"name":%q,"mode":0,"initial_balance":10000}`, userID, btName)
	btID := mustPostAccount(t, ts.URL, btBody)

	// --- Live account (no seed; exchange/mock fills on first read) ---
	lvBody := fmt.Sprintf(`{"user_id":%d,"name":%q,"mode":1,"api_key":"mock","api_secret":"mock"}`, userID, lvName)
	lvID := mustPostAccount(t, ts.URL, lvBody)

	ls, err := cli.ListSymbols(ctx, &accountv1.ListSymbolsRequest{Market: "spot", Query: "ETH", Limit: 5})
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(ls.GetSymbols()) != 1 || ls.GetSymbols()[0] != "ETHUSDT" {
		t.Fatalf("ListSymbols: %v", ls.GetSymbols())
	}

	// Backtest: initial read
	g1, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: btID, UserId: userID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo backtest: %v", err)
	}
	if g1.GetWallet().GetFutures().GetWalletBalance() != 10000 {
		t.Fatalf("backtest initial futures.wallet_balance want 10000 got %f", g1.GetWallet().GetFutures().GetWalletBalance())
	}

	// Backtest: push update (no snapshot)
	u1, err := cli.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        btID,
		WalletBalance:    15000,
		AvailableBalance: 14000,
		TotalValue:       15500,
		Futures:          &accountv1.FuturesWallet{MarginMode: "cross", PositionMode: "one_way"},
		Spot:             &accountv1.SpotWallet{Free: 600},
		SnapshotReason:   0, // no snapshot for plain state update
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState backtest: %v", err)
	}
	if u1.GetWallet().GetFutures().GetWalletBalance() != 15000 {
		t.Fatalf("backtest update response want 15000 got %f", u1.GetWallet().GetFutures().GetWalletBalance())
	}

	g2, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: btID, UserId: userID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo backtest 2: %v", err)
	}
	if g2.GetWallet().GetFutures().GetWalletBalance() != 15000 {
		t.Fatalf("backtest persisted want 15000 got %f", g2.GetWallet().GetFutures().GetWalletBalance())
	}

	// Live: mock exchange
	gl, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: lvID, UserId: userID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo live: %v", err)
	}
	if gl.GetWallet().GetFutures().GetWalletBalance() < 8888.0 || gl.GetWallet().GetFutures().GetWalletBalance() > 8889.0 {
		t.Fatalf("live mock futures.wallet_balance want ~8888.5 got %f", gl.GetWallet().GetFutures().GetWalletBalance())
	}

	ul, err := cli.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        lvID,
		WalletBalance:    999999,
		AvailableBalance: 999999,
		TotalValue:       999999,
		Futures:          &accountv1.FuturesWallet{WalletBalance: 999999},
		Spot:             &accountv1.SpotWallet{Free: 999999},
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState live: %v", err)
	}
	if ul.GetWallet().GetFutures().GetWalletBalance() > 100000 {
		t.Fatalf("live update should not echo bogus request; got %f", ul.GetWallet().GetFutures().GetWalletBalance())
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
		Name:           name,
		Environment:    int32(domain.EnvironmentBacktest),
		InitialBalance: 5000,
		UserId:         userID,
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

// TestBacktestMultiSymbolWalletBootstrap persists spot+futures via UpdateAccountWalletState after gRPC create.
func TestBacktestMultiSymbolWalletBootstrap(t *testing.T) {
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
	_, err = cli.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId: created.GetAccountId(),
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
		TotalValue:       5000 + 0.1*41000 + 1*2100 + 2000 + 1500,
		WalletBalance:    3500,
		AvailableBalance: 3500,
	})
	if err != nil {
		t.Fatalf("UpdateAccountWalletState: %v", err)
	}

	g, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: created.GetAccountId(), UserId: userID})
	if err != nil {
		t.Fatalf("GetOnlineAccountInfo: %v", err)
	}
	if g.GetWallet().GetTotalValue() <= 0 {
		t.Fatalf("expected positive total_value, got %f", g.GetWallet().GetTotalValue())
	}
	if len(g.GetWallet().GetSpot().GetAssets()) != 2 {
		t.Fatalf("spot assets: %d", len(g.GetWallet().GetSpot().GetAssets()))
	}
	if len(g.GetWallet().GetFutures().GetPositions()) != 2 {
		t.Fatalf("futures positions: %d", len(g.GetWallet().GetFutures().GetPositions()))
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
