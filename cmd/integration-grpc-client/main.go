// Command integration-grpc-client calls GetOnlineAccountInfo and UpdateAccountWalletState against a running core-service.
//
// Usage:
//
//	integration-grpc-client -addr 127.0.0.1:50051 -scenario create-user -username tester -password secret
//	integration-grpc-client -addr 127.0.0.1:50051 -account <id> -scenario backtest
//	integration-grpc-client -addr 127.0.0.1:50051 -account <id> -scenario live
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC address host:port")
	accountIDStr := flag.String("account", "", "account_id (integer) from POST /accounts")
	userID := flag.Int64("user", 0, "user_id for user-scoped reads")
	username := flag.String("username", "", "username for create-user")
	password := flag.String("password", "", "password for create-user")
	scenario := flag.String("scenario", "backtest", "create-user | backtest | live")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := accountv1.NewAccountServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch *scenario {
	case "create-user":
		runCreateUser(ctx, cli, *username, *password)
	case "backtest":
		accountID := parseAccountID(*accountIDStr)
		runBacktest(ctx, cli, accountID, *userID)
	case "live":
		accountID := parseAccountID(*accountIDStr)
		runLive(ctx, cli, accountID, *userID)
	default:
		log.Fatalf("unknown -scenario %q", *scenario)
	}
}

func parseAccountID(raw string) int64 {
	if raw == "" {
		log.Fatal("-account is required")
	}
	accountID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Fatalf("-account must be an integer: %v", err)
	}
	return accountID
}

func runCreateUser(ctx context.Context, cli accountv1.AccountServiceClient, username, password string) {
	if username == "" || password == "" {
		log.Fatal("-username and -password are required for create-user")
	}
	resp, err := cli.CreateUser(ctx, &accountv1.CreateUserRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CreateUser: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("user_id=%d username=%s\n", resp.GetUser().GetId(), resp.GetUser().GetUsername())
}

func runBacktest(ctx context.Context, cli accountv1.AccountServiceClient, accountID, userID int64) {
	if userID <= 0 {
		log.Fatal("-user is required for backtest")
	}
	fmt.Println("=== backtest: GetOnlineAccountInfo (expect seeded balance) ===")
	g1, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: accountID, UserId: userID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOnlineAccountInfo: %v\n", err)
		os.Exit(1)
	}
	w := g1.GetWallet()
	fmt.Printf("futures.wallet_balance=%.2f environment=%d\n", w.GetFutures().GetWalletBalance(), w.GetEnvironment())

	fmt.Println("=== backtest: UpdateAccountWalletState (push) ===")
	u, err := cli.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        accountID,
		WalletBalance:    15000,
		AvailableBalance: 14000,
		TotalValue:       15500,
		Futures: &accountv1.FuturesWallet{
			MarginMode:   "cross",
			PositionMode: "one_way",
			Positions: []*accountv1.FuturesPosition{
				{Symbol: "BTCUSDT", Direction: 0, InitialBalance: 2000, Leverage: 5, FeeRate: 0.0004},
			},
		},
		Spot: &accountv1.SpotWallet{Free: 600, Locked: 0},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "UpdateAccountWalletState: %v\n", err)
		os.Exit(1)
	}
	w2 := u.GetWallet()
	fmt.Printf("after update futures.wallet_balance=%.2f (expect 15000)\n", w2.GetFutures().GetWalletBalance())

	fmt.Println("=== backtest: GetOnlineAccountInfo again ===")
	g2, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: accountID, UserId: userID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOnlineAccountInfo: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("futures.wallet_balance=%.2f (expect 15000)\n", g2.GetWallet().GetFutures().GetWalletBalance())
}

func runLive(ctx context.Context, cli accountv1.AccountServiceClient, accountID, userID int64) {
	if userID <= 0 {
		log.Fatal("-user is required for live")
	}
	fmt.Println("=== live (mock exchange): GetOnlineAccountInfo ===")
	g1, err := cli.GetOnlineAccountInfo(ctx, &accountv1.GetOnlineAccountInfoRequest{AccountId: accountID, UserId: userID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOnlineAccountInfo: %v\n", err)
		os.Exit(1)
	}
	w := g1.GetWallet()
	fmt.Printf("futures.wallet_balance=%.2f (expect mock 8888.5) environment=%d\n", w.GetFutures().GetWalletBalance(), w.GetEnvironment())

	fmt.Println("=== live: UpdateAccountWalletState (request ignored; expect mock values) ===")
	u, err := cli.UpdateAccountWalletState(ctx, &accountv1.UpdateAccountWalletStateRequest{
		AccountId:        accountID,
		WalletBalance:    999999,
		AvailableBalance: 999999,
		TotalValue:       999999,
		Futures:          &accountv1.FuturesWallet{WalletBalance: 999999},
		Spot:             &accountv1.SpotWallet{Free: 999999},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "UpdateAccountWalletState: %v\n", err)
		os.Exit(1)
	}
	w2 := u.GetWallet()
	fmt.Printf("after update futures.wallet_balance=%.2f (expect ~8888.5 from mock, not 999999)\n", w2.GetFutures().GetWalletBalance())
	if w2.GetFutures().GetWalletBalance() > 100000 {
		fmt.Fprintln(os.Stderr, "FAIL: live update should not persist bogus request balance")
		os.Exit(1)
	}
}
