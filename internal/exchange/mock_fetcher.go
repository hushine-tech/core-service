package exchange

import (
	"context"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

// MockOnlineInfoFetcher returns deterministic wallet data without calling Binance (integration / dev).
type MockOnlineInfoFetcher struct {
	// Fixed is merged into each response; AccountID and UpdatedAt are set per call.
	Fixed domain.OnlineAccountInfo
}

// NewIntegrationMockFetcher builds a mock that yields recognizable balances for assertions.
func NewIntegrationMockFetcher() *MockOnlineInfoFetcher {
	return &MockOnlineInfoFetcher{
		Fixed: domain.OnlineAccountInfo{
			Futures: domain.FuturesWallet{
				MarginMode:         "cross",
				PositionMode:       "one_way",
				WalletBalance:      8888.5,
				AvailableBalance:   7777.25,
				TotalUnrealizedPnl: 42,
				UnrealizedPnl:      42,
				TotalMarginBalance: 8930.5,
				MarginBalance:      8930.5,
			},
			Spot: domain.SpotWallet{
				Free:   100,
				Locked: 10,
			},
			TotalValue:       9040.5,
			WalletBalance:    8888.5,
			AvailableBalance: 7777.25,
		},
	}
}

// FetchOnlineAccountInfo implements OnlineInfoFetcher with per-account context.
// Credentials on the account are not required for the mock — this lets dev /
// integration setups continue to work without wiring real testnet keys.
func (m *MockOnlineInfoFetcher) FetchOnlineAccountInfo(_ context.Context, account domain.Account) (domain.OnlineAccountInfo, error) {
	info := m.Fixed
	info.AccountID = account.AccountID
	info.UpdatedAt = time.Now().UTC()
	return info, nil
}
