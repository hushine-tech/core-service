package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/logger"
	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

const (
	BinanceLiveBaseURL = "https://fapi.binance.com"
	// Binance derivatives docs now publish demo-fapi as the USDⓈ-M futures
	// testnet/demo REST host. The older testnet.binancefuture.com host rejects
	// keys created under the current demo flow with -1022 invalid signature.
	BinanceTestnetBaseURL = "https://demo-fapi.binance.com"
	BinanceSpotBaseURL    = "https://api.binance.com"
	BinanceSpotTestnetURL = "https://demo-api.binance.com"
)

const maxTargetedMetadataSymbols = 3

// BinanceAdapter is an environment-scoped Binance adapter.
//
// One instance represents a single (futures_base, spot_base) pair — i.e.
// "Binance live" or "Binance testnet". Adapters do NOT hold credentials;
// credentials are read from domain.Account on every request so a single
// process can serve many accounts.
type BinanceAdapter struct {
	env         ExchangeEnvironment
	futuresBase string
	spotBase    string
	httpClient  *httpclient.Client
}

// NewBinanceAdapter builds an environment-scoped adapter for Binance.
// Phase A: use NewBinanceLiveAdapter / NewBinanceTestnetAdapter below.
func NewBinanceAdapter(env ExchangeEnvironment, futuresBase, spotBase string, logger elog.Logger, apiName string) *BinanceAdapter {
	return &BinanceAdapter{
		env:         env,
		futuresBase: futuresBase,
		spotBase:    spotBase,
		httpClient:  httpclient.New(&http.Client{Timeout: 10 * time.Second}, logger, apiName),
	}
}

// NewBinanceLiveAdapter is a convenience constructor for the live environment.
func NewBinanceLiveAdapter(logger elog.Logger) *BinanceAdapter {
	return NewBinanceAdapter(EnvLive, BinanceLiveBaseURL, BinanceSpotBaseURL, logger, "binance_futures")
}

// NewBinanceTestnetAdapter is a convenience constructor for the testnet environment.
func NewBinanceTestnetAdapter(logger elog.Logger) *BinanceAdapter {
	return NewBinanceAdapter(EnvDemo, BinanceTestnetBaseURL, BinanceSpotTestnetURL, logger, "binance_demo")
}

// FetchOnlineAccountInfo implements OnlineInfoFetcher.
//
// Phase A: futures snapshot uses /fapi/v3/account + /fapi/v3/balance +
// /fapi/v3/positionRisk. Supporting metadata is fetched best-effort from
// /fapi/v1/symbolConfig, /fapi/v1/leverageBracket and /fapi/v1/exchangeInfo.
//
// Spot remains best-effort in Phase A so futures testnet parity is not blocked
// by a separate spot environment or permission mismatch. When spot balances are
// returned successfully, however, non-USDT assets must resolve to a real price;
// unresolved pricing is treated as an error to avoid misleading valuations.
func (a *BinanceAdapter) FetchOnlineAccountInfo(ctx context.Context, account domain.Account) (domain.OnlineAccountInfo, error) {
	if account.APIKey == "" || account.APISecret == "" {
		return domain.OnlineAccountInfo{}, fmt.Errorf(
			"account %d missing binance api credentials (env=%s)", account.AccountID, a.env,
		)
	}

	futures, err := a.fetchFuturesAccount(ctx, account)
	if err != nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf("futures account (v3): %w", err)
	}
	multiAssetsMode, err := a.fetchMultiAssetsMode(ctx, account)
	if err != nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf("futures multi-assets mode: %w", err)
	}
	futures.MultiAssetsMode = multiAssetsMode
	futures.PortfolioMargin = false

	balances, err := a.fetchFuturesBalance(ctx, account)
	if err != nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf("futures balance (v3): %w", err)
	}
	mergeFuturesBalanceSummary(&futures, balances)

	positions, err := a.fetchFuturesPositionRisk(ctx, account)
	if err != nil {
		return domain.OnlineAccountInfo{}, fmt.Errorf("futures positionRisk (v3): %w", err)
	}

	metadataSymbols := symbolsFromPositions(positions)
	meta, err := a.fetchPhaseAMetadata(ctx, account, metadataSymbols)
	if err != nil {
		logger.Warn(ctx, "system", fmt.Sprintf(
			"binance phase-a metadata fetch skipped for account=%d env=%s: %v",
			account.AccountID, a.env, err,
		))
	} else {
		applyPositionMetadata(positions, meta)
		futures.RiskMetadata = metadataToDomainRiskMetadata(meta)
	}
	futures.Positions = positions

	spot, err := a.fetchSpotAccount(ctx, account)
	if err != nil {
		// Spot account fetch failed entirely (network / auth / decoding).
		// Degrade to an empty spot wallet — a futures-only strategy MUST
		// still be able to start; the runtime already handles an empty
		// spot leg. Note: per-asset pricing failures are handled inside
		// fetchSpotAccount and do NOT bubble up here any more (see
		// canonical-wallet-display-boundary review #2: a single unpriced
		// dust asset must not block wallet fetch).
		logger.Warn(ctx, "system", fmt.Sprintf(
			"binance spot account fetch skipped for account=%d env=%s: %v",
			account.AccountID, a.env, err,
		))
		spot = domain.SpotWallet{}
	}

	// Canonical total_value uses the primary-margin-asset futures leg. When the
	// exchange account runs in multi-assets mode, Binance also publishes a
	// separate USD aggregate; we keep that in display-only fields instead of
	// changing strategy/runtime wallet semantics.
	spotValue := spotEstimatedValue(spot)
	totalValue := futures.MarginBalance + spotValue
	if futures.MarginBalance == 0 {
		totalValue = futures.TotalMarginBalance + spotValue
	}
	if futures.MarginBalance == 0 && futures.TotalMarginBalance == 0 {
		// fallback when neither margin balance field was populated
		totalValue = futures.WalletBalance + spotValue
	}

	return domain.OnlineAccountInfo{
		AccountID:        account.AccountID,
		Futures:          futures,
		Spot:             spot,
		TotalValue:       totalValue,
		WalletBalance:    futures.WalletBalance,
		AvailableBalance: futures.AvailableBalance,
		UpdatedAt:        time.Now().UTC(),
	}, nil
}

// --- Binance Futures v3 /fapi/v3/account ---

type binanceFuturesAccountV3 struct {
	TotalWalletBalance          string                       `json:"totalWalletBalance"`
	TotalUnrealizedProfit       string                       `json:"totalUnrealizedProfit"`
	TotalMarginBalance          string                       `json:"totalMarginBalance"`
	TotalPositionInitialMargin  string                       `json:"totalPositionInitialMargin"`
	TotalOpenOrderInitialMargin string                       `json:"totalOpenOrderInitialMargin"`
	TotalMaintMargin            string                       `json:"totalMaintMargin"`
	TotalCrossWalletBalance     string                       `json:"totalCrossWalletBalance"`
	TotalCrossUnPnl             string                       `json:"totalCrossUnPnl"`
	AvailableBalance            string                       `json:"availableBalance"`
	Assets                      []binanceFuturesAssetV3      `json:"assets"`
	Positions                   []binanceFuturesAccountPosV3 `json:"positions"`
}

type binanceMultiAssetsMode struct {
	MultiAssetsMargin bool `json:"multiAssetsMargin"`
}

type binanceFuturesAssetV3 struct {
	Asset            string `json:"asset"`
	WalletBalance    string `json:"walletBalance"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	MarginBalance    string `json:"marginBalance"`
}

// binanceFuturesAccountPosV3 is the position slice embedded in v3/account.
// It's less detailed than positionRisk (no liquidationPrice, no
// openOrderInitialMargin, no breakEvenPrice), but it's useful for early
// cross-validation.
type binanceFuturesAccountPosV3 struct {
	Symbol                 string `json:"symbol"`
	PositionSide           string `json:"positionSide"`
	PositionAmt            string `json:"positionAmt"`
	EntryPrice             string `json:"entryPrice"`
	InitialMargin          string `json:"initialMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	MaintMargin            string `json:"maintMargin"`
	UnrealizedProfit       string `json:"unrealizedProfit"`
	Notional               string `json:"notional"`
	IsolatedWallet         string `json:"isolatedWallet"`
}

func (a *BinanceAdapter) fetchFuturesAccount(ctx context.Context, account domain.Account) (domain.FuturesWallet, error) {
	resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v3/account", nil)
	if err != nil {
		return domain.FuturesWallet{}, err
	}

	var raw binanceFuturesAccountV3
	if err := json.Unmarshal(resp, &raw); err != nil {
		return domain.FuturesWallet{}, fmt.Errorf("decode /fapi/v3/account: %w", err)
	}

	// Ingress order enforced by canonical-wallet-display-boundary:
	//   1. provider raw -> canonical wallet fields (authoritative)
	//   2. provider raw -> display wallet fields (UI-only projection)
	// Step 2 runs AFTER step 1 and MUST NOT feed back into canonical fields.
	// If the display aggregation fails we log and continue with canonical —
	// display is an explanation layer, never a gating contract.

	// Step 1: canonical wallet (single-asset USDT@-M semantics).
	wallet := domain.FuturesWallet{
		MarginMode:                  "cross",
		PositionMode:                "one_way",
		WalletBalance:               parseFloat(raw.TotalWalletBalance),
		AvailableBalance:            parseFloat(raw.AvailableBalance),
		TotalUnrealizedPnl:          parseFloat(raw.TotalUnrealizedProfit),
		UnrealizedPnl:               parseFloat(raw.TotalUnrealizedProfit),
		TotalMarginBalance:          parseFloat(raw.TotalMarginBalance),
		MarginBalance:               parseFloat(raw.TotalMarginBalance),
		TotalPositionInitialMargin:  parseFloat(raw.TotalPositionInitialMargin),
		TotalOpenOrderInitialMargin: parseFloat(raw.TotalOpenOrderInitialMargin),
		TotalMaintMargin:            parseFloat(raw.TotalMaintMargin),
		TotalCrossWalletBalance:     parseFloat(raw.TotalCrossWalletBalance),
		TotalCrossUnPnl:             parseFloat(raw.TotalCrossUnPnl),
	}

	// Step 2: display-only USD projection. Seeded from provider totals so the
	// field has a sensible default even if multi-asset aggregation fails.
	wallet.DisplayWalletBalanceUsd = parseFloat(raw.TotalWalletBalance)
	wallet.DisplayMarginBalanceUsd = parseFloat(raw.TotalMarginBalance)
	wallet.DisplayUnrealizedPnlUsd = parseFloat(raw.TotalUnrealizedProfit)
	if err := a.applyFuturesDisplayUSDTotals(ctx, &wallet, raw.Assets); err != nil {
		logger.Warn(ctx, "system", fmt.Sprintf(
			"binance futures USD display aggregate skipped for account=%d env=%s: %v",
			account.AccountID, a.env, err,
		))
	}
	return wallet, nil
}

// --- Binance Futures v3 /fapi/v3/balance ---

type binanceFuturesBalanceV3 struct {
	Asset              string `json:"asset"`
	Balance            string `json:"balance"`
	CrossWalletBalance string `json:"crossWalletBalance"`
	CrossUnPnl         string `json:"crossUnPnl"`
	AvailableBalance   string `json:"availableBalance"`
	MarginAvailable    bool   `json:"marginAvailable"`
}

func (a *BinanceAdapter) fetchFuturesBalance(ctx context.Context, account domain.Account) ([]binanceFuturesBalanceV3, error) {
	resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v3/balance", nil)
	if err != nil {
		return nil, err
	}

	var raw []binanceFuturesBalanceV3
	if err := json.Unmarshal(resp, &raw); err != nil {
		return nil, fmt.Errorf("decode /fapi/v3/balance: %w", err)
	}
	return raw, nil
}

type binanceAssetIndex struct {
	Symbol string `json:"symbol"`
	Index  string `json:"index"`
}

func (a *BinanceAdapter) fetchMultiAssetsMode(ctx context.Context, account domain.Account) (bool, error) {
	resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v1/multiAssetsMargin", nil)
	if err != nil {
		return false, err
	}

	var raw binanceMultiAssetsMode
	if err := json.Unmarshal(resp, &raw); err != nil {
		return false, fmt.Errorf("decode /fapi/v1/multiAssetsMargin: %w", err)
	}
	return raw.MultiAssetsMargin, nil
}

func (a *BinanceAdapter) fetchFuturesAssetIndex(ctx context.Context) (map[string]float64, error) {
	resp, err := a.publicGet(ctx, a.futuresBase+"/fapi/v1/assetIndex", nil)
	if err != nil {
		return nil, err
	}

	var raw []binanceAssetIndex
	if err := json.Unmarshal(resp, &raw); err != nil {
		return nil, fmt.Errorf("decode /fapi/v1/assetIndex: %w", err)
	}

	out := make(map[string]float64, len(raw))
	for _, item := range raw {
		if item.Symbol == "" {
			continue
		}
		if price := parseFloat(item.Index); price > 0 {
			out[item.Symbol] = price
		}
	}
	return out, nil
}

// --- Binance Futures v3 /fapi/v3/positionRisk ---

type binanceFuturesPositionRiskV3 struct {
	Symbol                 string `json:"symbol"`
	PositionSide           string `json:"positionSide"`
	PositionAmt            string `json:"positionAmt"`
	EntryPrice             string `json:"entryPrice"`
	BreakEvenPrice         string `json:"breakEvenPrice"`
	MarkPrice              string `json:"markPrice"`
	UnrealizedProfit       string `json:"unRealizedProfit"`
	LiquidationPrice       string `json:"liquidationPrice"`
	MarginType             string `json:"marginType"`
	IsolatedMargin         string `json:"isolatedMargin"`
	IsolatedWallet         string `json:"isolatedWallet"`
	Notional               string `json:"notional"`
	InitialMargin          string `json:"initialMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	MaintMargin            string `json:"maintMargin"`
}

func (a *BinanceAdapter) fetchFuturesPositionRisk(ctx context.Context, account domain.Account) ([]domain.FuturesPosition, error) {
	resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v3/positionRisk", nil)
	if err != nil {
		return nil, err
	}

	var raw []binanceFuturesPositionRiskV3
	if err := json.Unmarshal(resp, &raw); err != nil {
		return nil, fmt.Errorf("decode /fapi/v3/positionRisk: %w", err)
	}

	out := make([]domain.FuturesPosition, 0, len(raw))
	for _, p := range raw {
		qty := parseFloat(p.PositionAmt)
		if qty == 0 {
			continue // skip flat positions
		}
		out = append(out, domain.FuturesPosition{
			Symbol:                 p.Symbol,
			PositionSide:           p.PositionSide,
			Qty:                    qty,
			PositionQty:            qty,
			EntryPrice:             parseFloat(p.EntryPrice),
			MarkPrice:              parseFloat(p.MarkPrice),
			UnrealizedPnl:          parseFloat(p.UnrealizedProfit),
			MarginType:             p.MarginType,
			MarginMode:             p.MarginType,
			Notional:               parseFloat(p.Notional),
			InitialMargin:          parseFloat(p.InitialMargin),
			PositionInitialMargin:  parseFloat(p.PositionInitialMargin),
			OpenOrderInitialMargin: parseFloat(p.OpenOrderInitialMargin),
			MaintMargin:            parseFloat(p.MaintMargin),
			IsolatedWallet:         parseFloat(p.IsolatedWallet),
			LiquidationPrice:       parseFloat(p.LiquidationPrice),
			BreakEvenPrice:         parseFloat(p.BreakEvenPrice),
		})
	}
	return out, nil
}

// --- Binance Futures supporting metadata ---

type binanceSymbolConfig struct {
	Symbol     string  `json:"symbol"`
	Leverage   float64 `json:"leverage"`
	MarginType string  `json:"marginType"`
}

type binanceLeverageBracket struct {
	Symbol   string                  `json:"symbol"`
	Brackets []binanceBracketDetails `json:"brackets"`
}

type binanceBracketDetails struct {
	Bracket          int     `json:"bracket"`
	InitialLeverage  int     `json:"initialLeverage"`
	NotionalCap      float64 `json:"notionalCap"`
	NotionalFloor    float64 `json:"notionalFloor"`
	MaintMarginRatio float64 `json:"maintMarginRatio"`
	Cum              float64 `json:"cum"`
}

type binanceExchangeInfo struct {
	Symbols []binanceExchangeInfoSymbol `json:"symbols"`
}

type binanceExchangeInfoSymbol struct {
	Symbol            string                  `json:"symbol"`
	PricePrecision    int                     `json:"pricePrecision"`
	QuantityPrecision int                     `json:"quantityPrecision"`
	Filters           []binanceExchangeFilter `json:"filters"`
}

type binanceExchangeFilter struct {
	FilterType string `json:"filterType"`
	TickSize   string `json:"tickSize,omitempty"`
	StepSize   string `json:"stepSize,omitempty"`
}

type phaseASymbolMetadata struct {
	Symbol            string
	MarginType        string
	InitialLeverage   float64
	PricePrecision    int
	QuantityPrecision int
	TickSize          float64
	StepSize          float64
	Brackets          []phaseABracketMetadata
}

type phaseABracketMetadata struct {
	Bracket          int
	InitialLeverage  float64
	NotionalCap      float64
	NotionalFloor    float64
	MaintMarginRatio float64
	Cumulative       float64
}

func (a *BinanceAdapter) fetchPhaseAMetadata(ctx context.Context, account domain.Account, symbols []string) (map[string]phaseASymbolMetadata, error) {
	symbols = normalizeSymbols(symbols)
	if len(symbols) == 0 {
		return nil, nil
	}

	symbolConfig, err := a.fetchSymbolConfig(ctx, account, symbols)
	if err != nil {
		return nil, err
	}
	leverageBrackets, err := a.fetchLeverageBrackets(ctx, account, symbols)
	if err != nil {
		return nil, err
	}
	exchangeInfo, err := a.fetchExchangeInfo(ctx)
	if err != nil {
		return nil, err
	}

	out := make(map[string]phaseASymbolMetadata)
	for _, item := range symbolConfig {
		meta := out[item.Symbol]
		meta.Symbol = item.Symbol
		meta.MarginType = item.MarginType
		meta.InitialLeverage = item.Leverage
		out[item.Symbol] = meta
	}
	for _, item := range leverageBrackets {
		meta := out[item.Symbol]
		meta.Symbol = item.Symbol
		meta.Brackets = make([]phaseABracketMetadata, 0, len(item.Brackets))
		for _, b := range item.Brackets {
			meta.Brackets = append(meta.Brackets, phaseABracketMetadata{
				Bracket:          b.Bracket,
				InitialLeverage:  float64(b.InitialLeverage),
				NotionalCap:      b.NotionalCap,
				NotionalFloor:    b.NotionalFloor,
				MaintMarginRatio: b.MaintMarginRatio,
				Cumulative:       b.Cum,
			})
		}
		out[item.Symbol] = meta
	}
	for _, item := range exchangeInfo.Symbols {
		if !symbolAllowed(item.Symbol, symbols) {
			continue
		}
		meta := out[item.Symbol]
		meta.Symbol = item.Symbol
		meta.PricePrecision = item.PricePrecision
		meta.QuantityPrecision = item.QuantityPrecision
		for _, filter := range item.Filters {
			switch filter.FilterType {
			case "PRICE_FILTER":
				meta.TickSize = parseFloat(filter.TickSize)
			case "LOT_SIZE":
				meta.StepSize = parseFloat(filter.StepSize)
			}
		}
		out[item.Symbol] = meta
	}
	return out, nil
}

func (a *BinanceAdapter) fetchSymbolConfig(ctx context.Context, account domain.Account, symbols []string) ([]binanceSymbolConfig, error) {
	symbols = normalizeSymbols(symbols)
	if len(symbols) == 0 {
		return nil, nil
	}
	if len(symbols) > maxTargetedMetadataSymbols {
		resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v1/symbolConfig", nil)
		if err != nil {
			return nil, err
		}

		var raw []binanceSymbolConfig
		if err := json.Unmarshal(resp, &raw); err != nil {
			return nil, fmt.Errorf("decode /fapi/v1/symbolConfig: %w", err)
		}
		return filterSymbolConfig(raw, symbols), nil
	}

	out := make([]binanceSymbolConfig, 0, len(symbols))
	for _, symbol := range symbols {
		resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v1/symbolConfig", url.Values{"symbol": {symbol}})
		if err != nil {
			return nil, err
		}

		var raw []binanceSymbolConfig
		if err := json.Unmarshal(resp, &raw); err != nil {
			return nil, fmt.Errorf("decode /fapi/v1/symbolConfig: %w", err)
		}
		out = append(out, raw...)
	}
	return out, nil
}

func (a *BinanceAdapter) fetchLeverageBrackets(ctx context.Context, account domain.Account, symbols []string) ([]binanceLeverageBracket, error) {
	symbols = normalizeSymbols(symbols)
	if len(symbols) == 0 {
		return nil, nil
	}
	if len(symbols) > maxTargetedMetadataSymbols {
		resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v1/leverageBracket", nil)
		if err != nil {
			return nil, err
		}

		var raw []binanceLeverageBracket
		if err := json.Unmarshal(resp, &raw); err != nil {
			return nil, fmt.Errorf("decode /fapi/v1/leverageBracket: %w", err)
		}
		return filterLeverageBrackets(raw, symbols), nil
	}

	out := make([]binanceLeverageBracket, 0, len(symbols))
	for _, symbol := range symbols {
		resp, err := a.signedGet(ctx, account, a.futuresBase+"/fapi/v1/leverageBracket", url.Values{"symbol": {symbol}})
		if err != nil {
			return nil, err
		}

		var raw []binanceLeverageBracket
		if err := json.Unmarshal(resp, &raw); err != nil {
			return nil, fmt.Errorf("decode /fapi/v1/leverageBracket: %w", err)
		}
		out = append(out, raw...)
	}
	return out, nil
}

func (a *BinanceAdapter) fetchExchangeInfo(ctx context.Context) (binanceExchangeInfo, error) {
	resp, err := a.publicGet(ctx, a.futuresBase+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return binanceExchangeInfo{}, err
	}

	var raw binanceExchangeInfo
	if err := json.Unmarshal(resp, &raw); err != nil {
		return binanceExchangeInfo{}, fmt.Errorf("decode /fapi/v1/exchangeInfo: %w", err)
	}
	return raw, nil
}

func applyPositionMetadata(positions []domain.FuturesPosition, metadata map[string]phaseASymbolMetadata) {
	for i := range positions {
		meta, ok := metadata[positions[i].Symbol]
		if !ok {
			continue
		}
		if positions[i].MarginType == "" {
			positions[i].MarginType = meta.MarginType
		}
		if positions[i].MarginMode == "" {
			positions[i].MarginMode = positions[i].MarginType
		}
		if positions[i].Leverage == 0 && meta.InitialLeverage > 0 {
			positions[i].Leverage = meta.InitialLeverage
		}
	}
}

func symbolsFromPositions(positions []domain.FuturesPosition) []string {
	symbols := make([]string, 0, len(positions))
	for _, pos := range positions {
		symbol := strings.TrimSpace(strings.ToUpper(pos.Symbol))
		if symbol == "" {
			continue
		}
		symbols = append(symbols, symbol)
	}
	return normalizeSymbols(symbols)
}

func normalizeSymbols(symbols []string) []string {
	if len(symbols) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, raw := range symbols {
		symbol := strings.TrimSpace(strings.ToUpper(raw))
		if symbol == "" {
			continue
		}
		if _, ok := seen[symbol]; ok {
			continue
		}
		seen[symbol] = struct{}{}
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func symbolAllowed(symbol string, allowlist []string) bool {
	symbol = strings.TrimSpace(strings.ToUpper(symbol))
	for _, allowed := range allowlist {
		if symbol == allowed {
			return true
		}
	}
	return false
}

func filterSymbolConfig(items []binanceSymbolConfig, symbols []string) []binanceSymbolConfig {
	out := make([]binanceSymbolConfig, 0, len(symbols))
	for _, item := range items {
		if symbolAllowed(item.Symbol, symbols) {
			out = append(out, item)
		}
	}
	return out
}

func filterLeverageBrackets(items []binanceLeverageBracket, symbols []string) []binanceLeverageBracket {
	out := make([]binanceLeverageBracket, 0, len(symbols))
	for _, item := range items {
		if symbolAllowed(item.Symbol, symbols) {
			out = append(out, item)
		}
	}
	return out
}

func metadataToDomainRiskMetadata(metadata map[string]phaseASymbolMetadata) []domain.FuturesRiskMetadata {
	if len(metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(metadata))
	for symbol := range metadata {
		keys = append(keys, symbol)
	}
	sort.Strings(keys)

	out := make([]domain.FuturesRiskMetadata, 0, len(keys))
	for _, symbol := range keys {
		meta := metadata[symbol]
		item := domain.FuturesRiskMetadata{
			Symbol:               meta.Symbol,
			ConfiguredLeverage:   meta.InitialLeverage,
			ConfiguredMarginMode: meta.MarginType,
			PricePrecision:       int32(meta.PricePrecision),
			QuantityPrecision:    int32(meta.QuantityPrecision),
			TickSize:             meta.TickSize,
			StepSize:             meta.StepSize,
		}
		if len(meta.Brackets) > 0 {
			item.Brackets = make([]domain.FuturesRiskBracket, 0, len(meta.Brackets))
			for _, bracket := range meta.Brackets {
				item.Brackets = append(item.Brackets, domain.FuturesRiskBracket{
					Bracket:          int32(bracket.Bracket),
					NotionalFloor:    bracket.NotionalFloor,
					NotionalCap:      bracket.NotionalCap,
					InitialLeverage:  bracket.InitialLeverage,
					MaintMarginRatio: bracket.MaintMarginRatio,
					Cumulative:       bracket.Cumulative,
				})
			}
		}
		out = append(out, item)
	}
	return out
}

// --- Binance Spot API ---

type binanceSpotAccount struct {
	Balances []binanceBalance `json:"balances"`
}

type binanceBalance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

func (a *BinanceAdapter) fetchSpotAccount(ctx context.Context, account domain.Account) (domain.SpotWallet, error) {
	resp, err := a.signedGet(ctx, account, a.spotBase+"/api/v3/account", nil)
	if err != nil {
		return domain.SpotWallet{}, err
	}

	var raw binanceSpotAccount
	if err := json.Unmarshal(resp, &raw); err != nil {
		return domain.SpotWallet{}, fmt.Errorf("decode spot account: %w", err)
	}

	wallet := domain.SpotWallet{}
	for _, b := range raw.Balances {
		free := parseFloat(b.Free)
		locked := parseFloat(b.Locked)
		if free == 0 && locked == 0 {
			continue
		}
		if b.Asset == "USDT" {
			wallet.Free += free
			wallet.Locked += locked
			continue
		}
		wallet.Assets = append(wallet.Assets, domain.SpotAsset{
			Symbol: b.Asset,
			Qty:    free,
			Locked: locked,
		})
	}
	a.enrichSpotAssetPrices(ctx, &wallet)
	return wallet, nil
}

type binanceTickerPrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// enrichSpotAssetPrices resolves a USD price for each non-USDT spot asset.
//
// Per canonical-wallet-display-boundary: a single unpriced dust asset MUST
// NOT block wallet retrieval. The canonical spot runtime handles
// `Price == nil` by falling back to the cash (free+locked) leg, and
// futures-only strategies don't depend on spot assets at all. So we:
//
//  1. try to price every asset
//  2. log per-asset pricing failures as warnings
//  3. leave `Assets[i].Price = nil` for the ones we couldn't price
//  4. never return an error from this step
//
// Wallet ingress is intentionally more permissive than wallet ingestion used
// to be; the price resolution is a best-effort enrichment, not a gate.
func (a *BinanceAdapter) enrichSpotAssetPrices(ctx context.Context, wallet *domain.SpotWallet) {
	if wallet == nil {
		return
	}
	for i := range wallet.Assets {
		if wallet.Assets[i].Price != nil {
			continue
		}
		price, err := a.lookupSpotAssetPrice(ctx, wallet.Assets[i].Symbol)
		if err != nil {
			logger.Warn(ctx, "system", fmt.Sprintf(
				"binance spot asset %s left unpriced (env=%s): %v",
				wallet.Assets[i].Symbol, a.env, err,
			))
			continue
		}
		wallet.Assets[i].Price = floatPtr(price)
	}
}

func (a *BinanceAdapter) lookupSpotAssetPrice(ctx context.Context, asset string) (float64, error) {
	if asset == "" || asset == "USDT" {
		return 0, fmt.Errorf("unsupported asset %q", asset)
	}
	var lastErr error
	if price, err := a.fetchPublicSpotTickerPrice(ctx, asset+"USDT"); err == nil && price > 0 {
		return price, nil
	} else if err != nil {
		lastErr = err
	}
	if price, err := a.fetchPublicSpotTickerPrice(ctx, "USDT"+asset); err == nil && price > 0 {
		return 1 / price, nil
	} else if err != nil {
		lastErr = errors.Join(lastErr, err)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no usable ticker for asset %s", asset)
	}
	return 0, lastErr
}

func (a *BinanceAdapter) fetchPublicSpotTickerPrice(ctx context.Context, symbol string) (float64, error) {
	resp, err := a.publicGet(ctx, a.spotBase+"/api/v3/ticker/price", url.Values{"symbol": {symbol}})
	if err != nil {
		return 0, err
	}
	var raw binanceTickerPrice
	if err := json.Unmarshal(resp, &raw); err != nil {
		return 0, fmt.Errorf("decode /api/v3/ticker/price: %w", err)
	}
	price := parseFloat(raw.Price)
	if price <= 0 {
		return 0, fmt.Errorf("ticker price for %s is non-positive", symbol)
	}
	return price, nil
}

// --- HTTP signing helper ---

func (a *BinanceAdapter) publicGet(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if encoded := params.Encode(); encoded != "" {
		req.URL.RawQuery = encoded
	}

	resp, err := a.httpClient.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("http get %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance API error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// signedGet signs the request with the credentials on the given account.
// Adapters do NOT hold long-lived credentials; the HMAC key is picked from
// the account on every call.
func (a *BinanceAdapter) signedGet(ctx context.Context, account domain.Account, endpoint string, params url.Values) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	payload := params.Encode()
	sig := signHMAC(account.APISecret, payload)
	rawQuery := payload
	if rawQuery != "" {
		rawQuery += "&"
	}
	// Binance validates the signature against the original query string. Keep
	// signature appended last instead of re-encoding url.Values, which would
	// sort "signature" ahead of other params and break demo futures auth.
	rawQuery += "signature=" + url.QueryEscape(sig)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = rawQuery
	req.Header.Set("X-MBX-APIKEY", account.APIKey)

	resp, err := a.httpClient.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("http get %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance API error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func signHMAC(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// --- helpers ---

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		logger.Warn(context.Background(), "system", fmt.Sprintf("parseFloat: cannot parse %q: %v", s, err))
	}
	return v
}

func mergeFuturesBalanceSummary(wallet *domain.FuturesWallet, balances []binanceFuturesBalanceV3) {
	summary := pickPrimaryFuturesBalance(balances)
	if summary == nil {
		return
	}
	summaryWallet := parseFloat(summary.Balance)
	summaryAvailable := parseFloat(summary.AvailableBalance)
	summaryCrossWallet := parseFloat(summary.CrossWalletBalance)
	summaryCrossUnPnl := parseFloat(summary.CrossUnPnl)
	summaryMargin := summaryWallet + summaryCrossUnPnl

	if wallet.MultiAssetsMode {
		wallet.WalletBalance = summaryWallet
		wallet.AvailableBalance = summaryAvailable
		wallet.UnrealizedPnl = summaryCrossUnPnl
		wallet.TotalUnrealizedPnl = summaryCrossUnPnl
		wallet.MarginBalance = summaryMargin
		wallet.TotalMarginBalance = summaryMargin
		wallet.TotalCrossWalletBalance = summaryCrossWallet
		wallet.TotalCrossUnPnl = summaryCrossUnPnl
		return
	}
	if wallet.WalletBalance == 0 {
		wallet.WalletBalance = summaryWallet
	} else if !roughlyEqual(wallet.WalletBalance, summaryWallet) {
		logger.Warn(context.Background(), "system", fmt.Sprintf(
			"binance futures balance mismatch: wallet=%.10f balance=%.10f asset=%s",
			wallet.WalletBalance, summaryWallet, summary.Asset,
		))
	}
	if wallet.AvailableBalance == 0 {
		wallet.AvailableBalance = summaryAvailable
	}
	if wallet.TotalCrossWalletBalance == 0 {
		wallet.TotalCrossWalletBalance = summaryCrossWallet
	}
	if wallet.TotalCrossUnPnl == 0 {
		wallet.TotalCrossUnPnl = summaryCrossUnPnl
	}
}

func (a *BinanceAdapter) applyFuturesDisplayUSDTotals(ctx context.Context, wallet *domain.FuturesWallet, assets []binanceFuturesAssetV3) error {
	if wallet == nil || len(assets) == 0 {
		return nil
	}

	needsIndex := false
	for _, asset := range assets {
		if parseFloat(asset.WalletBalance) == 0 && parseFloat(asset.MarginBalance) == 0 && parseFloat(asset.UnrealizedProfit) == 0 {
			continue
		}
		// Non-USDT assets — both volatile and non-USDT stablecoins — need the
		// futures asset-index to convert accurately. USDT is the reference
		// currency in this system so it stays at rate=1 without a lookup.
		if asset.Asset == "USDT" {
			continue
		}
		needsIndex = true
		break
	}

	var indexBySymbol map[string]float64
	var err error
	if needsIndex {
		indexBySymbol, err = a.fetchFuturesAssetIndex(ctx)
		if err != nil {
			return err
		}
	}

	var walletUSD float64
	var marginUSD float64
	var unrealizedUSD float64
	for _, asset := range assets {
		rate, err := a.futuresDisplayUSDRate(ctx, asset.Asset, indexBySymbol)
		if err != nil {
			return err
		}
		walletUSD += parseFloat(asset.WalletBalance) * rate
		marginUSD += parseFloat(asset.MarginBalance) * rate
		unrealizedUSD += parseFloat(asset.UnrealizedProfit) * rate
	}

	wallet.DisplayWalletBalanceUsd = walletUSD
	wallet.DisplayMarginBalanceUsd = marginUSD
	wallet.DisplayUnrealizedPnlUsd = unrealizedUSD
	return nil
}

func pickPrimaryFuturesBalance(balances []binanceFuturesBalanceV3) *binanceFuturesBalanceV3 {
	if len(balances) == 0 {
		return nil
	}
	for i := range balances {
		if balances[i].Asset == "USDT" {
			return &balances[i]
		}
	}
	for i := range balances {
		if balances[i].MarginAvailable {
			return &balances[i]
		}
	}
	return &balances[0]
}

func roughlyEqual(a, b float64) bool {
	diff := math.Abs(a - b)
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return diff <= scale*1e-9
}

func spotEstimatedValue(w domain.SpotWallet) float64 {
	total := w.Free + w.Locked
	for _, a := range w.Assets {
		if a.Price != nil {
			total += (a.Qty + a.Locked) * (*a.Price)
		}
	}
	return total
}

func floatPtr(v float64) *float64 {
	x := v
	return &x
}

// futuresDisplayUSDRate resolves the USD-denominated conversion rate for
// `asset` used by the futures display-USD aggregation.
//
// Resolution order:
//  1. USDT — the reference currency in this system, always returns 1.0.
//  2. Known stablecoin — uses the live Binance spot ticker (`<ASSET>USDT`)
//     so USDC / BUSD / FDUSD / TUSD etc. reflect their actual market deviation
//     (typically within 0.15%); a previously-hardcoded 1.0 made the display
//     USD totals diverge from the Binance UI by a few basis points on
//     large stablecoin balances. Falls back to 1.0 with a warning when the
//     spot ticker is unavailable.
//  3. Volatile asset — uses the futures `assetIndex` endpoint
//     (`indexBySymbol[asset+"USD"]`).
func (a *BinanceAdapter) futuresDisplayUSDRate(
	ctx context.Context, asset string, indexBySymbol map[string]float64,
) (float64, error) {
	if asset == "USDT" {
		return 1, nil
	}
	if isKnownStablecoin(asset) {
		// Stablecoins trade on Binance spot as ``<STABLE>USDT`` (e.g. USDCUSDT).
		// The price is the stablecoin's USD value, since USDT is our $1 proxy.
		if price, err := a.fetchPublicSpotTickerPrice(ctx, asset+"USDT"); err == nil && price > 0 {
			return price, nil
		}
		logger.Warn(ctx, "system", fmt.Sprintf(
			"binance stablecoin display rate fallback to 1.0 for %s (spot ticker unavailable)",
			asset,
		))
		return 1, nil
	}
	if rate := indexBySymbol[asset+"USD"]; rate > 0 {
		return rate, nil
	}
	return 0, fmt.Errorf("missing USD conversion rate for futures asset %s", asset)
}

// isKnownStablecoin returns true for tokens typically pegged to $1 that Binance
// quotes against USDT on spot. Used to decide whether to look up the real spot
// price vs. treating it as a volatile asset in the futures asset-index.
func isKnownStablecoin(asset string) bool {
	switch asset {
	case "USDC", "BUSD", "FDUSD", "USDP", "TUSD", "USDS", "USD1", "UUSD":
		return true
	default:
		return false
	}
}
