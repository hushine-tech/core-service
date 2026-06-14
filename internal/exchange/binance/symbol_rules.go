package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type symbolRulesReader struct {
	baseURL string
}

type backtestSymbolRulesReader struct{}

func (backtestSymbolRulesReader) ReadSymbolRules(context.Context, adapter.SymbolRulesRequest) (adapter.SymbolRules, error) {
	return adapter.SymbolRules{}, nil
}

func (r symbolRulesReader) ReadSymbolRules(ctx context.Context, req adapter.SymbolRulesRequest) (adapter.SymbolRules, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(r.baseURL, "/")+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return adapter.SymbolRules{}, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(httpReq)
	if err != nil {
		return adapter.SymbolRules{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return adapter.SymbolRules{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return adapter.SymbolRules{}, fmt.Errorf("binance exchangeInfo HTTP %d: %s", resp.StatusCode, string(body))
	}
	return parseSymbolRules(body, req.Symbols)
}

type exchangeInfoResponse struct {
	Symbols []exchangeInfoSymbol `json:"symbols"`
}

type exchangeInfoSymbol struct {
	Symbol  string               `json:"symbol"`
	Filters []exchangeInfoFilter `json:"filters"`
}

type exchangeInfoFilter struct {
	FilterType     string `json:"filterType"`
	TickSize       string `json:"tickSize,omitempty"`
	StepSize       string `json:"stepSize,omitempty"`
	MinQty         string `json:"minQty,omitempty"`
	MinNotional    string `json:"notional,omitempty"`
	MinNotionalAlt string `json:"minNotional,omitempty"`
}

func parseSymbolRules(body []byte, symbols []string) (adapter.SymbolRules, error) {
	var raw exchangeInfoResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return adapter.SymbolRules{}, fmt.Errorf("decode binance exchangeInfo: %w", err)
	}
	allowed := symbolSet(symbols)
	rules := make([]adapter.SymbolRule, 0, len(symbols))
	for _, item := range raw.Symbols {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if len(allowed) > 0 && !allowed[symbol] {
			continue
		}
		rule := adapter.SymbolRule{
			Symbol: symbol,
			Market: domain.MarketPerpetualFutures,
		}
		for _, filter := range item.Filters {
			switch filter.FilterType {
			case "PRICE_FILTER":
				rule.TickSize = parsePositiveFloat(filter.TickSize)
			case "LOT_SIZE":
				rule.StepSize = parsePositiveFloat(filter.StepSize)
				rule.MinQty = parsePositiveFloat(filter.MinQty)
			case "MIN_NOTIONAL":
				rule.MinNotional = parsePositiveFloat(firstNonEmpty(filter.MinNotional, filter.MinNotionalAlt))
			}
		}
		rules = append(rules, rule)
	}
	return adapter.SymbolRules{Symbols: rules}, nil
}

func symbolSet(symbols []string) map[string]bool {
	out := make(map[string]bool, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol == "" {
			continue
		}
		out[symbol] = true
	}
	return out
}

func parsePositiveFloat(raw string) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value < 0 {
		return 0
	}
	return value
}
