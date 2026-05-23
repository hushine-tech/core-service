package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
)

const (
	spotExchangeInfoURL = "https://api.binance.com/api/v3/exchangeInfo"
	futExchangeInfoURL  = "https://fapi.binance.com/fapi/v1/exchangeInfo"
)

type spotExInfo struct {
	Symbols []struct {
		Symbol     string `json:"symbol"`
		Status     string `json:"status"`
		QuoteAsset string `json:"quoteAsset"`
	} `json:"symbols"`
}

type futExInfo struct {
	Symbols []struct {
		Symbol       string `json:"symbol"`
		Status       string `json:"status"`
		ContractType string `json:"contractType"`
		QuoteAsset   string `json:"quoteAsset"`
	} `json:"symbols"`
}

func fetchSpotSymbolsHTTP(ctx context.Context, client *httpclient.Client) ([]string, error) {
	body, err := httpGet(ctx, client, spotExchangeInfoURL)
	if err != nil {
		return nil, err
	}
	var info spotExInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("decode spot exchangeInfo: %w", err)
	}
	var out []string
	for _, s := range info.Symbols {
		if s.Status != "TRADING" || s.QuoteAsset != "USDT" {
			continue
		}
		out = append(out, s.Symbol)
	}
	sort.Strings(out)
	return dedupSorted(out), nil
}

func fetchFuturesSymbolsHTTP(ctx context.Context, client *httpclient.Client) ([]string, error) {
	body, err := httpGet(ctx, client, futExchangeInfoURL)
	if err != nil {
		return nil, err
	}
	var info futExInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("decode futures exchangeInfo: %w", err)
	}
	var out []string
	for _, s := range info.Symbols {
		if s.Status != "TRADING" || s.QuoteAsset != "USDT" || s.ContractType != "PERPETUAL" {
			continue
		}
		out = append(out, s.Symbol)
	}
	sort.Strings(out)
	return dedupSorted(out), nil
}

func httpGet(ctx context.Context, client *httpclient.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for i := 1; i < len(in); i++ {
		if in[i] != out[len(out)-1] {
			out = append(out, in[i])
		}
	}
	return out
}
