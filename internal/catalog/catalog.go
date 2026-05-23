package catalog

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

// Market identifies which symbol universe to query.
type Market string

const (
	MarketSpot        Market = "spot"
	MarketUSDMFutures Market = "usdm_futures"
)

// Catalog holds cached Binance symbol lists with TTL-based refresh.
type Catalog struct {
	mu sync.RWMutex

	spotSymbols []string
	futSymbols  []string

	spotFetchedAt time.Time
	futFetchedAt  time.Time

	ttl time.Duration

	// refreshMu serializes cache refresh per market to prevent thundering herd.
	spotRefreshMu sync.Mutex
	futRefreshMu  sync.Mutex

	fetchSpot func(ctx context.Context) ([]string, error)
	fetchFut  func(ctx context.Context) ([]string, error)
}

// New creates a catalog with default public Binance REST fetchers.
func New(ttl time.Duration, logger elog.Logger) *Catalog {
	if ttl <= 0 {
		ttl = 6 * time.Hour
	}
	client := httpclient.New(&http.Client{Timeout: 25 * time.Second}, logger, "symbol_catalog")
	return &Catalog{
		ttl: ttl,
		fetchSpot: func(ctx context.Context) ([]string, error) {
			return fetchSpotSymbolsHTTP(ctx, client)
		},
		fetchFut: func(ctx context.Context) ([]string, error) {
			return fetchFuturesSymbolsHTTP(ctx, client)
		},
	}
}

// NewWithFetchers is for tests.
func NewWithFetchers(ttl time.Duration, spot, fut func(ctx context.Context) ([]string, error)) *Catalog {
	if ttl <= 0 {
		ttl = 6 * time.Hour
	}
	return &Catalog{
		ttl:       ttl,
		fetchSpot: spot,
		fetchFut:  fut,
	}
}

// ParseMarket normalizes request market string.
func ParseMarket(s string) (Market, error) {
	m := strings.ToLower(strings.TrimSpace(s))
	switch m {
	case "spot":
		return MarketSpot, nil
	case "usdm_futures", "futures", "usdm":
		return MarketUSDMFutures, nil
	default:
		return "", errInvalidMarket
	}
}

// List returns symbols for market filtered by query (case-insensitive substring) and capped by limit.
func (c *Catalog) List(ctx context.Context, market Market, query string, limit int) (symbols []string, stale bool, err error) {
	if limit <= 0 || limit > 500 {
		limit = 80
	}
	q := strings.TrimSpace(strings.ToUpper(query))

	var list []string
	var fetchedAt time.Time
	var fetchFn func(context.Context) ([]string, error)
	var refreshMu *sync.Mutex

	switch market {
	case MarketSpot:
		fetchFn = c.fetchSpot
		refreshMu = &c.spotRefreshMu
		c.mu.RLock()
		list = append([]string(nil), c.spotSymbols...)
		fetchedAt = c.spotFetchedAt
		c.mu.RUnlock()
	case MarketUSDMFutures:
		fetchFn = c.fetchFut
		refreshMu = &c.futRefreshMu
		c.mu.RLock()
		list = append([]string(nil), c.futSymbols...)
		fetchedAt = c.futFetchedAt
		c.mu.RUnlock()
	default:
		return nil, false, errInvalidMarket
	}

	needRefresh := len(list) == 0 || time.Since(fetchedAt) > c.ttl
	stale = false
	if needRefresh && fetchFn != nil {
		// Serialize refresh so only one goroutine fetches; others wait and reuse result.
		refreshMu.Lock()
		defer refreshMu.Unlock()

		// Re-check under the refresh lock: another goroutine may have already refreshed.
		c.mu.RLock()
		switch market {
		case MarketSpot:
			list = append([]string(nil), c.spotSymbols...)
			fetchedAt = c.spotFetchedAt
		case MarketUSDMFutures:
			list = append([]string(nil), c.futSymbols...)
			fetchedAt = c.futFetchedAt
		}
		c.mu.RUnlock()

		if len(list) == 0 || time.Since(fetchedAt) > c.ttl {
			fresh, ferr := fetchFn(ctx)
			if ferr == nil && len(fresh) > 0 {
				c.mu.Lock()
				switch market {
				case MarketSpot:
					c.spotSymbols = fresh
					c.spotFetchedAt = time.Now()
					list = append([]string(nil), c.spotSymbols...)
				case MarketUSDMFutures:
					c.futSymbols = fresh
					c.futFetchedAt = time.Now()
					list = append([]string(nil), c.futSymbols...)
				}
				c.mu.Unlock()
			} else {
				if len(list) > 0 {
					stale = true
				} else {
					return nil, false, ferr
				}
			}
		}
	}

	out := filterSymbols(list, q, limit)
	return out, stale, nil
}
