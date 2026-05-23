package walletmetrics

import (
	"math"
	"strings"

	"github.com/hushine-tech/core-service/internal/domain"
)

const qtyEps = 1e-12

// SpotEstimatedExchangeAligned matches exchange.binance spotEstimatedValue: free+locked USDT shell
// plus (qty+locked)*mark for assets that have a mark price set.
func SpotEstimatedExchangeAligned(sw domain.SpotWallet) float64 {
	total := sw.Free + sw.Locked
	for _, a := range sw.Assets {
		if a.Price != nil {
			total += (a.Qty + a.Locked) * (*a.Price)
		}
	}
	return total
}

// SumMatchesTotal checks spot+futures display sum against total_value (tolerates float noise).
func SumMatchesTotal(sum, total float64) bool {
	const absTol = 0.05
	const relTol = 1e-8
	d := math.Abs(sum - total)
	if d <= absTol {
		return true
	}
	s := math.Max(math.Abs(sum), math.Abs(total))
	if s <= absTol {
		return d <= absTol
	}
	return d/s <= relTol
}

// FuturesPositionEquityDomain mirrors handler walletagg logic on domain types (backtest / DB snapshots).
func FuturesPositionEquityDomain(fw domain.FuturesWallet) float64 {
	mode := strings.ToLower(strings.TrimSpace(fw.MarginMode))
	pos := fw.Positions
	switch mode {
	case "cross":
		if len(pos) == 0 {
			return fw.InitialBalance
		}
		wb := fw.WalletBalance
		upnl := fw.TotalUnrealizedPnl
		im := 0.0
		for _, p := range pos {
			if math.Abs(p.Qty) <= qtyEps {
				continue
			}
			lev := p.Leverage
			if lev <= 0 {
				continue
			}
			mark := p.MarkPrice
			if mark == 0 {
				mark = p.EntryPrice
			}
			im += math.Abs(p.Qty) * mark / lev
		}
		if wb == 0 && upnl == 0 && im == 0 && fw.InitialBalance > 0 {
			return fw.InitialBalance
		}
		return wb + upnl + im
	default:
		sum := 0.0
		for _, p := range pos {
			if math.Abs(p.Qty) <= qtyEps {
				sum += p.InitialBalance
				continue
			}
			im := 0.0
			if p.Leverage > 0 && p.EntryPrice > 0 {
				im = math.Abs(p.Qty) * p.EntryPrice / p.Leverage
			}
			sum += im + p.InitialBalance + p.UnrealizedPnl
		}
		return sum
	}
}

// IsolatedPositionDisplayEquity is a per-row estimate for isolated margin (IM + shell + unrealized).
func IsolatedPositionDisplayEquity(p domain.FuturesPosition) float64 {
	if math.Abs(p.Qty) <= qtyEps {
		return p.InitialBalance
	}
	im := 0.0
	if p.Leverage > 0 && p.EntryPrice > 0 {
		im = math.Abs(p.Qty) * p.EntryPrice / p.Leverage
	}
	return im + p.InitialBalance + p.UnrealizedPnl
}

// Bundle is server-computed display metrics attached to AccountWalletState.
type Bundle struct {
	SpotEstimated float64
	FuturesEquity float64
	Authoritative bool
	// Per futures position optional display_equity (isolated only); same length as futures.Positions when non-nil.
	PositionDisplay []*float64
}

// ComputeDisplay derives spot/futures breakdown aligned with how total_value is produced for each mode.
func ComputeDisplay(info domain.OnlineAccountInfo) Bundle {
	se := SpotEstimatedExchangeAligned(info.Spot)
	var fe float64

	switch info.Mode {
	case domain.AccountModeBinanceLive, domain.AccountModeBinanceTestnet:
		fe = info.Futures.MarginBalance
		if fe == 0 {
			fe = info.Futures.TotalMarginBalance
		}
		if fe == 0 {
			fe = info.Futures.WalletBalance
		}
	default:
		fe = FuturesPositionEquityDomain(info.Futures)
		if !SumMatchesTotal(se+fe, info.TotalValue) {
			fe2 := info.TotalValue - se
			if SumMatchesTotal(se+fe2, info.TotalValue) {
				fe = fe2
			}
		}
	}

	auth := SumMatchesTotal(se+fe, info.TotalValue)
	posDisp := positionDisplaySlice(info.Futures)
	return Bundle{
		SpotEstimated:   se,
		FuturesEquity:   fe,
		Authoritative:   auth,
		PositionDisplay: posDisp,
	}
}

func positionDisplaySlice(fw domain.FuturesWallet) []*float64 {
	mm := strings.ToLower(strings.TrimSpace(fw.MarginMode))
	if mm != "isolated" {
		return nil
	}
	out := make([]*float64, len(fw.Positions))
	for i := range fw.Positions {
		v := IsolatedPositionDisplayEquity(fw.Positions[i])
		x := v
		out[i] = &x
	}
	return out
}
