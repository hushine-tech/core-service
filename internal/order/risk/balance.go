package risk

import (
	"fmt"
	"math"
	"strings"
)

func validateSpotBalance(req ReviewRequest, snapshot Snapshot) Violation {
	if req.ReduceOnly && strings.EqualFold(req.Side, "BUY") {
		return Violation{Code: "SPOT_REDUCE_ONLY_BUY", Message: "spot reduce_only only supports SELL"}
	}
	base := baseAsset(req.Symbol)
	quote := quoteAsset(req.Symbol)
	if base == "" || quote == "" {
		return Violation{Code: "SYMBOL_RULES_MISSING", Message: "cannot infer base asset from symbol"}
	}
	if strings.EqualFold(req.Side, "SELL") {
		available, ok := availableBalance(snapshot, base)
		if !ok || available+riskEpsilon < math.Abs(req.Qty) {
			return Violation{
				Code:    "INSUFFICIENT_UNLOCKED_QTY",
				Message: fmt.Sprintf("available %s qty is insufficient", base),
			}
		}
	}
	if strings.EqualFold(req.Side, "BUY") {
		notional := orderNotional(req)
		if notional <= 0 {
			return Violation{Code: "PRICE_REQUIRED_FOR_RISK", Message: "positive price or mark_price is required for spot buy risk check"}
		}
		available, ok := availableBalance(snapshot, quote)
		if !ok || available+riskEpsilon < notional {
			return Violation{
				Code:    "INSUFFICIENT_QUOTE_BALANCE",
				Message: fmt.Sprintf("available %s balance is below required notional %g", quote, notional),
			}
		}
	}
	return Violation{}
}

func validateFuturesBalance(req ReviewRequest, snapshot Snapshot) Violation {
	metadata, ok := futuresMetadata(snapshot, req.Symbol)
	if !ok || metadata.ConfiguredLeverage <= 0 {
		return Violation{Code: "RISK_METADATA_MISSING", Message: "futures risk metadata is missing"}
	}
	if violation := validateSymbolRules(req, metadata); violation.Code != "" {
		return violation
	}
	if req.ReduceOnly {
		return validateFuturesReduceOnly(req, snapshot)
	}
	notional := orderNotional(req)
	if notional <= 0 {
		return Violation{Code: "PRICE_REQUIRED_FOR_RISK", Message: "positive price or mark_price is required for futures risk check"}
	}
	requiredMargin := notional / metadata.ConfiguredLeverage
	if snapshot.AvailableBalance+riskEpsilon < requiredMargin {
		return Violation{
			Code:    "INSUFFICIENT_AVAILABLE_BALANCE",
			Message: fmt.Sprintf("available_balance %g is below required_margin %g", snapshot.AvailableBalance, requiredMargin),
		}
	}
	return Violation{}
}

func validateFuturesReduceOnly(req ReviewRequest, snapshot Snapshot) Violation {
	position, ok := futuresPosition(snapshot, req.Symbol, req.PositionSide)
	if !ok || math.Abs(position.Qty) <= riskEpsilon {
		return Violation{Code: "REDUCE_ONLY_POSITION_MISSING", Message: "reduce_only requires an existing position"}
	}
	qty := math.Abs(req.Qty)
	if strings.EqualFold(req.Side, "SELL") {
		if position.Qty <= 0 {
			return Violation{Code: "REDUCE_ONLY_POSITION_MISMATCH", Message: "reduce_only SELL requires a long position"}
		}
	} else if strings.EqualFold(req.Side, "BUY") {
		if position.Qty >= 0 {
			return Violation{Code: "REDUCE_ONLY_POSITION_MISMATCH", Message: "reduce_only BUY requires a short position"}
		}
	}
	if qty > math.Abs(position.Qty)+riskEpsilon {
		return Violation{
			Code:    "REDUCE_ONLY_QTY_EXCEEDS_POSITION",
			Message: fmt.Sprintf("reduce_only qty %g exceeds position qty %g", qty, math.Abs(position.Qty)),
		}
	}
	return Violation{}
}

func availableBalance(snapshot Snapshot, asset string) (float64, bool) {
	asset = strings.ToUpper(strings.TrimSpace(asset))
	for _, item := range snapshot.Balances {
		if strings.ToUpper(strings.TrimSpace(item.Asset)) == asset {
			return item.Available, true
		}
	}
	return 0, false
}

func futuresMetadata(snapshot Snapshot, symbol string) (FuturesRiskMetadata, bool) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	for _, item := range snapshot.FuturesRiskMetadata {
		if strings.ToUpper(strings.TrimSpace(item.Symbol)) == symbol {
			return item, true
		}
	}
	return FuturesRiskMetadata{}, false
}

func futuresPosition(snapshot Snapshot, symbol string, positionSide int32) (Position, bool) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	wantSide := positionSideText(positionSide)
	for _, item := range snapshot.Positions {
		if strings.ToUpper(strings.TrimSpace(item.Symbol)) != symbol {
			continue
		}
		itemSide := strings.ToUpper(strings.TrimSpace(item.PositionSide))
		if itemSide == "" {
			itemSide = "BOTH"
		}
		if itemSide == wantSide {
			return item, true
		}
	}
	return Position{}, false
}

func baseAsset(symbol string) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	for _, quote := range quoteAssets() {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return strings.TrimSuffix(symbol, quote)
		}
	}
	return ""
}

func quoteAsset(symbol string) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	for _, quote := range quoteAssets() {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return quote
		}
	}
	return ""
}

func quoteAssets() []string {
	return []string{"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB"}
}

func positionSideText(positionSide int32) string {
	switch positionSide {
	case 1:
		return "LONG"
	case 2:
		return "SHORT"
	default:
		return "BOTH"
	}
}
