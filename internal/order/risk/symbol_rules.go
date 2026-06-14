package risk

import (
	"fmt"
	"math"
	"strings"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

const riskEpsilon = 1e-12

func validateCapability(req ReviewRequest, capability adapter.OrderCapability) Violation {
	orderType := strings.ToUpper(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		orderType = "MARKET"
	}
	if !containsFold(capability.OrderTypes, orderType) {
		return Violation{Code: "ORDER_TYPE_UNSUPPORTED", Message: fmt.Sprintf("order_type %s is unsupported", orderType)}
	}
	if orderType == "MARKET" {
		if req.Price != nil {
			return Violation{Code: "UNSUPPORTED_ORDER_COMBINATION", Message: "market order must not set price"}
		}
		if req.PostOnly {
			return Violation{Code: "UNSUPPORTED_ORDER_COMBINATION", Message: "market order must not set post_only"}
		}
		if strings.TrimSpace(req.TimeInForce) != "" {
			return Violation{Code: "UNSUPPORTED_ORDER_COMBINATION", Message: "market order must not set time_in_force"}
		}
		if req.GoodTillDate != nil {
			return Violation{Code: "UNSUPPORTED_ORDER_COMBINATION", Message: "market order must not set good_till_date"}
		}
	}
	if req.PostOnly && !capability.SupportsPostOnly {
		return Violation{Code: "POST_ONLY_UNSUPPORTED", Message: "post_only is unsupported"}
	}
	tif := strings.ToUpper(strings.TrimSpace(req.TimeInForce))
	if orderType == "LIMIT" && tif == "" {
		tif = "GTC"
	}
	if tif != "" && !containsFold(capability.TimeInForce, tif) {
		return Violation{Code: "TIME_IN_FORCE_UNSUPPORTED", Message: fmt.Sprintf("time_in_force %s is unsupported", tif)}
	}
	if req.PostOnly && (tif == "IOC" || tif == "FOK" || tif == "GTD") {
		return Violation{Code: "UNSUPPORTED_ORDER_COMBINATION", Message: fmt.Sprintf("post_only cannot be combined with time_in_force=%s", tif)}
	}
	if (tif == "GTD" || req.GoodTillDate != nil) && !capability.SupportsGTD {
		return Violation{Code: "GTD_UNSUPPORTED", Message: "good_till_date is unsupported"}
	}
	if req.ReduceOnly && !capability.SupportsReduceOnly {
		return Violation{Code: "REDUCE_ONLY_UNSUPPORTED", Message: "reduce_only is unsupported"}
	}
	return Violation{}
}

func validateSymbolRules(req ReviewRequest, metadata FuturesRiskMetadata) Violation {
	qty := math.Abs(req.Qty)
	if metadata.MinQty > 0 && qty+riskEpsilon < metadata.MinQty {
		return Violation{Code: "MIN_QTY_VIOLATION", Message: fmt.Sprintf("qty %g is below min_qty %g", qty, metadata.MinQty)}
	}
	if metadata.StepSize > 0 && !alignsToIncrement(qty, metadata.StepSize) {
		return Violation{Code: "STEP_SIZE_VIOLATION", Message: fmt.Sprintf("qty %g is not aligned to step_size %g", qty, metadata.StepSize)}
	}
	if strings.EqualFold(strings.TrimSpace(req.OrderType), "LIMIT") && req.Price != nil && metadata.TickSize > 0 && !alignsToIncrement(*req.Price, metadata.TickSize) {
		return Violation{Code: "TICK_SIZE_VIOLATION", Message: fmt.Sprintf("price %g is not aligned to tick_size %g", *req.Price, metadata.TickSize)}
	}
	if metadata.MinNotional > 0 {
		notional := orderNotional(req)
		if notional+riskEpsilon < metadata.MinNotional {
			return Violation{Code: "MIN_NOTIONAL_VIOLATION", Message: fmt.Sprintf("notional %g is below min_notional %g", notional, metadata.MinNotional)}
		}
	}
	return Violation{}
}

func alignsToIncrement(value, increment float64) bool {
	if increment <= 0 {
		return true
	}
	scaled := value / increment
	nearest := math.Round(scaled)
	return math.Abs(scaled-nearest) <= 1e-9 || math.Abs(value-nearest*increment) <= math.Max(riskEpsilon, increment*1e-9)
}

func containsFold(values []string, needle string) bool {
	needle = strings.ToUpper(strings.TrimSpace(needle))
	for _, value := range values {
		if strings.ToUpper(strings.TrimSpace(value)) == needle {
			return true
		}
	}
	return false
}
