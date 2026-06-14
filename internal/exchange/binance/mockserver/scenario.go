package mockserver

import (
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

type SceneMode int

const (
	SceneNormalFill SceneMode = iota + 1
	ScenePartialNeverComplete
	ScenePartialThenWSFill
	SceneRestingNoFill
	ScenePostOnlyWouldTake
	SceneNoLiquidity
	SceneExchangeReject
	SceneRateLimit
	SceneTimeout
)

type Config struct {
	Scene3Delay      time.Duration
	Scene9Delay      time.Duration
	PartialFillRatio float64
}

func normalizeConfig(cfg Config) Config {
	if cfg.Scene3Delay <= 0 {
		cfg.Scene3Delay = 120 * time.Second
	}
	if cfg.Scene9Delay <= 0 {
		cfg.Scene9Delay = 15 * time.Second
	}
	if cfg.PartialFillRatio <= 0 || cfg.PartialFillRatio >= 1 {
		cfg.PartialFillRatio = 0.2
	}
	return cfg
}

func sceneName(scene SceneMode) string {
	switch scene {
	case SceneNormalFill:
		return "normal_fill"
	case ScenePartialNeverComplete:
		return "partial_never_complete"
	case ScenePartialThenWSFill:
		return "partial_then_ws_fill"
	case SceneRestingNoFill:
		return "resting_no_fill"
	case ScenePostOnlyWouldTake:
		return "post_only_would_take"
	case SceneNoLiquidity:
		return "no_liquidity"
	case SceneExchangeReject:
		return "exchange_reject"
	case SceneRateLimit:
		return "rate_limit"
	case SceneTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

type EventKind string

const (
	EventAcceptNew            EventKind = "ACCEPT_NEW"
	EventWSPartialFill        EventKind = "WS_PARTIAL_FILL"
	EventWSFinalFill          EventKind = "WS_FINAL_FILL"
	EventRESTTradesIncomplete EventKind = "REST_TRADES_INCOMPLETE"
	EventRESTTradesComplete   EventKind = "REST_TRADES_COMPLETE"
	EventWSDuplicateEvent     EventKind = "WS_DUPLICATE_EVENT"
	EventOrderCanceled        EventKind = "ORDER_CANCELED"
	EventOrderExpired         EventKind = "ORDER_EXPIRED"
)

type BinanceScenario struct {
	Market       domain.Market
	Symbol       string
	Side         string
	PositionSide string
	OrderType    string
	TimeInForce  string
	PostOnly     bool
	ReduceOnly   bool
	OrigQty      float64
	Price        float64
	Status       string
	Events       []BinanceOrderEventStep
}

type BinanceOrderEventStep struct {
	Kind  EventKind
	Fills []BinanceFill
	Delay time.Duration
}

type BinanceFill struct {
	TradeID  string
	Qty      float64
	Price    float64
	Fee      float64
	FeeAsset string
	Time     time.Time
}

type BinanceOrderEvent struct {
	Symbol               string
	ClientOrderID        string
	ExchangeOrderID      string
	ExchangeTradeID      string
	Side                 string
	PositionSide         string
	OrderType            string
	TimeInForce          string
	ExecutionType        string
	OrderStatus          string
	LastFilledQty        float64
	LastFilledPrice      float64
	AccumulatedFilledQty float64
	Fee                  float64
	FeeAsset             string
	ReduceOnly           bool
	EventTime            time.Time
}
