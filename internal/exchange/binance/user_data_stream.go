package binance

import (
	"fmt"

	"github.com/hushine-tech/core-service/internal/domain"
)

type UserDataStreamParser struct {
	Market domain.Market
}

func NewUserDataStreamParser(market domain.Market) UserDataStreamParser {
	return UserDataStreamParser{Market: market}
}

func (p UserDataStreamParser) ParseOrderEvent(payload []byte) (UserDataOrderEvent, error) {
	switch p.Market {
	case domain.MarketSpot:
		return ParseSpotUserDataOrderEvent(payload)
	case domain.MarketPerpetualFutures:
		return ParseFuturesUserDataOrderEvent(payload)
	default:
		return UserDataOrderEvent{}, fmt.Errorf("unsupported binance user data stream market: %s", p.Market.String())
	}
}
