package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

type binanceUserDataStreamClient struct {
	route      adapter.Route
	endpoints  Endpoints
	httpClient *http.Client
	dialer     *websocket.Dialer
}

type listenKeyResponse struct {
	ListenKey string `json:"listenKey"`
}

func (c binanceUserDataStreamClient) Listen(ctx context.Context, req adapter.UserDataStreamRequest, handle func(context.Context, adapter.UserDataOrderEvent) error) error {
	listenKey, err := c.createListenKey(ctx, req.Credential)
	if err != nil {
		return err
	}
	wsURL := strings.TrimRight(c.endpoints.WSBaseURL, "/") + "/ws/" + listenKey
	dialer := c.dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("connect binance user data stream: %w", err)
	}
	defer conn.Close()

	parser := NewUserDataStreamParser(c.route.Market)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read binance user data stream: %w", err)
		}
		raw, err := parser.ParseOrderEvent(payload)
		if err != nil {
			return err
		}
		if err := handle(ctx, toAdapterUserDataOrderEvent(raw)); err != nil {
			return err
		}
	}
}

func (c binanceUserDataStreamClient) createListenKey(ctx context.Context, credential adapter.ParsedCredential) (string, error) {
	apiKey := strings.TrimSpace(credential.Metadata["api_key"])
	if apiKey == "" {
		return "", fmt.Errorf("%w: missing api_key", ErrInvalidCredential)
	}
	client := c.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	path := "/fapi/v1/listenKey"
	if c.route.Market == domain.MarketSpot {
		path = "/api/v3/userDataStream"
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.endpoints.RESTBaseURL, "/")+path, bytes.NewReader(nil))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("X-MBX-APIKEY", apiKey)
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("binance listenKey HTTP %d", resp.StatusCode)
	}
	var decoded listenKeyResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode binance listenKey: %w", err)
	}
	if strings.TrimSpace(decoded.ListenKey) == "" {
		return "", fmt.Errorf("binance listenKey response missing listenKey")
	}
	return decoded.ListenKey, nil
}

func toAdapterUserDataOrderEvent(event UserDataOrderEvent) adapter.UserDataOrderEvent {
	return adapter.UserDataOrderEvent{
		EventSource:          event.EventSource,
		EventTime:            event.EventTime,
		Symbol:               event.Symbol,
		ClientOrderID:        event.ClientOrderID,
		ExchangeOrderID:      event.ExchangeOrderID,
		ExchangeTradeID:      event.ExchangeTradeID,
		Side:                 event.Side,
		PositionSide:         event.PositionSide,
		OrderType:            event.OrderType,
		TimeInForce:          event.TimeInForce,
		OrderStatus:          event.OrderStatus,
		ExecutionType:        event.ExecutionType,
		LastFilledQty:        event.LastFilledQty,
		LastFilledPrice:      event.LastFilledPrice,
		AccumulatedFilledQty: event.AccumulatedFilledQty,
		Fee:                  event.Fee,
		FeeAsset:             event.FeeAsset,
		ReduceOnly:           event.ReduceOnly,
	}
}
