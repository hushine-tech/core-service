package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hushine-tech/core-service/internal/exchange/adapter"
)

var ErrInvalidCredential = errors.New("invalid_binance_credential")

type credentialValidator struct {
	route adapter.Route
}

func (v credentialValidator) ValidateCredential(_ context.Context, raw json.RawMessage) (adapter.ParsedCredential, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return adapter.ParsedCredential{}, fmt.Errorf("%w: decode json: %v", ErrInvalidCredential, err)
	}

	apiKey := firstString(payload, "api_key", "key", "apiKey")
	apiSecret := firstString(payload, "api_secret", "secret", "secret_key", "apiSecret")
	if apiKey == "" {
		return adapter.ParsedCredential{}, fmt.Errorf("%w: missing api_key", ErrInvalidCredential)
	}
	if apiSecret == "" {
		return adapter.ParsedCredential{}, fmt.Errorf("%w: missing api_secret", ErrInvalidCredential)
	}

	return adapter.ParsedCredential{
		Exchange:    v.route.Exchange,
		Environment: v.route.Environment,
		Raw:         raw,
		Metadata: map[string]string{
			"api_key":    apiKey,
			"api_secret": apiSecret,
		},
	}, nil
}

func firstString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key].(string)
		if !ok {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
