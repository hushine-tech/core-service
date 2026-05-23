package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultTelegramAPIBase = "https://api.telegram.org"

type TelegramClient struct {
	Token   string
	APIBase string
	HTTP    *http.Client
}

func NewTelegramClient(token string, timeout time.Duration) *TelegramClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &TelegramClient{
		Token:   token,
		APIBase: defaultTelegramAPIBase,
		HTTP:    &http.Client{Timeout: timeout},
	}
}

func (c *TelegramClient) SendMessage(ctx context.Context, chatID string, text string) error {
	if c == nil || strings.TrimSpace(c.Token) == "" {
		return fmt.Errorf("telegram token is not configured")
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	apiBase := strings.TrimRight(c.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultTelegramAPIBase
	}
	body, err := json.Marshal(map[string]any{
		"chat_id": chatID,
		"text":    text,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/bot"+c.Token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		OK          bool   `json:"ok"`
		ErrorCode   int    `json:"error_code"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("telegram api error: status=%d code=%d desc=%s", resp.StatusCode, result.ErrorCode, result.Description)
	}
	return nil
}

func (c *TelegramClient) GetUpdates(ctx context.Context, offset int64, timeoutSeconds int) ([]TelegramUpdate, error) {
	if c == nil || strings.TrimSpace(c.Token) == "" {
		return nil, fmt.Errorf("telegram token is not configured")
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: time.Duration(timeoutSeconds+5) * time.Second}
	}
	apiBase := strings.TrimRight(c.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultTelegramAPIBase
	}
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", fmt.Sprintf("%d", offset))
	}
	if timeoutSeconds > 0 {
		q.Set("timeout", fmt.Sprintf("%d", timeoutSeconds))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"/bot"+c.Token+"/getUpdates?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		OK          bool             `json:"ok"`
		Result      []TelegramUpdate `json:"result"`
		ErrorCode   int              `json:"error_code"`
		Description string           `json:"description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("telegram getUpdates error: status=%d code=%d desc=%s", resp.StatusCode, result.ErrorCode, result.Description)
	}
	return result.Result, nil
}

func RunTelegramPolling(ctx context.Context, svc *Service, client *TelegramClient, pollInterval time.Duration) error {
	if svc == nil {
		return fmt.Errorf("notification service is required")
	}
	if client == nil {
		return fmt.Errorf("telegram client is required")
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	var offset int64
	for {
		if ctx.Err() != nil {
			return nil
		}
		updates, err := client.GetUpdates(ctx, offset, int(pollInterval/time.Second))
		if err != nil {
			timer := time.NewTimer(pollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			_ = svc.HandleTelegramUpdate(ctx, update)
		}
	}
}

type TelegramUpdate struct {
	UpdateID int64           `json:"update_id"`
	Message  TelegramMessage `json:"message"`
}

type TelegramMessage struct {
	Text string       `json:"text"`
	Chat TelegramChat `json:"chat"`
}

type TelegramChat struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Title     string `json:"title"`
}
