package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
)

// Handler implements the HTTP REST API for account management.
type Handler struct {
	repo repository.Repository
}

func NewHandler(repo repository.Repository) *Handler {
	return &Handler{repo: repo}
}

// RegisterRoutes attaches all routes to the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/accounts", h.handleAccounts)
	mux.HandleFunc("/accounts/", h.handleAccountByID)
}

// POST /accounts — create account
// GET  /accounts — list all accounts
func (h *Handler) handleAccounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createAccount(w, r)
	case http.MethodGet:
		h.listAccounts(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// GET /accounts/{id}       — get account info
// PUT /accounts/{id}/wallet — update wallet state (backtest import)
// GET /accounts/{id}/wallet — get current wallet state
func (h *Handler) handleAccountByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/accounts/")
	parts := strings.SplitN(path, "/", 2)
	rawID := parts[0]
	if rawID == "" {
		writeError(w, http.StatusBadRequest, "account_id required")
		return
	}
	accountID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "account_id must be an integer")
		return
	}

	if len(parts) == 1 {
		// /accounts/{id}
		switch r.Method {
		case http.MethodGet:
			h.getAccount(w, r, accountID)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	sub := parts[1]
	if sub == "wallet" {
		switch r.Method {
		case http.MethodPut:
			h.updateWallet(w, r, accountID)
		case http.MethodGet:
			h.getWallet(w, r, accountID)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	writeError(w, http.StatusNotFound, "not found")
}

type createAccountRequest struct {
	UserID         int64   `json:"user_id"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Environment    int     `json:"environment"`
	APIKey         string  `json:"api_key"`
	APISecret      string  `json:"api_secret"`
	InitialBalance float64 `json:"initial_balance"`
	MarginMode     string  `json:"margin_mode"`
	PositionMode   string  `json:"position_mode"`
	SlippageBps    float64 `json:"slippage_bps"`
	DefaultFeeRate float64 `json:"default_fee_rate"`
}

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	marginMode := req.MarginMode
	if marginMode == "" {
		marginMode = "cross"
	}
	positionMode := req.PositionMode
	if positionMode == "" {
		positionMode = "one_way"
	}
	feeRate := req.DefaultFeeRate
	if feeRate == 0 {
		feeRate = 0.0004
	}
	account := domain.Account{
		UserID:         req.UserID,
		Name:           strings.TrimSpace(req.Name),
		Description:    strings.TrimSpace(req.Description),
		Environment:    domain.Environment(req.Environment),
		APIKey:         req.APIKey,
		APISecret:      req.APISecret,
		MarginMode:     marginMode,
		PositionMode:   positionMode,
		SlippageBps:    req.SlippageBps,
		DefaultFeeRate: feeRate,
		CreatedAt:      time.Now().UTC(),
	}

	ctx := r.Context()
	newID, err := h.repo.CreateAccount(ctx, account)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create account: "+err.Error())
		return
	}
	account.AccountID = newID

	// 回测账号有初始余额：写入 accounts 表当前状态 + initial_seed 快照
	if domain.Environment(req.Environment) == domain.EnvironmentBacktest && req.InitialBalance > 0 {
		totalValue := req.InitialBalance
		spot := domain.SpotWallet{}
		if req.InitialBalance > 0 {
			spot.Free = req.InitialBalance
			totalValue += req.InitialBalance
		}
		info := domain.OnlineAccountInfo{
			AccountID:   newID,
			Environment: account.Environment,
			Futures: domain.FuturesWallet{
				MarginMode:         account.MarginMode,
				PositionMode:       account.PositionMode,
				InitialBalance:     req.InitialBalance,
				WalletBalance:      req.InitialBalance,
				AvailableBalance:   req.InitialBalance,
				TotalMarginBalance: req.InitialBalance,
				MarginBalance:      req.InitialBalance,
			},
			Spot:             spot,
			TotalValue:       totalValue,
			WalletBalance:    req.InitialBalance,
			AvailableBalance: req.InitialBalance,
			UpdatedAt:        time.Now().UTC(),
		}
		if err := h.repo.UpdateAccountState(ctx, info); err != nil {
			writeError(w, http.StatusInternalServerError, "init wallet state: "+err.Error())
			return
		}
		if err := h.repo.SaveSnapshot(ctx, newID, domain.SnapshotReasonInitialSeed, 0, ""); err != nil {
			writeError(w, http.StatusInternalServerError, "init snapshot: "+err.Error())
			return
		}
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"account_id":  newID,
		"user_id":     account.UserID,
		"name":        account.Name,
		"description": account.Description,
		"environment": int(account.Environment),
		"created_at":  account.CreatedAt,
	})
}

func (h *Handler) listAccounts(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	accounts, err := h.repo.ListAccounts(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range accounts {
		accounts[i].APIKey = ""
		accounts[i].APISecret = ""
	}
	writeJSON(w, http.StatusOK, accounts)
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request, accountID int64) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	account, err := h.repo.GetAccount(r.Context(), accountID, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "account not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Mask secrets in response
	account.APIKey = ""
	account.APISecret = ""
	writeJSON(w, http.StatusOK, account)
}

type updateWalletRequest struct {
	Futures          *domain.FuturesWallet `json:"futures"`
	Spot             *domain.SpotWallet    `json:"spot"`
	TotalValue       float64               `json:"total_value"`
	WalletBalance    float64               `json:"wallet_balance"`
	AvailableBalance float64               `json:"available_balance"`
}

func (h *Handler) updateWallet(w http.ResponseWriter, r *http.Request, accountID int64) {
	var req updateWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	ctx := r.Context()
	account, err := h.repo.GetAccount(ctx, accountID, 0)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "account not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	info := domain.OnlineAccountInfo{
		AccountID:        accountID,
		Environment:      account.Environment,
		TotalValue:       req.TotalValue,
		WalletBalance:    req.WalletBalance,
		AvailableBalance: req.AvailableBalance,
		UpdatedAt:        time.Now().UTC(),
	}
	if req.Futures != nil {
		info.Futures = *req.Futures
	}
	if req.Spot != nil {
		info.Spot = *req.Spot
	}

	if err := h.repo.UpdateAccountState(ctx, info); err != nil {
		writeError(w, http.StatusInternalServerError, "update wallet state: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "updated_at": info.UpdatedAt})
}

func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request, accountID int64) {
	info, err := h.repo.GetAccountState(r.Context(), accountID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "no wallet state found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func parseUserID(r *http.Request) (int64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("user_id"))
	if raw == "" {
		return 0, errors.New("user_id is required")
	}
	userID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || userID <= 0 {
		return 0, errors.New("user_id must be a positive integer")
	}
	return userID, nil
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// NewMux creates an http.ServeMux with all routes registered.
// Use this when you need to wrap the mux with external middleware.
func NewMux(repo repository.Repository) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewHandler(repo)
	h.RegisterRoutes(mux)
	return mux
}
