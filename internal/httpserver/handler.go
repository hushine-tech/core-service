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

// GET /accounts/{id} — get account info
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

	writeError(w, http.StatusNotFound, "not found")
}

type createAccountRequest struct {
	UserID         int64   `json:"user_id"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Environment    int     `json:"environment"`
	SlippageBps    float64 `json:"slippage_bps"`
	DefaultFeeRate float64 `json:"default_fee_rate"`
}

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
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
	feeRate := req.DefaultFeeRate
	if feeRate == 0 {
		feeRate = 0.0004
	}
	account := domain.Account{
		UserID:         req.UserID,
		Name:           strings.TrimSpace(req.Name),
		Description:    strings.TrimSpace(req.Description),
		Environment:    domain.Environment(req.Environment),
		MarginMode:     "cross",
		PositionMode:   "one_way",
		SlippageBps:    req.SlippageBps,
		DefaultFeeRate: feeRate,
		CreatedAt:      time.Now().UTC(),
	}

	ctx := r.Context()
	newID, err := h.repo.CreateAccount(ctx, account)
	if err != nil {
		if errors.Is(err, repository.ErrConflict) {
			writeError(w, http.StatusConflict, "account already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "create account: "+err.Error())
		return
	}
	account.AccountID = newID

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
