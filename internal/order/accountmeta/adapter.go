package accountmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hushine-tech/core-service/internal/credential"
	accountdomain "github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AccountRepository interface {
	ResolveVenueRouteMeta(ctx context.Context, accountID int64, exchange accountdomain.Exchange, market accountdomain.Market) (accountdomain.VenueRouteMeta, error)
	GetSession(ctx context.Context, sessionID string, userID int64) (accountdomain.StrategySession, error)
}

type Adapter struct {
	repo  AccountRepository
	creds *credential.Manager
}

func NewAdapter(repo AccountRepository, creds *credential.Manager) *Adapter {
	return &Adapter{repo: repo, creds: creds}
}

func (a *Adapter) Get(ctx context.Context, accountID int64, exchange int32, market int32) (Meta, error) {
	meta, err := a.repo.ResolveVenueRouteMeta(ctx, accountID, accountdomain.Exchange(exchange), accountdomain.Market(market))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return Meta{}, status.Error(codes.FailedPrecondition, "no active venue route for account/exchange/market")
		}
		return Meta{}, mapRepositoryError(err)
	}

	credentialJSON := ""
	apiSecret := ""
	if meta.Environment != accountdomain.EnvironmentBacktest {
		if a.creds == nil {
			return Meta{}, status.Error(codes.FailedPrecondition, "credential manager is not configured")
		}
		if strings.TrimSpace(meta.CredentialInfo) == "" {
			return Meta{}, status.Error(codes.FailedPrecondition, "venue credential is missing")
		}
		credentialJSON, err = a.creds.Decrypt(meta.CredentialInfo)
		if err != nil {
			return Meta{}, status.Errorf(codes.FailedPrecondition, "decrypt venue credential: %v", err)
		}
		apiSecret, err = apiSecretFromCredentialJSON(credentialJSON)
		if err != nil {
			return Meta{}, err
		}
	}

	return Meta{
		AccountID:      meta.AccountID,
		VenueID:        meta.VenueID,
		UserID:         meta.UserID,
		Environment:    int32(meta.Environment),
		Exchange:       int32(meta.Exchange),
		Market:         int32(meta.Market),
		MarginMode:     marginModeText(meta.MarginMode),
		PositionMode:   positionModeText(meta.PositionMode),
		APIKey:         meta.APIKey,
		APISecret:      apiSecret,
		CredentialJSON: credentialJSON,
		DefaultFeeRate: meta.DefaultFeeRate,
		SlippageBps:    meta.SlippageBps,
	}, nil
}

func (a *Adapter) ValidateActiveSession(ctx context.Context, meta Meta, strategyID int64, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}

	session, err := a.repo.GetSession(ctx, sessionID, meta.UserID)
	if err != nil {
		return mapRepositoryError(err)
	}
	if session.AccountID != meta.AccountID {
		return status.Errorf(codes.PermissionDenied, "session %s does not belong to account %d", sessionID, meta.AccountID)
	}
	if strategyID != 0 && session.StrategyID != strategyID {
		return status.Errorf(codes.PermissionDenied, "session %s does not belong to strategy %d", sessionID, strategyID)
	}

	sessionStatus := strings.TrimSpace(session.Status)
	switch strings.ToLower(sessionStatus) {
	case "running", "stopping":
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition, "session %s status %s is not active", sessionID, sessionStatus)
	}
}

func apiSecretFromCredentialJSON(raw string) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "invalid venue credential json: %v", err)
	}
	for _, key := range []string{"api_secret", "secret", "secret_key"} {
		if v, ok := payload[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v), nil
		}
	}
	return "", status.Error(codes.FailedPrecondition, "venue credential missing api_secret")
}

func marginModeText(mode accountdomain.MarginMode) string {
	switch mode {
	case accountdomain.MarginModeCross:
		return "cross"
	case accountdomain.MarginModeIsolated:
		return "isolated"
	default:
		return ""
	}
}

func positionModeText(mode accountdomain.PositionMode) string {
	switch mode {
	case accountdomain.PositionModeOneWay:
		return "one_way"
	case accountdomain.PositionModeHedge:
		return "hedge"
	default:
		return ""
	}
}

func mapRepositoryError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, repository.ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("repository error: %v", err))
	}
}
