package accountmeta

import (
	"context"
	"errors"
	"fmt"
	"strings"

	accountdomain "github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AccountRepository interface {
	GetAccount(ctx context.Context, accountID, userID int64) (accountdomain.Account, error)
	GetSession(ctx context.Context, sessionID string, userID int64) (accountdomain.StrategySession, error)
}

type Adapter struct {
	repo AccountRepository
}

func NewAdapter(repo AccountRepository) *Adapter {
	return &Adapter{repo: repo}
}

func (a *Adapter) Get(ctx context.Context, accountID int64) (Meta, error) {
	account, err := a.repo.GetAccount(ctx, accountID, 0)
	if err != nil {
		return Meta{}, mapRepositoryError(err)
	}

	return Meta{
		AccountID:      account.AccountID,
		UserID:         account.UserID,
		Mode:           int32(account.Mode),
		MarginMode:     account.MarginMode,
		PositionMode:   account.PositionMode,
		APIKey:         account.APIKey,
		APISecret:      account.APISecret,
		DefaultFeeRate: account.DefaultFeeRate,
		SlippageBps:    account.SlippageBps,
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
