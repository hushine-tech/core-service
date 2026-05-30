package service

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/catalog"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/domain"
	"github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/notification"
	"github.com/hushine-tech/core-service/internal/reconciliation"
	"github.com/hushine-tech/core-service/internal/repository"
	"github.com/hushine-tech/core-service/internal/walletmetrics"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	reMyStrategyClass = regexp.MustCompile(`class\s+MyStrategy`)
	reOnMarketData    = regexp.MustCompile(`def\s+on_market_data\s*\(\s*self(?:\s*:\s*[^,\)\n]+)?\s*,\s*data(?:\s*:\s*[^,\)\n]+)?\s*,\s*wallet(?:\s*:\s*[^,\)\n]+)?\s*,?\s*\)\s*(?:->\s*[^:\n]+)?\s*:`)
	reValidVersion    = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
	reValidUsername   = regexp.MustCompile(`^[a-z0-9_-]{3,32}$`)
)

const (
	defaultRuntimeVersion = "1.0.0"
	defaultRuntimeProfile = "platform-python-3.13"
	sessionTypeBacktest   = "backtest"
	sessionTypeDebugging  = "debugging"
	sessionTypeTestnet    = "testnet"
)

type AccountGRPCService struct {
	accountv1.UnimplementedAccountServiceServer
	repo       repository.Repository
	router     *exchange.AdapterRouter
	symbols    *catalog.Catalog
	reconciler *reconciliation.Service // Phase C; may be nil or disabled
	notifier   *notification.Service
	creds      *credential.Manager
	registry   *adapter.Registry
}

type AccountServiceOption func(*AccountGRPCService)

func WithCredentialManager(manager *credential.Manager) AccountServiceOption {
	return func(s *AccountGRPCService) {
		s.creds = manager
	}
}

func WithExchangeRegistry(registry *adapter.Registry) AccountServiceOption {
	return func(s *AccountGRPCService) {
		s.registry = registry
	}
}

func NewAccountGRPCService(
	repo repository.Repository,
	router *exchange.AdapterRouter,
	symbols *catalog.Catalog,
	reconciler *reconciliation.Service,
	opts ...any,
) *AccountGRPCService {
	svc := &AccountGRPCService{
		repo:       repo,
		router:     router,
		symbols:    symbols,
		reconciler: reconciler,
	}
	for _, opt := range opts {
		switch v := opt.(type) {
		case *notification.Service:
			svc.notifier = v
		case AccountServiceOption:
			v(svc)
		}
	}
	return svc
}

func requireUserID(userID int64) error {
	if userID <= 0 {
		return status.Error(codes.InvalidArgument, "user_id is required")
	}
	return nil
}

func validateEnvironment(raw int32) (domain.Environment, error) {
	env := domain.Environment(raw)
	switch env {
	case domain.EnvironmentBacktest, domain.EnvironmentDemo, domain.EnvironmentLive:
		return env, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "environment is invalid")
	}
}

func validateExchange(raw int32) (domain.Exchange, error) {
	exchange := domain.Exchange(raw)
	switch exchange {
	case domain.ExchangeBinance, domain.ExchangeOKX:
		return exchange, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "exchange is invalid")
	}
}

func validateMarket(raw int32) (domain.Market, error) {
	market := domain.Market(raw)
	switch market {
	case domain.MarketSpot, domain.MarketPerpetualFutures, domain.MarketDeliveryFutures:
		return market, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "market is invalid")
	}
}

func normalizeVenueModes(market domain.Market, rawMargin, rawPosition int32) (domain.MarginMode, domain.PositionMode, error) {
	margin := domain.MarginMode(rawMargin)
	position := domain.PositionMode(rawPosition)
	switch market {
	case domain.MarketSpot:
		if rawMargin == 0 {
			margin = domain.MarginModeNone
		}
		if rawPosition == 0 {
			position = domain.PositionModeNone
		}
	case domain.MarketPerpetualFutures:
		if rawMargin == 0 {
			margin = domain.MarginModeCross
		}
		if rawPosition == 0 {
			position = domain.PositionModeOneWay
		}
	}
	venue := domain.Venue{Market: market, MarginMode: margin, PositionMode: position}
	if err := venue.ValidateMarketModes(); err != nil {
		return 0, 0, status.Error(codes.InvalidArgument, err.Error())
	}
	return margin, position, nil
}

func accountModeFromEnvironment(env domain.Environment) domain.AccountMode {
	switch env {
	case domain.EnvironmentDemo:
		return domain.AccountModeBinanceTestnet
	case domain.EnvironmentLive:
		return domain.AccountModeBinanceLive
	default:
		return domain.AccountModeBacktest
	}
}

func marginModeText(mode domain.MarginMode) string {
	switch mode {
	case domain.MarginModeCross:
		return "cross"
	case domain.MarginModeIsolated:
		return "isolated"
	default:
		return ""
	}
}

func positionModeText(mode domain.PositionMode) string {
	switch mode {
	case domain.PositionModeOneWay:
		return "one_way"
	case domain.PositionModeHedge:
		return "hedge"
	default:
		return ""
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

func (s *AccountGRPCService) accountForExchangeFetch(ctx context.Context, account domain.Account) (domain.Account, error) {
	if account.Mode == domain.AccountModeBacktest {
		return account, nil
	}
	meta, err := s.repo.ResolveVenueRouteMeta(ctx, account.AccountID, domain.ExchangeBinance, domain.MarketPerpetualFutures)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return domain.Account{}, status.Error(codes.FailedPrecondition, "no active binance perpetual_futures venue for account")
		}
		return domain.Account{}, mapRepoErr(err)
	}
	if s.creds == nil {
		return domain.Account{}, status.Error(codes.FailedPrecondition, "credential manager is not configured")
	}
	if strings.TrimSpace(meta.CredentialInfo) == "" {
		return domain.Account{}, status.Error(codes.FailedPrecondition, "venue credential is missing")
	}
	credentialJSON, err := s.creds.Decrypt(meta.CredentialInfo)
	if err != nil {
		return domain.Account{}, status.Errorf(codes.FailedPrecondition, "decrypt venue credential: %v", err)
	}
	apiSecret, err := apiSecretFromCredentialJSON(credentialJSON)
	if err != nil {
		return domain.Account{}, err
	}
	account.UserID = meta.UserID
	account.Environment = meta.Environment
	account.Mode = accountModeFromEnvironment(meta.Environment)
	account.APIKey = meta.APIKey
	account.APISecret = apiSecret
	account.MarginMode = marginModeText(meta.MarginMode)
	account.PositionMode = positionModeText(meta.PositionMode)
	account.DefaultFeeRate = meta.DefaultFeeRate
	account.SlippageBps = meta.SlippageBps
	return account, nil
}

func (s *AccountGRPCService) ensureVenueCanAttachAccount(ctx context.Context, userID int64, accountID int64, env domain.Environment) error {
	if accountID == 0 {
		return nil
	}
	account, err := s.repo.GetAccount(ctx, accountID, userID)
	if err != nil {
		return mapRepoErr(err)
	}
	if account.Status != 0 && account.Status != domain.AccountStatusActive {
		return status.Error(codes.FailedPrecondition, "account is not active")
	}
	if environmentFromAccount(account) != env {
		return status.Error(codes.FailedPrecondition, "venue environment does not match account environment")
	}
	if n, err := s.repo.CountActiveSessionsForAccount(ctx, userID, accountID); err != nil {
		return mapRepoErr(err)
	} else if n > 0 {
		return status.Error(codes.FailedPrecondition, "account has active sessions")
	}
	return nil
}

func environmentFromAccount(account domain.Account) domain.Environment {
	if account.Environment == domain.EnvironmentDemo || account.Environment == domain.EnvironmentLive {
		return account.Environment
	}
	switch account.Mode {
	case domain.AccountModeBinanceLive:
		return domain.EnvironmentLive
	case domain.AccountModeBinanceTestnet:
		return domain.EnvironmentDemo
	default:
		return domain.EnvironmentBacktest
	}
}

func isActiveSessionStatus(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "running", "stopping":
		return true
	default:
		return false
	}
}

func (s *AccountGRPCService) requireActiveSessionForAccount(
	ctx context.Context,
	sessionID string,
	accountID int64,
	strategyID int64,
	userID int64,
) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	sess, err := s.repo.GetSession(ctx, sessionID, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return status.Error(codes.NotFound, "session not found")
		}
		return status.Errorf(codes.Unavailable, "get session: %v", err)
	}
	if sess.AccountID != accountID {
		return status.Error(codes.PermissionDenied, "session account_id does not match wallet update account_id")
	}
	if strategyID != 0 && sess.StrategyID != strategyID {
		return status.Error(codes.PermissionDenied, "session strategy_id does not match wallet update strategy_id")
	}
	if !isActiveSessionStatus(sess.Status) {
		return status.Errorf(codes.FailedPrecondition, "session %s is not active: %s", sessionID, sess.Status)
	}
	return nil
}

func normalizeUsername(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

func validateUsername(username string) error {
	if !reValidUsername.MatchString(username) {
		return status.Error(codes.InvalidArgument, "username must match [a-z0-9_-]{3,32}")
	}
	return nil
}

func toProtoUser(user domain.User) *accountv1.User {
	return &accountv1.User{
		Id:        user.ID,
		Username:  user.Username,
		CreatedAt: timestamppb.New(user.CreatedAt),
		PlanCode:  user.PlanCode,
	}
}

func isDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique")
}

func (s *AccountGRPCService) CreateUser(ctx context.Context, req *accountv1.CreateUserRequest) (*accountv1.CreateUserResponse, error) {
	username := normalizeUsername(req.GetUsername())
	if err := validateUsername(username); err != nil {
		return nil, err
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password is required")
	}

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.GetPassword()), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hash password: %v", err)
	}

	user, err := s.repo.CreateUser(ctx, domain.User{
		Username:     username,
		PasswordHash: string(passwordHash),
		CreatedAt:    time.Now().UTC(),
	})
	if err != nil {
		if isDuplicateErr(err) {
			return nil, status.Errorf(codes.AlreadyExists, "username %q already exists", username)
		}
		return nil, status.Errorf(codes.Internal, "create user: %v", err)
	}
	return &accountv1.CreateUserResponse{User: toProtoUser(user)}, nil
}

func (s *AccountGRPCService) VerifyUserPassword(ctx context.Context, req *accountv1.VerifyUserPasswordRequest) (*accountv1.VerifyUserPasswordResponse, error) {
	username := normalizeUsername(req.GetUsername())
	if err := validateUsername(username); err != nil {
		return nil, err
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password is required")
	}

	user, err := s.repo.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return &accountv1.VerifyUserPasswordResponse{Valid: false}, nil
		}
		return nil, status.Errorf(codes.Unavailable, "lookup user: %v", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.GetPassword())); err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return &accountv1.VerifyUserPasswordResponse{Valid: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "verify password: %v", err)
	}

	return &accountv1.VerifyUserPasswordResponse{
		User:  toProtoUser(user),
		Valid: true,
	}, nil
}

// GetUser fetches a user by id. Used by control-panel-service to read
// users.plan_code during runtime quota / route resolution.
func (s *AccountGRPCService) GetUser(ctx context.Context, req *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	user, err := s.repo.GetUser(ctx, req.GetUserId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Unavailable, "get user: %v", err)
	}
	return &accountv1.GetUserResponse{User: toProtoUser(user)}, nil
}

func (s *AccountGRPCService) requireNotifier() (*notification.Service, error) {
	if s.notifier == nil {
		return nil, status.Error(codes.FailedPrecondition, "notification service is not configured")
	}
	return s.notifier, nil
}

func (s *AccountGRPCService) GetNotificationSettings(ctx context.Context, req *accountv1.GetNotificationSettingsRequest) (*accountv1.GetNotificationSettingsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	settings, plan, channel, err := notifier.GetSettings(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return toProtoNotificationSettings(notifier, settings, plan, channel), nil
}

func (s *AccountGRPCService) UpdateNotificationPreferences(ctx context.Context, req *accountv1.UpdateNotificationPreferencesRequest) (*accountv1.UpdateNotificationPreferencesResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetPreferences() == nil {
		return nil, status.Error(codes.InvalidArgument, "preferences are required")
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	if _, err := notifier.UpdatePreferences(ctx, domain.NotificationSettings{
		UserID:          req.GetUserId(),
		SystemEnabled:   req.GetPreferences().GetSystemEnabled(),
		StrategyEnabled: req.GetPreferences().GetStrategyEnabled(),
		CustomEnabled:   req.GetPreferences().GetCustomEnabled(),
	}); err != nil {
		return nil, mapRepoErr(err)
	}
	settings, plan, channel, err := notifier.GetSettings(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.UpdateNotificationPreferencesResponse{Settings: toProtoNotificationSettings(notifier, settings, plan, channel)}, nil
}

func (s *AccountGRPCService) CreateNotificationBindCode(ctx context.Context, req *accountv1.CreateNotificationBindCodeRequest) (*accountv1.CreateNotificationBindCodeResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if ch := strings.TrimSpace(req.GetChannel()); ch != "" && ch != domain.NotificationChannelTelegram {
		return nil, status.Error(codes.InvalidArgument, "only telegram notification channel is supported")
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	code, expiresAt, err := notifier.CreateBindCode(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.CreateNotificationBindCodeResponse{
		BindCode:    code,
		ExpiresAt:   timestamppb.New(expiresAt),
		BotUsername: notifier.BotUsername(),
	}, nil
}

func (s *AccountGRPCService) ConfirmNotificationBinding(ctx context.Context, req *accountv1.ConfirmNotificationBindingRequest) (*accountv1.ConfirmNotificationBindingResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if ch := strings.TrimSpace(req.GetChannel()); ch != "" && ch != domain.NotificationChannelTelegram {
		return nil, status.Error(codes.InvalidArgument, "only telegram notification channel is supported")
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	if _, err := notifier.ConfirmBinding(ctx, req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	settings, plan, channel, err := notifier.GetSettings(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.ConfirmNotificationBindingResponse{Settings: toProtoNotificationSettings(notifier, settings, plan, channel)}, nil
}

func (s *AccountGRPCService) UnbindNotificationChannel(ctx context.Context, req *accountv1.UnbindNotificationChannelRequest) (*accountv1.UnbindNotificationChannelResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if ch := strings.TrimSpace(req.GetChannel()); ch != "" && ch != domain.NotificationChannelTelegram {
		return nil, status.Error(codes.InvalidArgument, "only telegram notification channel is supported")
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	if err := notifier.Unbind(ctx, req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	settings, plan, channel, err := notifier.GetSettings(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.UnbindNotificationChannelResponse{Settings: toProtoNotificationSettings(notifier, settings, plan, channel)}, nil
}

func (s *AccountGRPCService) SendTestNotification(ctx context.Context, req *accountv1.SendTestNotificationRequest) (*accountv1.SendTestNotificationResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	notifier, err := s.requireNotifier()
	if err != nil {
		return nil, err
	}
	if err := notifier.SendTest(ctx, req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	settings, plan, channel, err := notifier.GetSettings(ctx, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.SendTestNotificationResponse{
		Accepted: true,
		Settings: toProtoNotificationSettings(notifier, settings, plan, channel),
	}, nil
}

func toProtoNotificationSettings(notifier *notification.Service, settings domain.NotificationSettings, plan domain.NotificationPlan, channel domain.NotificationChannel) *accountv1.GetNotificationSettingsResponse {
	return &accountv1.GetNotificationSettingsResponse{
		Preferences: &accountv1.NotificationPreferences{
			SystemEnabled:   settings.SystemEnabled,
			StrategyEnabled: settings.StrategyEnabled,
			CustomEnabled:   settings.CustomEnabled,
		},
		Plan:        toProtoNotificationPlan(plan),
		Telegram:    toProtoNotificationChannel(channel),
		BotUsername: notifier.BotUsername(),
	}
}

func toProtoNotificationPlan(plan domain.NotificationPlan) *accountv1.NotificationPlan {
	return &accountv1.NotificationPlan{
		PlanCode:                 plan.PlanCode,
		NotificationEnabled:      plan.NotificationEnabled,
		AllowSystem:              plan.AllowSystem,
		AllowStrategy:            plan.AllowStrategy,
		AllowCustom:              plan.AllowCustom,
		CustomRateLimitPerMinute: int32(plan.CustomRateLimitPerMinute),
		CustomRateLimitBurst:     int32(plan.CustomRateLimitBurst),
	}
}

func toProtoNotificationChannel(channel domain.NotificationChannel) *accountv1.NotificationChannel {
	out := &accountv1.NotificationChannel{
		Channel:             channel.Channel,
		Status:              channel.Status,
		ProviderUsername:    channel.TargetLabel,
		ProviderDisplayName: channel.TargetLabel,
		LastDeliveryStatus:  channel.LastDeliveryStatus,
		LastDeliveryError:   channel.LastDeliveryError,
	}
	if channel.BoundAt != nil {
		out.BoundAt = timestamppb.New(*channel.BoundAt)
	}
	if channel.LastDeliveryAt != nil {
		out.LastDeliveryAt = timestamppb.New(*channel.LastDeliveryAt)
	}
	return out
}

// ListSymbols returns cached Binance symbol lists for portal pickers.
func (s *AccountGRPCService) ListSymbols(ctx context.Context, req *accountv1.ListSymbolsRequest) (*accountv1.ListSymbolsResponse, error) {
	if s.symbols == nil {
		return nil, status.Error(codes.Unavailable, "symbol catalog not configured")
	}
	market, err := catalog.ParseMarket(req.GetMarket())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	syms, stale, err := s.symbols.List(ctx, market, req.GetQuery(), int(req.GetLimit()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "symbol list: %v", err)
	}
	return &accountv1.ListSymbolsResponse{Symbols: syms, Stale: stale}, nil
}

// CreateAccount persists a new account and returns the auto-assigned BIGINT ID.
func (s *AccountGRPCService) CreateAccount(ctx context.Context, req *accountv1.CreateAccountRequest) (*accountv1.CreateAccountResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	env, err := validateEnvironment(req.GetEnvironment())
	if err != nil {
		return nil, err
	}
	feeRate := req.GetDefaultFeeRate()
	if feeRate == 0 {
		feeRate = 0.0004
	}
	account := domain.Account{
		UserID:         req.GetUserId(),
		Name:           name,
		Description:    strings.TrimSpace(req.GetDescription()),
		Environment:    env,
		Status:         domain.AccountStatusActive,
		Mode:           accountModeFromEnvironment(env),
		MarginMode:     "cross",
		PositionMode:   "one_way",
		SlippageBps:    req.GetSlippageBps(),
		DefaultFeeRate: feeRate,
		CreatedAt:      time.Now().UTC(),
	}

	newID, err := s.repo.CreateAccount(ctx, account)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create account: %v", err)
	}
	account.AccountID = newID

	if env == domain.EnvironmentBacktest {
		accountID := newID
		if _, err := s.repo.CreateVenue(ctx, domain.Venue{
			UserID:       req.GetUserId(),
			AccountID:    &accountID,
			Exchange:     domain.ExchangeBinance,
			Market:       domain.MarketPerpetualFutures,
			Environment:  domain.EnvironmentBacktest,
			Status:       domain.VenueStatusActive,
			DisplayName:  "Simulated Binance Perpetual Futures",
			Description:  "default simulated venue",
			MarginMode:   domain.MarginModeCross,
			PositionMode: domain.PositionModeOneWay,
			CreatedAt:    time.Now().UTC(),
			UpdatedAt:    time.Now().UTC(),
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "create default venue: %v", err)
		}
	}

	// 回测账号有初始余额：写入 accounts 表当前状态 + initial_seed 快照
	if env == domain.EnvironmentBacktest && req.GetInitialBalance() > 0 {
		totalValue := req.GetInitialBalance()
		spot := domain.SpotWallet{}
		if req.GetInitialBalance() > 0 {
			spot.Free = req.GetInitialBalance()
			totalValue += req.GetInitialBalance()
		}
		info := domain.OnlineAccountInfo{
			AccountID: newID,
			Mode:      account.Mode,
			Futures: domain.FuturesWallet{
				MarginMode:         account.MarginMode,
				PositionMode:       account.PositionMode,
				InitialBalance:     req.GetInitialBalance(),
				WalletBalance:      req.GetInitialBalance(),
				AvailableBalance:   req.GetInitialBalance(),
				TotalMarginBalance: req.GetInitialBalance(),
				MarginBalance:      req.GetInitialBalance(),
			},
			Spot:             spot,
			TotalValue:       totalValue,
			WalletBalance:    req.GetInitialBalance(),
			AvailableBalance: req.GetInitialBalance(),
			UpdatedAt:        time.Now().UTC(),
		}
		if err := s.repo.UpdateAccountState(ctx, info); err != nil {
			return nil, status.Errorf(codes.Internal, "init wallet state: %v", err)
		}
		if err := s.repo.SaveSnapshot(ctx, newID, domain.SnapshotReasonInitialSeed, 0, ""); err != nil {
			return nil, status.Errorf(codes.Internal, "init snapshot: %v", err)
		}
	}

	return &accountv1.CreateAccountResponse{
		AccountId:   newID,
		Name:        account.Name,
		Environment: int32(account.Environment),
		CreatedAt:   timestamppb.New(account.CreatedAt),
		Description: account.Description,
		Status:      int32(account.Status),
	}, nil
}

// ListAccounts returns all accounts without credentials.
func (s *AccountGRPCService) ListAccounts(ctx context.Context, req *accountv1.ListAccountsRequest) (*accountv1.ListAccountsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetLimit() > 0 || req.GetOffset() > 0 {
		accounts, meta, err := s.repo.ListAccountsPage(ctx, req.GetUserId(), int(req.GetLimit()), int(req.GetOffset()))
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "list accounts: %v", err)
		}
		out := make([]*accountv1.AccountRegistryEntry, 0, len(accounts))
		for _, a := range accounts {
			out = append(out, toProtoRegistryEntry(a))
		}
		return &accountv1.ListAccountsResponse{Accounts: out, HasMore: meta.HasMore, Total: meta.Total}, nil
	}
	accounts, err := s.repo.ListAccounts(ctx, req.GetUserId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list accounts: %v", err)
	}
	out := make([]*accountv1.AccountRegistryEntry, 0, len(accounts))
	for _, a := range accounts {
		out = append(out, toProtoRegistryEntry(a))
	}
	return &accountv1.ListAccountsResponse{Accounts: out, Total: int64(len(out))}, nil
}

// GetAccount returns one account without credentials.
func (s *AccountGRPCService) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest) (*accountv1.GetAccountResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	account, err := s.repo.GetAccount(ctx, accountID, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.GetAccountResponse{Account: toProtoRegistryEntry(account)}, nil
}

func (s *AccountGRPCService) CreateVenue(ctx context.Context, req *accountv1.CreateVenueRequest) (*accountv1.CreateVenueResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	env, err := validateEnvironment(req.GetEnvironment())
	if err != nil {
		return nil, err
	}
	exchangeValue, err := validateExchange(req.GetExchange())
	if err != nil {
		return nil, err
	}
	market, err := validateMarket(req.GetMarket())
	if err != nil {
		return nil, err
	}
	marginMode, positionMode, err := normalizeVenueModes(market, req.GetMarginMode(), req.GetPositionMode())
	if err != nil {
		return nil, err
	}
	statusValue := domain.VenueStatus(req.GetStatus())
	if statusValue == 0 {
		statusValue = domain.VenueStatusActive
	}
	if statusValue != domain.VenueStatusActive && statusValue != domain.VenueStatusDisabled && statusValue != domain.VenueStatusRevoked {
		return nil, status.Error(codes.InvalidArgument, "venue status is invalid")
	}
	if err := s.ensureVenueCanAttachAccount(ctx, req.GetUserId(), req.GetAccountId(), env); err != nil {
		return nil, err
	}

	credentialJSON := strings.TrimSpace(req.GetCredentialJson())
	encryptedCredential := ""
	credentialKeyVersion := ""
	apiKey := strings.TrimSpace(req.GetApiKey())
	if env != domain.EnvironmentBacktest {
		if s.creds == nil {
			return nil, status.Error(codes.FailedPrecondition, "credential manager is not configured")
		}
		if apiKey == "" {
			return nil, status.Error(codes.InvalidArgument, "api_key is required")
		}
		if credentialJSON == "" {
			return nil, status.Error(codes.InvalidArgument, "credential_json is required")
		}
		encryptedCredential, err = s.creds.Encrypt(credentialJSON)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "encrypt credential: %v", err)
		}
		credentialKeyVersion = s.creds.KeyVersion()
	}
	var accountID *int64
	if req.GetAccountId() > 0 {
		id := req.GetAccountId()
		accountID = &id
	}
	now := time.Now().UTC()
	venue, err := s.repo.CreateVenue(ctx, domain.Venue{
		UserID:                req.GetUserId(),
		AccountID:             accountID,
		Exchange:              exchangeValue,
		Market:                market,
		Environment:           env,
		Status:                statusValue,
		DisplayName:           strings.TrimSpace(req.GetDisplayName()),
		Description:           strings.TrimSpace(req.GetDescription()),
		APIKey:                apiKey,
		CredentialInfo:        encryptedCredential,
		CredentialKeyVersion:  credentialKeyVersion,
		CredentialFingerprint: credential.Fingerprint(apiKey),
		MarginMode:            marginMode,
		PositionMode:          positionMode,
		CreatedAt:             now,
		UpdatedAt:             now,
	})
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.CreateVenueResponse{Venue: toProtoVenue(venue)}, nil
}

func (s *AccountGRPCService) ListVenues(ctx context.Context, req *accountv1.ListVenuesRequest) (*accountv1.ListVenuesResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	venues, meta, err := s.repo.ListVenues(ctx, req.GetUserId(), req.GetAccountId(), req.GetIncludeUnbound(), req.GetIncludeInactive(), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, mapRepoErr(err)
	}
	out := make([]*accountv1.VenueEntry, 0, len(venues))
	for _, venue := range venues {
		out = append(out, toProtoVenue(venue))
	}
	return &accountv1.ListVenuesResponse{Venues: out, HasMore: meta.HasMore, Total: meta.Total}, nil
}

func (s *AccountGRPCService) GetVenue(ctx context.Context, req *accountv1.GetVenueRequest) (*accountv1.GetVenueResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetVenueId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "venue_id is required")
	}
	venue, err := s.repo.GetVenue(ctx, req.GetVenueId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.GetVenueResponse{Venue: toProtoVenue(venue)}, nil
}

func (s *AccountGRPCService) GetVenueOnlineInfo(ctx context.Context, req *accountv1.GetVenueOnlineInfoRequest) (*accountv1.GetVenueOnlineInfoResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetVenueId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "venue_id is required")
	}
	venue, err := s.repo.GetVenue(ctx, req.GetVenueId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if venue.Status != domain.VenueStatusActive {
		return nil, status.Error(codes.FailedPrecondition, "venue is not active")
	}
	if venue.Environment == domain.EnvironmentBacktest {
		return nil, status.Error(codes.FailedPrecondition, "backtest venue has no exchange wallet")
	}
	if venue.Exchange != domain.ExchangeBinance || venue.Market != domain.MarketPerpetualFutures {
		return nil, status.Errorf(codes.FailedPrecondition, "venue wallet fetch unsupported for exchange=%d market=%d", venue.Exchange, venue.Market)
	}
	if s.creds == nil {
		return nil, status.Error(codes.FailedPrecondition, "credential manager is not configured")
	}
	if strings.TrimSpace(venue.CredentialInfo) == "" {
		return nil, status.Error(codes.FailedPrecondition, "venue credential is missing")
	}
	credentialJSON, err := s.creds.Decrypt(venue.CredentialInfo)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "decrypt venue credential: %v", err)
	}
	apiSecret, err := apiSecretFromCredentialJSON(credentialJSON)
	if err != nil {
		return nil, err
	}
	accountID := int64(0)
	if venue.AccountID != nil {
		accountID = *venue.AccountID
	}
	info, err := s.router.GetOnlineInfo(ctx, domain.Account{
		AccountID:      accountID,
		UserID:         venue.UserID,
		Environment:    venue.Environment,
		Mode:           accountModeFromEnvironment(venue.Environment),
		APIKey:         venue.APIKey,
		APISecret:      apiSecret,
		MarginMode:     marginModeText(venue.MarginMode),
		PositionMode:   positionModeText(venue.PositionMode),
		DefaultFeeRate: 0.0004,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "get venue online info: %v", err)
	}
	return &accountv1.GetVenueOnlineInfoResponse{
		Venue:  toProtoVenue(venue),
		Wallet: toProtoAccountWalletState(info),
	}, nil
}

func (s *AccountGRPCService) BindVenue(ctx context.Context, req *accountv1.BindVenueRequest) (*accountv1.BindVenueResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 || req.GetVenueId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id and venue_id are required")
	}
	if n, err := s.repo.CountActiveSessionsForAccount(ctx, req.GetUserId(), req.GetAccountId()); err != nil {
		return nil, mapRepoErr(err)
	} else if n > 0 {
		return nil, status.Error(codes.FailedPrecondition, "account has active sessions")
	}
	venue, err := s.repo.BindVenue(ctx, req.GetUserId(), req.GetAccountId(), req.GetVenueId(), strings.TrimSpace(req.GetReason()))
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.BindVenueResponse{Venue: toProtoVenue(venue)}, nil
}

func (s *AccountGRPCService) ReleaseVenue(ctx context.Context, req *accountv1.ReleaseVenueRequest) (*accountv1.ReleaseVenueResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetVenueId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "venue_id is required")
	}
	venue, err := s.repo.GetVenue(ctx, req.GetVenueId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if venue.AccountID != nil {
		if n, err := s.repo.CountActiveSessionsForAccount(ctx, req.GetUserId(), *venue.AccountID); err != nil {
			return nil, mapRepoErr(err)
		} else if n > 0 {
			return nil, status.Error(codes.FailedPrecondition, "account has active sessions")
		}
	}
	venue, err = s.repo.ReleaseVenue(ctx, req.GetUserId(), req.GetVenueId(), strings.TrimSpace(req.GetReason()))
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.ReleaseVenueResponse{Venue: toProtoVenue(venue)}, nil
}

func (s *AccountGRPCService) ArchiveVenue(ctx context.Context, req *accountv1.ArchiveVenueRequest) (*accountv1.ArchiveVenueResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetVenueId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "venue_id is required")
	}
	venue, err := s.repo.GetVenue(ctx, req.GetVenueId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if venue.AccountID != nil {
		if n, err := s.repo.CountActiveSessionsForAccount(ctx, req.GetUserId(), *venue.AccountID); err != nil {
			return nil, mapRepoErr(err)
		} else if n > 0 {
			return nil, status.Error(codes.FailedPrecondition, "account has active sessions")
		}
	}
	if err := s.repo.ArchiveVenue(ctx, req.GetUserId(), req.GetVenueId(), strings.TrimSpace(req.GetReason())); err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.ArchiveVenueResponse{}, nil
}

func (s *AccountGRPCService) PreflightStrategySession(ctx context.Context, req *accountv1.PreflightStrategySessionRequest) (*accountv1.PreflightStrategySessionResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	account, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	var issues []*accountv1.PreflightIssue
	if account.Status != 0 && account.Status != domain.AccountStatusActive {
		issues = append(issues, &accountv1.PreflightIssue{Code: "ACCOUNT_INACTIVE", Message: "account is not active"})
	}
	if n, err := s.repo.CountActiveSessionsForAccount(ctx, req.GetUserId(), req.GetAccountId()); err != nil {
		return nil, mapRepoErr(err)
	} else if n > 0 {
		issues = append(issues, &accountv1.PreflightIssue{Code: "ACTIVE_SESSION_EXISTS", Message: "account already has an active session"})
	}

	resolved := make([]*accountv1.VenueEntry, 0, len(req.GetRequiredRoutes()))
	for _, required := range req.GetRequiredRoutes() {
		exchangeValue, err := validateExchange(required.GetExchange())
		if err != nil {
			return nil, err
		}
		market, err := validateMarket(required.GetMarket())
		if err != nil {
			return nil, err
		}
		meta, err := s.repo.ResolveVenueRouteMeta(ctx, req.GetAccountId(), exchangeValue, market)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				issues = append(issues, &accountv1.PreflightIssue{
					Code:     "VENUE_MISSING",
					Message:  "active venue is missing",
					Exchange: int32(exchangeValue),
					Market:   int32(market),
				})
				continue
			}
			return nil, mapRepoErr(err)
		}
		if meta.Environment != environmentFromAccount(account) {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "VENUE_ENVIRONMENT_MISMATCH",
				Message:  "venue environment does not match account environment",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
			})
			continue
		}
		if meta.Environment != domain.EnvironmentBacktest && strings.TrimSpace(meta.CredentialInfo) == "" {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "VENUE_CREDENTIAL_MISSING",
				Message:  "venue credential is missing",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
			})
			continue
		}
		resolved = append(resolved, &accountv1.VenueEntry{
			VenueId:      meta.VenueID,
			UserId:       meta.UserID,
			AccountId:    meta.AccountID,
			Exchange:     int32(meta.Exchange),
			Market:       int32(meta.Market),
			Environment:  int32(meta.Environment),
			Status:       int32(domain.VenueStatusActive),
			ApiKey:       meta.APIKey,
			MarginMode:   int32(meta.MarginMode),
			PositionMode: int32(meta.PositionMode),
		})
	}
	for _, sym := range req.GetRequiredSymbols() {
		exchangeValue, err := validateExchange(sym.GetExchange())
		if err != nil {
			return nil, err
		}
		market, err := validateMarket(sym.GetMarket())
		if err != nil {
			return nil, err
		}
		symbol := strings.ToUpper(strings.TrimSpace(sym.GetSymbol()))
		if symbol == "" {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "symbol_rules_missing",
				Message:  "symbol is required for symbol rules preflight",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
			})
			continue
		}
		if s.registry == nil {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "symbol_rules_missing",
				Message:  "symbol rules registry is not configured",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
				Symbol:   symbol,
			})
			continue
		}
		route := adapter.Route{
			Exchange:    exchangeValue,
			Environment: environmentFromAccount(account),
			Market:      market,
		}
		reader, err := s.registry.SymbolRulesReader(route)
		if err != nil || reader == nil {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "symbol_rules_missing",
				Message:  "symbol rules reader is unavailable",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
				Symbol:   symbol,
			})
			continue
		}
		rules, err := reader.ReadSymbolRules(ctx, adapter.SymbolRulesRequest{Symbols: []string{symbol}})
		if err != nil || !symbolRulesContain(rules, symbol) {
			issues = append(issues, &accountv1.PreflightIssue{
				Code:     "symbol_rules_missing",
				Message:  "symbol rules are missing for requested symbol",
				Exchange: int32(exchangeValue),
				Market:   int32(market),
				Symbol:   symbol,
			})
			continue
		}
	}
	return &accountv1.PreflightStrategySessionResponse{
		Ok:             len(issues) == 0,
		Issues:         issues,
		ResolvedVenues: resolved,
	}, nil
}

func symbolRulesContain(rules adapter.SymbolRules, symbol string) bool {
	for _, rule := range rules.Symbols {
		if strings.EqualFold(strings.TrimSpace(rule.Symbol), symbol) {
			return true
		}
	}
	return false
}

func (s *AccountGRPCService) GetVenueRouteMeta(ctx context.Context, req *accountv1.GetVenueRouteMetaRequest) (*accountv1.GetVenueRouteMetaResponse, error) {
	if req.GetAccountId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	exchangeValue, err := validateExchange(req.GetExchange())
	if err != nil {
		return nil, err
	}
	market, err := validateMarket(req.GetMarket())
	if err != nil {
		return nil, err
	}
	meta, err := s.repo.ResolveVenueRouteMeta(ctx, req.GetAccountId(), exchangeValue, market)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	credentialJSON := ""
	if meta.Environment != domain.EnvironmentBacktest {
		if s.creds == nil {
			return nil, status.Error(codes.FailedPrecondition, "credential manager is not configured")
		}
		if strings.TrimSpace(meta.CredentialInfo) == "" {
			return nil, status.Error(codes.FailedPrecondition, "venue credential is missing")
		}
		credentialJSON, err = s.creds.Decrypt(meta.CredentialInfo)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "decrypt venue credential: %v", err)
		}
	}
	return &accountv1.GetVenueRouteMetaResponse{
		AccountId:      meta.AccountID,
		VenueId:        meta.VenueID,
		UserId:         meta.UserID,
		Environment:    int32(meta.Environment),
		Exchange:       int32(meta.Exchange),
		Market:         int32(meta.Market),
		MarginMode:     int32(meta.MarginMode),
		PositionMode:   int32(meta.PositionMode),
		ApiKey:         meta.APIKey,
		CredentialJson: credentialJSON,
		DefaultFeeRate: meta.DefaultFeeRate,
		SlippageBps:    meta.SlippageBps,
	}, nil
}

func toProtoRegistryEntry(a domain.Account) *accountv1.AccountRegistryEntry {
	env := environmentFromAccount(a)
	statusValue := a.Status
	if statusValue == 0 {
		statusValue = domain.AccountStatusActive
	}
	return &accountv1.AccountRegistryEntry{
		AccountId:   a.AccountID,
		Name:        a.Name,
		Environment: int32(env),
		CreatedAt:   timestamppb.New(a.CreatedAt),
		UserId:      a.UserID,
		Description: a.Description,
		Status:      int32(statusValue),
	}
}

func toProtoVenue(v domain.Venue) *accountv1.VenueEntry {
	var accountID int64
	if v.AccountID != nil {
		accountID = *v.AccountID
	}
	out := &accountv1.VenueEntry{
		VenueId:               v.VenueID,
		UserId:                v.UserID,
		AccountId:             accountID,
		Exchange:              int32(v.Exchange),
		Market:                int32(v.Market),
		Environment:           int32(v.Environment),
		Status:                int32(v.Status),
		DisplayName:           v.DisplayName,
		Description:           v.Description,
		ApiKey:                v.APIKey,
		CredentialFingerprint: v.CredentialFingerprint,
		MarginMode:            int32(v.MarginMode),
		PositionMode:          int32(v.PositionMode),
		CreatedAt:             timestamppb.New(v.CreatedAt),
		UpdatedAt:             timestamppb.New(v.UpdatedAt),
		ArchivedReason:        v.ArchivedReason,
	}
	if v.LastUsedAt != nil {
		out.LastUsedAt = timestamppb.New(*v.LastUsedAt)
	}
	if v.ArchivedAt != nil {
		out.ArchivedAt = timestamppb.New(*v.ArchivedAt)
	}
	return out
}

// GetOnlineAccountInfo returns wallet state: backtest from accounts table; live/testnet from exchange (then updates accounts table).
// 不再写快照——快照由独立的事件触发。
func (s *AccountGRPCService) GetOnlineAccountInfo(ctx context.Context, req *accountv1.GetOnlineAccountInfoRequest) (*accountv1.GetOnlineAccountInfoResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	resp, err := s.UpdatePortfolioSnapshot(ctx, &accountv1.UpdatePortfolioSnapshotRequest{
		AccountId: accountID,
		UserId:    req.GetUserId(),
	})
	if err != nil {
		return nil, err
	}
	return &accountv1.GetOnlineAccountInfoResponse{Wallet: resp.GetSnapshot().GetWallet()}, nil
}

func (s *AccountGRPCService) GetPortfolioSnapshot(ctx context.Context, req *accountv1.GetPortfolioSnapshotRequest) (*accountv1.GetPortfolioSnapshotResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	account, err := s.repo.GetAccount(ctx, accountID, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	snapshot, err := s.readPortfolioSnapshot(ctx, account)
	if err != nil {
		return nil, err
	}
	return &accountv1.GetPortfolioSnapshotResponse{Snapshot: toProtoPortfolioSnapshot(snapshot)}, nil
}

func (s *AccountGRPCService) UpdatePortfolioSnapshot(ctx context.Context, req *accountv1.UpdatePortfolioSnapshotRequest) (*accountv1.UpdatePortfolioSnapshotResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	account, err := s.repo.GetAccount(ctx, accountID, req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if err := s.requireActiveSessionForAccount(ctx, req.GetSessionId(), accountID, req.GetStrategyId(), account.UserID); err != nil {
		return nil, err
	}
	snapshot, err := s.readPortfolioSnapshot(ctx, account)
	if err != nil {
		return nil, err
	}
	info := onlineInfoFromPortfolioSnapshot(snapshot, account)
	if err := s.repo.UpdateAccountState(ctx, info); err != nil {
		return nil, status.Errorf(codes.Unavailable, "update account state: %v", err)
	}
	if reason := domain.SnapshotReason(req.GetSnapshotReason()); reason > 0 {
		if err := s.repo.SaveSnapshot(ctx, accountID, reason, req.GetStrategyId(), req.GetSessionId()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "save snapshot: %v", err)
		}
	}
	return &accountv1.UpdatePortfolioSnapshotResponse{Snapshot: toProtoPortfolioSnapshot(snapshot)}, nil
}

func (s *AccountGRPCService) readPortfolioSnapshot(ctx context.Context, account domain.Account) (adapter.PortfolioSnapshot, error) {
	if account.Mode == domain.AccountModeBacktest || account.Environment == domain.EnvironmentBacktest {
		info, err := s.repo.GetAccountState(ctx, account.AccountID)
		if err != nil {
			return adapter.PortfolioSnapshot{}, status.Errorf(codes.Unavailable, "read backtest portfolio snapshot: %v", err)
		}
		snapshot := portfolioSnapshotFromOnlineInfo(info, account)
		venues, err := s.repo.ListActiveAccountVenues(ctx, account.UserID, account.AccountID)
		if err != nil {
			return adapter.PortfolioSnapshot{}, mapRepoErr(err)
		}
		for _, venue := range venues {
			snapshot.VenueSnapshots = append(snapshot.VenueSnapshots, portfolioSnapshotFromOnlineInfoForVenue(info, account, venue))
		}
		return snapshot, nil
	}
	if s.registry == nil {
		return adapter.PortfolioSnapshot{}, status.Error(codes.FailedPrecondition, "exchange capability registry is not configured")
	}
	venues, err := s.repo.ListActiveAccountVenues(ctx, account.UserID, account.AccountID)
	if err != nil {
		return adapter.PortfolioSnapshot{}, mapRepoErr(err)
	}
	if len(venues) == 0 {
		return adapter.PortfolioSnapshot{}, status.Error(codes.FailedPrecondition, "account has no active venue")
	}

	out := adapter.PortfolioSnapshot{
		UserID:      account.UserID,
		AccountID:   account.AccountID,
		Environment: account.Environment,
		UpdatedAt:   time.Now().UTC(),
	}
	for _, venue := range venues {
		route := adapter.Route{Exchange: venue.Exchange, Environment: venue.Environment, Market: venue.Market}
		validator, err := s.registry.CredentialValidator(route)
		if err != nil {
			return adapter.PortfolioSnapshot{}, status.Errorf(codes.FailedPrecondition, "credential validator: %v", err)
		}
		reader, err := s.registry.AccountSnapshotReader(route)
		if err != nil {
			return adapter.PortfolioSnapshot{}, status.Errorf(codes.FailedPrecondition, "portfolio snapshot reader: %v", err)
		}
		parsed, err := s.parsedCredentialForVenue(ctx, venue, validator)
		if err != nil {
			return adapter.PortfolioSnapshot{}, err
		}
		venueSnapshot, err := reader.ReadPortfolioSnapshot(ctx, adapter.PortfolioSnapshotRequest{
			UserID:     account.UserID,
			AccountID:  account.AccountID,
			VenueID:    venue.VenueID,
			Credential: parsed,
		})
		if err != nil {
			return adapter.PortfolioSnapshot{}, status.Errorf(codes.Unavailable, "read venue portfolio snapshot: %v", err)
		}
		if venueSnapshot.Exchange == 0 {
			venueSnapshot.Exchange = venue.Exchange
		}
		if venueSnapshot.Environment == 0 {
			venueSnapshot.Environment = venue.Environment
		}
		if venueSnapshot.Market == 0 {
			venueSnapshot.Market = venue.Market
		}
		out.TotalValue += venueSnapshot.TotalValue
		out.WalletBalance += venueSnapshot.WalletBalance
		out.AvailableBalance += venueSnapshot.AvailableBalance
		out.Balances = append(out.Balances, venueSnapshot.Balances...)
		out.Positions = append(out.Positions, venueSnapshot.Positions...)
		out.OnlineInfo = mergePortfolioOnlineInfo(out.OnlineInfo, venueSnapshot.OnlineInfo, account)
		out.RawPayload = venueSnapshot.RawPayload
		if venueSnapshot.UpdatedAt.After(out.UpdatedAt) {
			out.UpdatedAt = venueSnapshot.UpdatedAt
		}
		out.VenueSnapshots = append(out.VenueSnapshots, venueSnapshot)
	}
	return out, nil
}

func (s *AccountGRPCService) parsedCredentialForVenue(ctx context.Context, venue domain.Venue, validator adapter.CredentialValidator) (adapter.ParsedCredential, error) {
	if venue.Environment == domain.EnvironmentBacktest {
		return adapter.ParsedCredential{Exchange: venue.Exchange, Environment: venue.Environment}, nil
	}
	if s.creds == nil {
		return adapter.ParsedCredential{}, status.Error(codes.FailedPrecondition, "credential manager is not configured")
	}
	if strings.TrimSpace(venue.CredentialInfo) == "" {
		return adapter.ParsedCredential{}, status.Error(codes.FailedPrecondition, "venue credential is missing")
	}
	credentialJSON, err := s.creds.Decrypt(venue.CredentialInfo)
	if err != nil {
		return adapter.ParsedCredential{}, status.Errorf(codes.FailedPrecondition, "decrypt venue credential: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(credentialJSON), &payload); err != nil {
		return adapter.ParsedCredential{}, status.Errorf(codes.FailedPrecondition, "invalid venue credential json: %v", err)
	}
	payload["api_key"] = venue.APIKey
	raw, err := json.Marshal(payload)
	if err != nil {
		return adapter.ParsedCredential{}, status.Errorf(codes.FailedPrecondition, "marshal venue credential json: %v", err)
	}
	parsed, err := validator.ValidateCredential(ctx, raw)
	if err != nil {
		return adapter.ParsedCredential{}, status.Errorf(codes.FailedPrecondition, "validate venue credential: %v", err)
	}
	if parsed.Exchange == 0 {
		parsed.Exchange = venue.Exchange
	}
	if parsed.Environment == 0 {
		parsed.Environment = venue.Environment
	}
	return parsed, nil
}

func portfolioSnapshotFromOnlineInfo(info domain.OnlineAccountInfo, account domain.Account) adapter.PortfolioSnapshot {
	if info.UpdatedAt.IsZero() {
		info.UpdatedAt = time.Now().UTC()
	}
	snapshot := adapter.PortfolioSnapshot{
		UserID:           account.UserID,
		AccountID:        account.AccountID,
		Environment:      account.Environment,
		TotalValue:       info.TotalValue,
		WalletBalance:    info.WalletBalance,
		AvailableBalance: info.AvailableBalance,
		Balances: []adapter.BalanceEntry{
			{
				Asset:            "USDT",
				WalletBalance:    info.WalletBalance,
				AvailableBalance: info.AvailableBalance,
				ValueUSDT:        info.WalletBalance,
			},
		},
		OnlineInfo: &info,
		UpdatedAt:  info.UpdatedAt,
	}
	for _, position := range info.Futures.Positions {
		snapshot.Positions = append(snapshot.Positions, adapter.PositionEntry{
			Symbol:           position.Symbol,
			PositionSide:     position.PositionSide,
			Qty:              position.PositionQty,
			EntryPrice:       position.EntryPrice,
			MarkPrice:        position.MarkPrice,
			UnrealizedPnl:    position.UnrealizedPnl,
			LiquidationPrice: position.LiquidationPrice,
		})
	}
	return snapshot
}

func portfolioSnapshotFromOnlineInfoForVenue(info domain.OnlineAccountInfo, account domain.Account, venue domain.Venue) adapter.PortfolioSnapshot {
	snapshot := portfolioSnapshotFromOnlineInfo(info, account)
	snapshot.VenueID = venue.VenueID
	snapshot.Exchange = venue.Exchange
	snapshot.Environment = venue.Environment
	snapshot.Market = venue.Market
	snapshot.VenueSnapshots = nil
	return snapshot
}

func onlineInfoFromPortfolioSnapshot(snapshot adapter.PortfolioSnapshot, account domain.Account) domain.OnlineAccountInfo {
	if snapshot.OnlineInfo != nil {
		info := *snapshot.OnlineInfo
		info.AccountID = account.AccountID
		info.Mode = account.Mode
		if info.UpdatedAt.IsZero() {
			info.UpdatedAt = snapshot.UpdatedAt
		}
		return info
	}
	futures := domain.FuturesWallet{
		MarginMode:       account.MarginMode,
		PositionMode:     account.PositionMode,
		WalletBalance:    snapshot.WalletBalance,
		AvailableBalance: snapshot.AvailableBalance,
		MarginBalance:    snapshot.WalletBalance,
	}
	for _, position := range snapshot.Positions {
		futures.Positions = append(futures.Positions, domain.FuturesPosition{
			Symbol:           position.Symbol,
			PositionSide:     position.PositionSide,
			PositionQty:      position.Qty,
			Qty:              position.Qty,
			EntryPrice:       position.EntryPrice,
			MarkPrice:        position.MarkPrice,
			UnrealizedPnl:    position.UnrealizedPnl,
			LiquidationPrice: position.LiquidationPrice,
		})
	}
	return domain.OnlineAccountInfo{
		AccountID:        account.AccountID,
		Mode:             account.Mode,
		Futures:          futures,
		TotalValue:       snapshot.TotalValue,
		WalletBalance:    snapshot.WalletBalance,
		AvailableBalance: snapshot.AvailableBalance,
		UpdatedAt:        snapshot.UpdatedAt,
	}
}

func mergePortfolioOnlineInfo(current *domain.OnlineAccountInfo, next *domain.OnlineAccountInfo, account domain.Account) *domain.OnlineAccountInfo {
	if next == nil {
		return current
	}
	merged := *next
	merged.AccountID = account.AccountID
	merged.Mode = account.Mode
	if current == nil {
		return &merged
	}
	merged.TotalValue += current.TotalValue
	merged.WalletBalance += current.WalletBalance
	merged.AvailableBalance += current.AvailableBalance
	merged.Futures.Positions = append(current.Futures.Positions, merged.Futures.Positions...)
	merged.Spot.Assets = append(current.Spot.Assets, merged.Spot.Assets...)
	if current.UpdatedAt.After(merged.UpdatedAt) {
		merged.UpdatedAt = current.UpdatedAt
	}
	return &merged
}

func toProtoPortfolioSnapshot(snapshot adapter.PortfolioSnapshot) *accountv1.PortfolioSnapshot {
	wallet := toProtoAccountWalletState(onlineInfoFromPortfolioSnapshot(snapshot, domain.Account{
		AccountID:   snapshot.AccountID,
		UserID:      snapshot.UserID,
		Environment: snapshot.Environment,
		Mode:        accountModeFromEnvironment(snapshot.Environment),
	}))
	out := &accountv1.PortfolioSnapshot{
		AccountId:        snapshot.AccountID,
		UserId:           snapshot.UserID,
		TotalValue:       snapshot.TotalValue,
		WalletBalance:    snapshot.WalletBalance,
		AvailableBalance: snapshot.AvailableBalance,
		UpdatedAt:        timestamppb.New(snapshot.UpdatedAt),
		Wallet:           wallet,
	}
	sourceVenues := snapshot.VenueSnapshots
	if len(sourceVenues) == 0 && snapshot.VenueID != 0 {
		sourceVenues = []adapter.PortfolioSnapshot{snapshot}
	}
	for _, venue := range sourceVenues {
		out.Venues = append(out.Venues, toProtoVenueSnapshot(venue))
	}
	return out
}

func toProtoVenueSnapshot(snapshot adapter.PortfolioSnapshot) *accountv1.VenueSnapshot {
	out := &accountv1.VenueSnapshot{
		VenueId:          snapshot.VenueID,
		Exchange:         int32(snapshot.Exchange),
		Environment:      int32(snapshot.Environment),
		Market:           int32(snapshot.Market),
		TotalValue:       snapshot.TotalValue,
		WalletBalance:    snapshot.WalletBalance,
		AvailableBalance: snapshot.AvailableBalance,
		UpdatedAt:        timestamppb.New(snapshot.UpdatedAt),
	}
	for _, balance := range snapshot.Balances {
		out.Balances = append(out.Balances, &accountv1.BalanceEntry{
			Asset:            balance.Asset,
			WalletBalance:    balance.WalletBalance,
			AvailableBalance: balance.AvailableBalance,
			Locked:           balance.Locked,
			ValueUsdt:        balance.ValueUSDT,
		})
	}
	for _, position := range snapshot.Positions {
		out.Positions = append(out.Positions, &accountv1.PositionEntry{
			Symbol:           position.Symbol,
			PositionSide:     position.PositionSide,
			Qty:              position.Qty,
			EntryPrice:       position.EntryPrice,
			MarkPrice:        position.MarkPrice,
			UnrealizedPnl:    position.UnrealizedPnl,
			MarginBalance:    position.MarginBalance,
			LiquidationPrice: position.LiquidationPrice,
		})
	}
	return out
}

// UpdateAccountWalletState branches on the account's registered mode.
// snapshot_reason > 0 时额外写一条快照。
func (s *AccountGRPCService) UpdateAccountWalletState(ctx context.Context, req *accountv1.UpdateAccountWalletStateRequest) (*accountv1.UpdateAccountWalletStateResponse, error) {
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	account, err := s.repo.GetAccount(ctx, accountID, 0)
	if err != nil {
		return nil, mapRepoErr(err)
	}

	snapshotReason := domain.SnapshotReason(req.GetSnapshotReason())
	strategyID := req.GetStrategyId()
	sessionID := req.GetSessionId()
	if err := s.requireActiveSessionForAccount(ctx, sessionID, accountID, strategyID, account.UserID); err != nil {
		return nil, err
	}

	switch account.Mode {
	case domain.AccountModeBacktest:
		info := domain.OnlineAccountInfo{
			AccountID:        accountID,
			Mode:             account.Mode,
			TotalValue:       req.GetTotalValue(),
			WalletBalance:    req.GetWalletBalance(),
			AvailableBalance: req.GetAvailableBalance(),
			UpdatedAt:        time.Now().UTC(),
		}
		if f := req.GetFutures(); f != nil {
			info.Futures = fromProtoFuturesWallet(f)
		}
		if info.Futures.MarginMode == "" {
			info.Futures.MarginMode = account.MarginMode
		}
		if info.Futures.PositionMode == "" {
			info.Futures.PositionMode = account.PositionMode
		}
		if info.Futures.WalletBalance == 0 && req.GetWalletBalance() != 0 {
			info.Futures.WalletBalance = req.GetWalletBalance()
		}
		if info.Futures.AvailableBalance == 0 && req.GetAvailableBalance() != 0 {
			info.Futures.AvailableBalance = req.GetAvailableBalance()
		}
		if info.Futures.UnrealizedPnl == 0 && info.Futures.TotalUnrealizedPnl != 0 {
			info.Futures.UnrealizedPnl = info.Futures.TotalUnrealizedPnl
		}
		if info.Futures.MarginBalance == 0 {
			info.Futures.MarginBalance = info.Futures.WalletBalance + info.Futures.UnrealizedPnl
		}
		if info.Futures.TotalMarginBalance == 0 {
			info.Futures.TotalMarginBalance = info.Futures.MarginBalance
		}
		if sp := req.GetSpot(); sp != nil {
			info.Spot = fromProtoSpotWallet(sp)
		}
		if err := s.repo.UpdateAccountState(ctx, info); err != nil {
			return nil, status.Errorf(codes.Unavailable, "update account state: %v", err)
		}
		if snapshotReason > 0 {
			if err := s.repo.SaveSnapshot(ctx, accountID, snapshotReason, strategyID, sessionID); err != nil {
				return nil, status.Errorf(codes.Unavailable, "save snapshot: %v", err)
			}
		}
		saved, err := s.repo.GetAccountState(ctx, accountID)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read account state: %v", err)
		}
		return &accountv1.UpdateAccountWalletStateResponse{Wallet: toProtoAccountWalletState(saved)}, nil

	case domain.AccountModeBinanceLive, domain.AccountModeBinanceTestnet:
		snapshot, err := s.readPortfolioSnapshot(ctx, account)
		if err != nil {
			return nil, err
		}
		info := onlineInfoFromPortfolioSnapshot(snapshot, account)
		if err := s.repo.UpdateAccountState(ctx, info); err != nil {
			return nil, status.Errorf(codes.Unavailable, "update account state: %v", err)
		}
		if snapshotReason > 0 {
			if err := s.repo.SaveSnapshot(ctx, accountID, snapshotReason, strategyID, sessionID); err != nil {
				return nil, status.Errorf(codes.Unavailable, "save snapshot: %v", err)
			}
		}

		// Phase C shadow-compare: fire-and-forget goroutine. Reuses the
		// authoritative snapshot we just fetched (no second Binance call)
		// and the local canonical snapshot from the request. Never blocks
		// the response; errors / panics stay inside the goroutine.
		//
		// Guard fires when EITHER a futures or spot payload is present.
		// Spot-only accounts must still get reconciliation; filtering by
		// futures alone was a blind-spot. The "no wallet payload at all"
		// case (neither futures nor spot) still short-circuits, because
		// comparing a blank local wallet against an authoritative one
		// would produce spurious soft-fails across every field.
		if s.reconciler != nil && s.reconciler.Enabled() && (req.GetFutures() != nil || req.GetSpot() != nil) {
			local := domain.OnlineAccountInfo{
				AccountID:        accountID,
				Mode:             account.Mode,
				Futures:          fromProtoFuturesWallet(req.GetFutures()),
				TotalValue:       req.GetTotalValue(),
				WalletBalance:    req.GetWalletBalance(),
				AvailableBalance: req.GetAvailableBalance(),
				UpdatedAt:        time.Now().UTC(),
			}
			if sp := req.GetSpot(); sp != nil {
				local.Spot = fromProtoSpotWallet(sp)
			}
			s.reconciler.LaunchAsync(reconciliation.Task{
				Account:        account,
				Local:          local,
				Exchange:       info,
				SessionID:      sessionID,
				StrategyID:     strategyID,
				SnapshotReason: snapshotReason,
				TriggerTime:    time.Now().UTC(),
			})
		}

		return &accountv1.UpdateAccountWalletStateResponse{Wallet: toProtoAccountWalletState(info)}, nil

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported account mode: %d", account.Mode)
	}
}

func toProtoAccountWalletState(info domain.OnlineAccountInfo) *accountv1.AccountWalletState {
	disp := walletmetrics.ComputeDisplay(info)
	return &accountv1.AccountWalletState{
		Futures:               toProtoFuturesWalletWithDisplay(info.Futures, disp.PositionDisplay),
		Spot:                  toProtoSpotWallet(info.Spot),
		TotalValue:            info.TotalValue,
		Mode:                  int32(info.Mode),
		UpdatedAt:             timestamppb.New(info.UpdatedAt),
		SpotEstimatedValue:    disp.SpotEstimated,
		FuturesPositionEquity: disp.FuturesEquity,
		MetricsAuthoritative:  disp.Authoritative,
	}
}

func toProtoFuturesWalletWithDisplay(fw domain.FuturesWallet, display []*float64) *accountv1.FuturesWallet {
	positions := make([]*accountv1.FuturesPosition, 0, len(fw.Positions))
	for i, p := range fw.Positions {
		positionQty := p.PositionQty
		if positionQty == 0 {
			positionQty = p.Qty
		}
		marginMode := p.MarginMode
		if marginMode == "" {
			marginMode = p.MarginType
		}
		if marginMode == "" {
			marginMode = fw.MarginMode
		}
		if marginMode == "" {
			marginMode = "cross"
		}
		fp := &accountv1.FuturesPosition{
			Symbol:         p.Symbol,
			Direction:      p.Direction,
			InitialBalance: p.InitialBalance,
			Leverage:       p.Leverage,
			FeeRate:        p.FeeRate,
			MarkPrice:      p.MarkPrice,
			Qty:            p.Qty,
			PositionQty:    positionQty,
			EntryPrice:     p.EntryPrice,
			UnrealizedPnl:  p.UnrealizedPnl,
			PositionSide:   p.PositionSide,
			// Phase A additive fields
			MarginType:             p.MarginType,
			MarginMode:             marginMode,
			Notional:               p.Notional,
			InitialMargin:          p.InitialMargin,
			PositionInitialMargin:  p.PositionInitialMargin,
			OpenOrderInitialMargin: p.OpenOrderInitialMargin,
			MaintMargin:            p.MaintMargin,
			IsolatedWallet:         p.IsolatedWallet,
			LiquidationPrice:       p.LiquidationPrice,
			BreakEvenPrice:         p.BreakEvenPrice,
		}
		if display != nil && i < len(display) && display[i] != nil {
			v := *display[i]
			fp.DisplayEquity = &v
		}
		positions = append(positions, fp)
	}
	riskMetadata := make([]*accountv1.FuturesRiskMetadata, 0, len(fw.RiskMetadata))
	for _, item := range fw.RiskMetadata {
		brackets := make([]*accountv1.FuturesRiskBracket, 0, len(item.Brackets))
		for _, bracket := range item.Brackets {
			brackets = append(brackets, &accountv1.FuturesRiskBracket{
				Bracket:          bracket.Bracket,
				NotionalFloor:    bracket.NotionalFloor,
				NotionalCap:      bracket.NotionalCap,
				InitialLeverage:  bracket.InitialLeverage,
				MaintMarginRatio: bracket.MaintMarginRatio,
				Cumulative:       bracket.Cumulative,
			})
		}
		riskMetadata = append(riskMetadata, &accountv1.FuturesRiskMetadata{
			Symbol:               item.Symbol,
			ConfiguredLeverage:   item.ConfiguredLeverage,
			ConfiguredMarginMode: item.ConfiguredMarginMode,
			PricePrecision:       item.PricePrecision,
			QuantityPrecision:    item.QuantityPrecision,
			TickSize:             item.TickSize,
			StepSize:             item.StepSize,
			Brackets:             brackets,
		})
	}
	marginBalance := fw.MarginBalance
	if marginBalance == 0 {
		marginBalance = fw.TotalMarginBalance
	}
	unrealizedPnL := fw.UnrealizedPnl
	if unrealizedPnL == 0 {
		unrealizedPnL = fw.TotalUnrealizedPnl
	}
	walletMarginMode := fw.MarginMode
	if walletMarginMode == "" {
		walletMarginMode = "cross"
	}
	positionMode := fw.PositionMode
	if positionMode == "" {
		positionMode = "one_way"
	}
	return &accountv1.FuturesWallet{
		MarginMode:         walletMarginMode,
		PositionMode:       positionMode,
		InitialBalance:     fw.InitialBalance,
		DepositSum:         fw.DepositSum,
		WithdrawalSum:      fw.WithdrawalSum,
		Positions:          positions,
		WalletBalance:      fw.WalletBalance,
		AvailableBalance:   fw.AvailableBalance,
		TotalUnrealizedPnl: fw.TotalUnrealizedPnl,
		// Phase A additive account-level fields
		TotalMarginBalance:          fw.TotalMarginBalance,
		TotalPositionInitialMargin:  fw.TotalPositionInitialMargin,
		TotalOpenOrderInitialMargin: fw.TotalOpenOrderInitialMargin,
		TotalMaintMargin:            fw.TotalMaintMargin,
		TotalCrossWalletBalance:     fw.TotalCrossWalletBalance,
		TotalCrossUnPnl:             fw.TotalCrossUnPnl,
		RiskMetadata:                riskMetadata,
		MarginBalance:               marginBalance,
		UnrealizedPnl:               unrealizedPnL,
		MultiAssetsMode:             fw.MultiAssetsMode,
		PortfolioMargin:             fw.PortfolioMargin,
		DisplayWalletBalanceUsd:     fw.DisplayWalletBalanceUsd,
		DisplayMarginBalanceUsd:     fw.DisplayMarginBalanceUsd,
		DisplayUnrealizedPnlUsd:     fw.DisplayUnrealizedPnlUsd,
	}
}

func toProtoSpotWallet(sw domain.SpotWallet) *accountv1.SpotWallet {
	assets := make([]*accountv1.SpotAsset, 0, len(sw.Assets))
	for _, a := range sw.Assets {
		asset := &accountv1.SpotAsset{
			Symbol:        a.Symbol,
			Qty:           a.Qty,
			Locked:        a.Locked,
			AvgEntryPrice: a.AvgEntryPrice,
		}
		if a.Price != nil {
			asset.Price = a.Price
		}
		assets = append(assets, asset)
	}
	return &accountv1.SpotWallet{Free: sw.Free, Locked: sw.Locked, Assets: assets}
}

func fromProtoFuturesWallet(f *accountv1.FuturesWallet) domain.FuturesWallet {
	positions := make([]domain.FuturesPosition, 0, len(f.GetPositions()))
	for _, p := range f.GetPositions() {
		positionQty := p.GetPositionQty()
		if positionQty == 0 {
			positionQty = p.GetQty()
		}
		marginMode := p.GetMarginMode()
		if marginMode == "" {
			marginMode = p.GetMarginType()
		}
		if marginMode == "" {
			marginMode = f.GetMarginMode()
		}
		if marginMode == "" {
			marginMode = "cross"
		}
		positions = append(positions, domain.FuturesPosition{
			Symbol:         p.GetSymbol(),
			Direction:      p.GetDirection(),
			InitialBalance: p.GetInitialBalance(),
			Leverage:       p.GetLeverage(),
			FeeRate:        p.GetFeeRate(),
			MarkPrice:      p.GetMarkPrice(),
			Qty:            p.GetQty(),
			PositionQty:    positionQty,
			EntryPrice:     p.GetEntryPrice(),
			UnrealizedPnl:  p.GetUnrealizedPnl(),
			PositionSide:   p.GetPositionSide(),
			// Phase A additive fields
			MarginType:             p.GetMarginType(),
			MarginMode:             marginMode,
			Notional:               p.GetNotional(),
			InitialMargin:          p.GetInitialMargin(),
			PositionInitialMargin:  p.GetPositionInitialMargin(),
			OpenOrderInitialMargin: p.GetOpenOrderInitialMargin(),
			MaintMargin:            p.GetMaintMargin(),
			IsolatedWallet:         p.GetIsolatedWallet(),
			LiquidationPrice:       p.GetLiquidationPrice(),
			BreakEvenPrice:         p.GetBreakEvenPrice(),
		})
	}
	riskMetadata := make([]domain.FuturesRiskMetadata, 0, len(f.GetRiskMetadata()))
	for _, item := range f.GetRiskMetadata() {
		brackets := make([]domain.FuturesRiskBracket, 0, len(item.GetBrackets()))
		for _, bracket := range item.GetBrackets() {
			brackets = append(brackets, domain.FuturesRiskBracket{
				Bracket:          bracket.GetBracket(),
				NotionalFloor:    bracket.GetNotionalFloor(),
				NotionalCap:      bracket.GetNotionalCap(),
				InitialLeverage:  bracket.GetInitialLeverage(),
				MaintMarginRatio: bracket.GetMaintMarginRatio(),
				Cumulative:       bracket.GetCumulative(),
			})
		}
		riskMetadata = append(riskMetadata, domain.FuturesRiskMetadata{
			Symbol:               item.GetSymbol(),
			ConfiguredLeverage:   item.GetConfiguredLeverage(),
			ConfiguredMarginMode: item.GetConfiguredMarginMode(),
			PricePrecision:       item.GetPricePrecision(),
			QuantityPrecision:    item.GetQuantityPrecision(),
			TickSize:             item.GetTickSize(),
			StepSize:             item.GetStepSize(),
			Brackets:             brackets,
		})
	}
	marginBalance := f.GetMarginBalance()
	if marginBalance == 0 {
		marginBalance = f.GetTotalMarginBalance()
	}
	unrealizedPnL := f.GetUnrealizedPnl()
	if unrealizedPnL == 0 {
		unrealizedPnL = f.GetTotalUnrealizedPnl()
	}
	if marginBalance == 0 {
		marginBalance = f.GetWalletBalance() + unrealizedPnL
	}
	totalMarginBalance := f.GetTotalMarginBalance()
	if totalMarginBalance == 0 {
		totalMarginBalance = marginBalance
	}
	walletMarginMode := f.GetMarginMode()
	if walletMarginMode == "" {
		walletMarginMode = "cross"
	}
	positionMode := f.GetPositionMode()
	if positionMode == "" {
		positionMode = "one_way"
	}
	return domain.FuturesWallet{
		MarginMode:         walletMarginMode,
		PositionMode:       positionMode,
		InitialBalance:     f.GetInitialBalance(),
		DepositSum:         f.GetDepositSum(),
		WithdrawalSum:      f.GetWithdrawalSum(),
		Positions:          positions,
		WalletBalance:      f.GetWalletBalance(),
		AvailableBalance:   f.GetAvailableBalance(),
		TotalUnrealizedPnl: f.GetTotalUnrealizedPnl(),
		// Phase A additive account-level fields
		TotalMarginBalance:          totalMarginBalance,
		TotalPositionInitialMargin:  f.GetTotalPositionInitialMargin(),
		TotalOpenOrderInitialMargin: f.GetTotalOpenOrderInitialMargin(),
		TotalMaintMargin:            f.GetTotalMaintMargin(),
		TotalCrossWalletBalance:     f.GetTotalCrossWalletBalance(),
		TotalCrossUnPnl:             f.GetTotalCrossUnPnl(),
		RiskMetadata:                riskMetadata,
		MarginBalance:               marginBalance,
		UnrealizedPnl:               unrealizedPnL,
		MultiAssetsMode:             f.GetMultiAssetsMode(),
		PortfolioMargin:             f.GetPortfolioMargin(),
		DisplayWalletBalanceUsd:     f.GetDisplayWalletBalanceUsd(),
		DisplayMarginBalanceUsd:     f.GetDisplayMarginBalanceUsd(),
		DisplayUnrealizedPnlUsd:     f.GetDisplayUnrealizedPnlUsd(),
	}
}

func fromProtoSpotWallet(s *accountv1.SpotWallet) domain.SpotWallet {
	assets := make([]domain.SpotAsset, 0, len(s.GetAssets()))
	for _, a := range s.GetAssets() {
		asset := domain.SpotAsset{
			Symbol:        a.GetSymbol(),
			Qty:           a.GetQty(),
			Locked:        a.GetLocked(),
			AvgEntryPrice: a.GetAvgEntryPrice(),
		}
		if a.Price != nil {
			asset.Price = a.Price
		}
		assets = append(assets, asset)
	}
	return domain.SpotWallet{Free: s.GetFree(), Locked: s.GetLocked(), Assets: assets}
}

// GetAccountMeta returns full account config including API credentials.
// Internal use only — intended for the internal order module; NOT safe to expose to BFFs.
func (s *AccountGRPCService) GetAccountMeta(ctx context.Context, req *accountv1.GetAccountMetaRequest) (*accountv1.GetAccountMetaResponse, error) {
	accountID := req.GetAccountId()
	if accountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	account, err := s.repo.GetAccount(ctx, accountID, 0)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.GetAccountMetaResponse{
		AccountId:      account.AccountID,
		Mode:           int32(account.Mode),
		MarginMode:     account.MarginMode,
		PositionMode:   account.PositionMode,
		ApiKey:         account.APIKey,
		ApiSecret:      account.APISecret,
		DefaultFeeRate: account.DefaultFeeRate,
		SlippageBps:    account.SlippageBps,
		UserId:         account.UserID,
	}, nil
}

// ── 策略管理 RPC ──────────────────────────────────────────────────────────────

func (s *AccountGRPCService) CreateStrategy(ctx context.Context, req *accountv1.CreateStrategyRequest) (*accountv1.CreateStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	ver := strings.TrimSpace(req.GetVersion())
	if !reValidVersion.MatchString(ver) {
		return nil, status.Error(codes.InvalidArgument, "version must be semver (e.g. 1.0.0)")
	}
	code := req.GetCode()
	if !reMyStrategyClass.MatchString(code) {
		return nil, status.Error(codes.InvalidArgument, "code must define 'class MyStrategy'")
	}
	if !reOnMarketData.MatchString(code) {
		return nil, status.Error(codes.InvalidArgument, "code must define 'def on_market_data(self, data, wallet)'")
	}

	st := domain.Strategy{
		UserID:         req.GetUserId(),
		Name:           name,
		Version:        ver,
		Description:    strings.TrimSpace(req.GetDescription()),
		Code:           code,
		RuntimeVersion: normalizeServiceRuntimeVersion(req.GetRuntimeVersion()),
		RuntimeProfile: normalizeServiceRuntimeProfile(req.GetRuntimeProfile()),
	}
	id, err := s.repo.CreateStrategy(ctx, st)
	if err != nil {
		// name+version unique constraint violation
		if isDuplicateErr(err) {
			return nil, status.Errorf(codes.AlreadyExists, "strategy %s@%s already exists", name, ver)
		}
		return nil, status.Errorf(codes.Internal, "create strategy: %v", err)
	}
	st.StrategyID = id
	return &accountv1.CreateStrategyResponse{Strategy: toProtoStrategy(st, true)}, nil
}

func (s *AccountGRPCService) ListStrategies(ctx context.Context, req *accountv1.ListStrategiesRequest) (*accountv1.ListStrategiesResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetLimit() > 0 || req.GetOffset() > 0 {
		list, meta, err := s.repo.ListStrategiesPage(ctx, req.GetUserId(), req.GetNamePrefix(), req.GetActiveOnly(), int(req.GetLimit()), int(req.GetOffset()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list strategies: %v", err)
		}
		out := make([]*accountv1.StrategyEntry, 0, len(list))
		for _, st := range list {
			out = append(out, toProtoStrategy(st, false))
		}
		return &accountv1.ListStrategiesResponse{Strategies: out, HasMore: meta.HasMore, Total: meta.Total}, nil
	}
	list, err := s.repo.ListStrategies(ctx, req.GetUserId(), req.GetNamePrefix(), req.GetActiveOnly())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list strategies: %v", err)
	}
	out := make([]*accountv1.StrategyEntry, 0, len(list))
	for _, st := range list {
		out = append(out, toProtoStrategy(st, false)) // no code in list
	}
	return &accountv1.ListStrategiesResponse{Strategies: out, Total: int64(len(out))}, nil
}

func (s *AccountGRPCService) GetStrategy(ctx context.Context, req *accountv1.GetStrategyRequest) (*accountv1.GetStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "strategy_id is required")
	}
	st, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId())
	if err != nil {
		return nil, mapStrategyErr(err)
	}
	return &accountv1.GetStrategyResponse{Strategy: toProtoStrategy(st, true)}, nil
}

func (s *AccountGRPCService) ArchiveStrategy(ctx context.Context, req *accountv1.ArchiveStrategyRequest) (*accountv1.ArchiveStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "strategy_id is required")
	}
	if _, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	if err := s.repo.ArchiveStrategy(ctx, req.GetStrategyId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	return &accountv1.ArchiveStrategyResponse{}, nil
}

func (s *AccountGRPCService) MountStrategy(ctx context.Context, req *accountv1.MountStrategyRequest) (*accountv1.MountStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 || req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id and strategy_id are required")
	}
	if _, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	// Verify strategy exists and is not archived
	st, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId())
	if err != nil {
		return nil, mapStrategyErr(err)
	}
	if st.Archived {
		return nil, status.Error(codes.FailedPrecondition, "cannot mount an archived strategy")
	}
	if err := s.repo.MountStrategy(ctx, req.GetAccountId(), req.GetStrategyId()); err != nil {
		return nil, status.Errorf(codes.Internal, "mount strategy: %v", err)
	}
	return &accountv1.MountStrategyResponse{}, nil
}

func (s *AccountGRPCService) UnmountStrategy(ctx context.Context, req *accountv1.UnmountStrategyRequest) (*accountv1.UnmountStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 || req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id and strategy_id are required")
	}
	if _, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	if _, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	// 检查是否在尝试卸载 active 策略
	active, err := s.repo.GetActiveStrategy(ctx, req.GetAccountId())
	if err == nil && active.StrategyID == req.GetStrategyId() {
		return nil, status.Error(codes.FailedPrecondition, "cannot unmount the active strategy; deactivate it first")
	}
	if err := s.repo.UnmountStrategy(ctx, req.GetAccountId(), req.GetStrategyId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	return &accountv1.UnmountStrategyResponse{}, nil
}

func (s *AccountGRPCService) ActivateStrategy(ctx context.Context, req *accountv1.ActivateStrategyRequest) (*accountv1.ActivateStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 || req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id and strategy_id are required")
	}
	if _, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	if _, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	if err := s.repo.ActivateStrategy(ctx, req.GetAccountId(), req.GetStrategyId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	return &accountv1.ActivateStrategyResponse{}, nil
}

func (s *AccountGRPCService) DeactivateStrategy(ctx context.Context, req *accountv1.DeactivateStrategyRequest) (*accountv1.DeactivateStrategyResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 || req.GetStrategyId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id and strategy_id are required")
	}
	if _, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	if _, err := s.repo.GetStrategy(ctx, req.GetStrategyId(), req.GetUserId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	if err := s.repo.DeactivateStrategy(ctx, req.GetAccountId(), req.GetStrategyId()); err != nil {
		return nil, mapStrategyErr(err)
	}
	return &accountv1.DeactivateStrategyResponse{}, nil
}

func (s *AccountGRPCService) ListAccountStrategies(ctx context.Context, req *accountv1.ListAccountStrategiesRequest) (*accountv1.ListAccountStrategiesResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetAccountId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	if _, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId()); err != nil {
		return nil, mapRepoErr(err)
	}
	list, err := s.repo.ListAccountStrategies(ctx, req.GetAccountId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list account strategies: %v", err)
	}
	out := make([]*accountv1.AccountStrategyEntry, 0, len(list))
	for _, entry := range list {
		if entry.Strategy.UserID != 0 && entry.Strategy.UserID != req.GetUserId() {
			continue
		}
		out = append(out, &accountv1.AccountStrategyEntry{
			Strategy:  toProtoStrategy(entry.Strategy, false),
			Active:    entry.Active,
			MountedAt: timestamppb.New(entry.MountedAt),
		})
	}
	return &accountv1.ListAccountStrategiesResponse{Entries: out}, nil
}

func (s *AccountGRPCService) GetActiveStrategy(ctx context.Context, req *accountv1.GetActiveStrategyRequest) (*accountv1.GetActiveStrategyResponse, error) {
	if req.GetAccountId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	st, err := s.repo.GetActiveStrategy(ctx, req.GetAccountId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			// No active strategy — return zero ID
			return &accountv1.GetActiveStrategyResponse{StrategyId: 0}, nil
		}
		return nil, status.Errorf(codes.Internal, "get active strategy: %v", err)
	}
	return &accountv1.GetActiveStrategyResponse{
		StrategyId:     st.StrategyID,
		Code:           st.Code,
		Name:           st.Name,
		Version:        st.Version,
		RuntimeVersion: st.RuntimeVersion,
		RuntimeProfile: st.RuntimeProfile,
	}, nil
}

func toProtoStrategy(st domain.Strategy, includeCode bool) *accountv1.StrategyEntry {
	e := &accountv1.StrategyEntry{
		StrategyId:     st.StrategyID,
		Name:           st.Name,
		Version:        st.Version,
		Description:    st.Description,
		Archived:       st.Archived,
		CreatedAt:      timestamppb.New(st.CreatedAt),
		UserId:         st.UserID,
		RuntimeVersion: normalizeServiceRuntimeVersion(st.RuntimeVersion),
		RuntimeProfile: normalizeServiceRuntimeProfile(st.RuntimeProfile),
	}
	if includeCode {
		e.Code = st.Code
	}
	return e
}

// ── Session RPC ─────────────────────────────────────────────────────────────

func (s *AccountGRPCService) SaveSession(ctx context.Context, req *accountv1.SaveSessionRequest) (*accountv1.SaveSessionResponse, error) {
	sessionType := normalizeServiceSessionType(int(req.GetMode()), req.GetSessionType())
	if req.GetSessionId() == "" || req.GetAccountId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "session_id and account_id are required")
	}
	if req.GetStrategyId() == 0 && sessionType != sessionTypeDebugging {
		return nil, status.Error(codes.InvalidArgument, "strategy_id is required unless session_type=debugging")
	}
	if strings.TrimSpace(req.GetRuntimeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	account, err := s.repo.GetAccount(ctx, req.GetAccountId(), req.GetUserId())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	env := environmentFromAccount(account)
	sess := domain.StrategySession{
		SessionID:      req.GetSessionId(),
		AccountID:      req.GetAccountId(),
		UserID:         account.UserID,
		StrategyID:     req.GetStrategyId(),
		Environment:    env,
		Mode:           int(accountModeFromEnvironment(env)),
		Status:         "running",
		Interval:       req.GetInterval(),
		RuntimeID:      req.GetRuntimeId(),
		RuntimeSource:  req.GetRuntimeSource(),
		RuntimeName:    req.GetRuntimeName(),
		SessionType:    sessionType,
		RuntimeVersion: normalizeServiceRuntimeVersion(req.GetRuntimeVersion()),
		SessionName:    strings.TrimSpace(req.GetSessionName()),
		StartedAt:      time.Now().UTC(),
	}
	if req.GetStartTimeMs() != 0 {
		v := req.GetStartTimeMs()
		sess.StartTimeMs = &v
	}
	if req.GetEndTimeMs() != 0 {
		v := req.GetEndTimeMs()
		sess.EndTimeMs = &v
	}
	if sess.Interval == "" {
		sess.Interval = "1m"
	}
	if err := s.repo.SaveSession(ctx, sess); err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.SaveSessionResponse{}, nil
}

func (s *AccountGRPCService) UpdateSession(ctx context.Context, req *accountv1.UpdateSessionRequest) (*accountv1.UpdateSessionResponse, error) {
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	existing, err := s.repo.GetSession(ctx, req.GetSessionId(), 0)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "session not found")
		}
		return nil, status.Errorf(codes.Internal, "get session: %v", err)
	}
	if !isActiveSessionStatus(existing.Status) {
		return nil, status.Errorf(codes.FailedPrecondition, "session %s is not active: %s", req.GetSessionId(), existing.Status)
	}
	if err := s.repo.UpdateSession(ctx, req.GetSessionId(), req.GetStatus(), int(req.GetBarsProcessed()), req.GetError(), req.GetRuntimeId()); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "session not found")
		}
		return nil, status.Errorf(codes.Internal, "update session: %v", err)
	}
	return &accountv1.UpdateSessionResponse{}, nil
}

func (s *AccountGRPCService) GetSession(ctx context.Context, req *accountv1.GetSessionRequest) (*accountv1.GetSessionResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := s.repo.GetSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "session not found")
		}
		return nil, status.Errorf(codes.Internal, "get session: %v", err)
	}
	return &accountv1.GetSessionResponse{Session: toProtoSession(sess)}, nil
}

func (s *AccountGRPCService) ListSessions(ctx context.Context, req *accountv1.ListSessionsRequest) (*accountv1.ListSessionsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	list, meta, err := s.repo.ListSessionsPage(ctx, repository.SessionListFilter{
		AccountID:         req.GetAccountId(),
		UserID:            req.GetUserId(),
		RuntimeID:         strings.TrimSpace(req.GetRuntimeId()),
		StrategyID:        req.GetStrategyId(),
		Mode:              int(req.GetMode()),
		ModeSet:           req.GetModeSet(),
		Status:            strings.TrimSpace(req.GetStatus()),
		SessionIDContains: strings.TrimSpace(req.GetSessionIdContains()),
		StartedAfterMs:    req.GetStartedAfterMs(),
		StartedBeforeMs:   req.GetStartedBeforeMs(),
		Limit:             int(req.GetLimit()),
		Offset:            int(req.GetOffset()),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sessions: %v", err)
	}
	out := make([]*accountv1.StrategySessionEntry, 0, len(list))
	for _, sess := range list {
		out = append(out, toProtoSession(sess))
	}
	return &accountv1.ListSessionsResponse{Sessions: out, HasMore: meta.HasMore, Total: meta.Total}, nil
}

func (s *AccountGRPCService) ListRunningSessions(ctx context.Context, req *accountv1.ListRunningSessionsRequest) (*accountv1.ListRunningSessionsResponse, error) {
	list, err := s.repo.ListRunningSessions(ctx, req.GetRuntimeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list running sessions: %v", err)
	}
	out := make([]*accountv1.StrategySessionEntry, 0, len(list))
	for _, sess := range list {
		out = append(out, toProtoSession(sess))
	}
	return &accountv1.ListRunningSessionsResponse{Sessions: out}, nil
}

func (s *AccountGRPCService) MarkRuntimeSessionsRecoverable(ctx context.Context, req *accountv1.MarkRuntimeSessionsRecoverableRequest) (*accountv1.MarkRuntimeSessionsRecoverableResponse, error) {
	runtimeID := strings.TrimSpace(req.GetRuntimeId())
	if runtimeID == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime_id is required")
	}
	count, err := s.repo.MarkRuntimeSessionsRecoverable(ctx, runtimeID, req.GetError())
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return &accountv1.MarkRuntimeSessionsRecoverableResponse{SessionsMarked: count}, nil
}

// Pagination bounds shared by every session-scoped audit list endpoint;
// see "paginate-session-detail-lists" spec.
const (
	auditListDefaultLimit = 20
	auditListMaxLimit     = 200
)

// clampAuditListLimit enforces the shared audit-list paging contract:
// non-positive → default (20); oversized → silently clamped to max (200).
func clampAuditListLimit(raw int32) int {
	v := int(raw)
	if v <= 0 {
		return auditListDefaultLimit
	}
	if v > auditListMaxLimit {
		return auditListMaxLimit
	}
	return v
}

func clampAuditListOffset(raw int32) int {
	v := int(raw)
	if v < 0 {
		return 0
	}
	return v
}

func (s *AccountGRPCService) ListSessionSnapshots(ctx context.Context, req *accountv1.ListSessionSnapshotsRequest) (*accountv1.ListSessionSnapshotsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	limit := clampAuditListLimit(req.GetLimit())
	offset := clampAuditListOffset(req.GetOffset())
	rows, total, hasMore, err := s.repo.ListSessionSnapshots(ctx, req.GetSessionId(), req.GetUserId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list session snapshots: %v", err)
	}
	out := make([]*accountv1.SnapshotEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, &accountv1.SnapshotEntry{
			Time:             timestamppb.New(r.Time),
			AccountId:        r.AccountID,
			SnapshotReason:   int32(r.SnapshotReason),
			TotalValue:       r.TotalValue,
			WalletBalance:    r.WalletBalance,
			AvailableBalance: r.AvailableBalance,
			FuturesJson:      r.FuturesJSON,
			SpotJson:         r.SpotJSON,
			SessionId:        r.SessionID,
			StrategyId:       r.StrategyID,
		})
	}
	return &accountv1.ListSessionSnapshotsResponse{
		Items:      out,
		NextOffset: int32(offset + len(out)),
		HasMore:    hasMore,
		Total:      total,
	}, nil
}

func (s *AccountGRPCService) ListReconciliationRuns(ctx context.Context, req *accountv1.ListReconciliationRunsRequest) (*accountv1.ListReconciliationRunsResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	limit := clampAuditListLimit(req.GetLimit())
	offset := clampAuditListOffset(req.GetOffset())

	runs, total, hasMore, err := s.repo.ListReconciliationRuns(ctx, req.GetSessionId(), req.GetUserId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list reconciliation runs: %v", err)
	}

	out := make([]*accountv1.ReconciliationRunEntry, 0, len(runs))
	for _, run := range runs {
		localJSON, err := json.Marshal(run.LocalSnapshot)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal local snapshot for run %s: %v", run.RunID, err)
		}
		exchangeJSON, err := json.Marshal(run.ExchangeSnapshot)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal exchange snapshot for run %s: %v", run.RunID, err)
		}

		entry := &accountv1.ReconciliationRunEntry{
			Time:                 timestamppb.New(run.Time),
			RunId:                run.RunID,
			AccountId:            run.AccountID,
			StrategyId:           run.StrategyID,
			SessionId:            run.SessionID,
			SnapshotReason:       int32(run.SnapshotReason),
			RunType:              string(run.RunType),
			Mode:                 int32(run.Mode),
			HardPass:             run.HardPass,
			SoftPass:             run.SoftPass,
			LocalSnapshotJson:    string(localJSON),
			ExchangeSnapshotJson: string(exchangeJSON),
		}
		for _, diff := range run.FieldDiffs {
			entry.FieldDiffs = append(entry.FieldDiffs, toProtoFieldDiff(diff))
		}
		for _, diff := range run.AdvisoryDiffs {
			entry.AdvisoryDiffs = append(entry.AdvisoryDiffs, toProtoFieldDiff(diff))
		}
		out = append(out, entry)
	}

	return &accountv1.ListReconciliationRunsResponse{
		Items:      out,
		NextOffset: int32(offset + len(out)),
		HasMore:    hasMore,
		Total:      total,
	}, nil
}

func (s *AccountGRPCService) GetSessionReconciliationSummary(ctx context.Context, req *accountv1.GetSessionReconciliationSummaryRequest) (*accountv1.GetSessionReconciliationSummaryResponse, error) {
	if err := requireUserID(req.GetUserId()); err != nil {
		return nil, err
	}
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	totalRuns, hardFailRuns, softFailRuns, err := s.repo.GetSessionReconciliationSummary(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "session reconciliation summary: %v", err)
	}
	return &accountv1.GetSessionReconciliationSummaryResponse{
		TotalRuns:    totalRuns,
		HardFailRuns: hardFailRuns,
		SoftFailRuns: softFailRuns,
	}, nil
}

func toProtoFieldDiff(diff domain.FieldDiff) *accountv1.FieldDiffEntry {
	thresholdJSON := ""
	if diff.Threshold != nil {
		if b, err := json.Marshal(diff.Threshold); err == nil {
			thresholdJSON = string(b)
		}
	}
	return &accountv1.FieldDiffEntry{
		Field:         diff.Field,
		Severity:      string(diff.Severity),
		Exchange:      diff.Exchange,
		Local:         diff.Local,
		DiffAbs:       diff.DiffAbs,
		DiffRatio:     diff.DiffRatio,
		ThresholdJson: thresholdJSON,
		Passed:        diff.Passed,
	}
}

func toProtoSession(s domain.StrategySession) *accountv1.StrategySessionEntry {
	e := &accountv1.StrategySessionEntry{
		SessionId:      s.SessionID,
		AccountId:      s.AccountID,
		StrategyId:     s.StrategyID,
		Mode:           int32(s.Mode),
		Status:         s.Status,
		Interval:       s.Interval,
		BarsProcessed:  int32(s.BarsProcessed),
		Error:          s.Error,
		StartedAt:      timestamppb.New(s.StartedAt),
		CreatedAt:      timestamppb.New(s.CreatedAt),
		UserId:         s.UserID,
		RuntimeId:      s.RuntimeID,
		RuntimeSource:  s.RuntimeSource,
		RuntimeName:    s.RuntimeName,
		SessionType:    normalizeServiceSessionType(s.Mode, s.SessionType),
		RuntimeVersion: normalizeServiceRuntimeVersion(s.RuntimeVersion),
		SessionName:    s.SessionName,
	}
	if s.StartTimeMs != nil {
		e.StartTimeMs = *s.StartTimeMs
	}
	if s.EndTimeMs != nil {
		e.EndTimeMs = *s.EndTimeMs
	}
	if s.CompletedAt != nil {
		e.CompletedAt = timestamppb.New(*s.CompletedAt)
	}
	return e
}

func normalizeServiceRuntimeVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return defaultRuntimeVersion
	}
	return version
}

func normalizeServiceRuntimeProfile(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return defaultRuntimeProfile
	}
	return profile
}

func normalizeServiceSessionType(mode int, sessionType string) string {
	sessionType = strings.TrimSpace(sessionType)
	if sessionType != "" {
		return sessionType
	}
	if mode == int(domain.AccountModeBinanceTestnet) {
		return sessionTypeTestnet
	}
	return sessionTypeBacktest
}

func mapRepoErr(err error) error {
	if errors.Is(err, repository.ErrNotFound) {
		return status.Error(codes.NotFound, "resource not found")
	}
	if errors.Is(err, repository.ErrConflict) {
		return status.Error(codes.FailedPrecondition, "resource conflict")
	}
	return status.Errorf(codes.Unavailable, "repository error: %v", err)
}

func mapStrategyErr(err error) error {
	if errors.Is(err, repository.ErrNotFound) {
		return status.Error(codes.NotFound, "strategy not found")
	}
	return status.Errorf(codes.Internal, "repository error: %v", err)
}
