package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hushine-tech/core-service/internal/domain"
	exchangeadapter "github.com/hushine-tech/core-service/internal/exchange/adapter"
	"github.com/hushine-tech/core-service/internal/logger"
	"github.com/hushine-tech/core-service/internal/order/lifecycle"
)

type UserDataStreamStore interface {
	lifecycle.UserDataIngestStore
	ListOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error)
}

type UserDataStreamManagerConfig struct {
	BatchSize int
}

type UserDataStreamManager struct {
	registry   *exchangeadapter.Registry
	metaGetter RecoveryMetaGetter
	store      UserDataStreamStore
	ingestor   *lifecycle.UserDataIngestor
	router     *AdapterRouter
	cfg        UserDataStreamManagerConfig

	mu     sync.Mutex
	active map[userDataStreamKey]context.CancelFunc
}

type userDataStreamKey struct {
	accountID   int64
	venueID     int64
	exchange    int32
	environment int32
	market      int32
}

func NewUserDataStreamManager(registry *exchangeadapter.Registry, metaGetter RecoveryMetaGetter, store UserDataStreamStore, cfg UserDataStreamManagerConfig) *UserDataStreamManager {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	return &UserDataStreamManager{
		registry:   registry,
		metaGetter: metaGetter,
		store:      store,
		ingestor:   lifecycle.NewUserDataIngestor(store),
		router:     NewAdapterRouter(registry),
		cfg:        cfg,
		active:     make(map[userDataStreamKey]context.CancelFunc),
	}
}

func (m *UserDataStreamManager) SyncOnce(ctx context.Context) (int, error) {
	orders, err := m.store.ListOpenOrders(ctx, m.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	started := 0
	var lastErr error
	for _, order := range orders {
		key := userDataStreamKeyFromOrder(order)
		if !key.valid() || m.isActive(key) {
			continue
		}
		stream, req, err := m.resolveStream(ctx, order)
		if err != nil {
			if isUserDataStreamUnsupported(err) {
				continue
			}
			lastErr = err
			continue
		}
		streamCtx, cancel := context.WithCancel(ctx)
		if !m.markActive(key, cancel) {
			cancel()
			continue
		}
		started++
		go m.runStream(streamCtx, key, stream, req)
	}
	return started, lastErr
}

func (m *UserDataStreamManager) resolveStream(ctx context.Context, order lifecycle.OpenOrder) (exchangeadapter.UserDataStream, exchangeadapter.UserDataStreamRequest, error) {
	meta, err := m.metaGetter.Get(ctx, order.AccountID, order.Exchange, order.Market)
	if err != nil {
		return nil, exchangeadapter.UserDataStreamRequest{}, err
	}
	route := routeFromMeta(meta)
	credential, err := m.router.parseCredential(ctx, route, meta)
	if err != nil {
		return nil, exchangeadapter.UserDataStreamRequest{}, err
	}
	stream, err := m.registry.UserDataStream(route)
	if err != nil {
		return nil, exchangeadapter.UserDataStreamRequest{}, err
	}
	return stream, exchangeadapter.UserDataStreamRequest{
		AccountID:  order.AccountID,
		VenueID:    firstPositiveInt64(meta.VenueID, order.VenueID),
		Credential: credential,
	}, nil
}

func (m *UserDataStreamManager) runStream(ctx context.Context, key userDataStreamKey, stream exchangeadapter.UserDataStream, req exchangeadapter.UserDataStreamRequest) {
	defer m.clearActive(key)
	if err := stream.Listen(ctx, req, func(eventCtx context.Context, event exchangeadapter.UserDataOrderEvent) error {
		err := m.ingestor.Ingest(eventCtx, req.VenueID, event)
		if errors.Is(err, lifecycle.ErrOpenOrderNotFound) {
			return nil
		}
		return err
	}); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Warn(context.Background(), "system", fmt.Sprintf(
			"order user data stream stopped: account_id=%d venue_id=%d exchange=%d environment=%d market=%d err=%v",
			key.accountID,
			key.venueID,
			key.exchange,
			key.environment,
			key.market,
			err,
		))
	}
}

func (m *UserDataStreamManager) isActive(key userDataStreamKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.active[key]
	return ok
}

func (m *UserDataStreamManager) markActive(key userDataStreamKey, cancel context.CancelFunc) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.active[key]; ok {
		return false
	}
	m.active[key] = cancel
	return true
}

func (m *UserDataStreamManager) clearActive(key userDataStreamKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, key)
}

func userDataStreamKeyFromOrder(order lifecycle.OpenOrder) userDataStreamKey {
	return userDataStreamKey{
		accountID:   order.AccountID,
		venueID:     order.VenueID,
		exchange:    order.Exchange,
		environment: order.Environment,
		market:      order.Market,
	}
}

func (k userDataStreamKey) valid() bool {
	return k.accountID > 0 &&
		k.venueID > 0 &&
		k.exchange != 0 &&
		k.environment != int32(domain.EnvironmentBacktest) &&
		k.market != 0
}

func isUserDataStreamUnsupported(err error) bool {
	return errors.Is(err, exchangeadapter.ErrRouteUnsupported) ||
		errors.Is(err, exchangeadapter.ErrCapabilityUnsupported)
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
