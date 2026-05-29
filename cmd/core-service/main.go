package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcmw "github.com/hushine-tech/golang-lib/middleware/grpc"
	httpmw "github.com/hushine-tech/golang-lib/middleware/httpserver"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"google.golang.org/grpc"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/gen/orderv1"
	"github.com/hushine-tech/core-service/internal/catalog"
	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/credential"
	"github.com/hushine-tech/core-service/internal/exchange"
	"github.com/hushine-tech/core-service/internal/httpserver"
	"github.com/hushine-tech/core-service/internal/logger"
	"github.com/hushine-tech/core-service/internal/notification"
	orderaccountmeta "github.com/hushine-tech/core-service/internal/order/accountmeta"
	orderexecutor "github.com/hushine-tech/core-service/internal/order/executor"
	ordernotify "github.com/hushine-tech/core-service/internal/order/notification"
	orderrepository "github.com/hushine-tech/core-service/internal/order/repository"
	ordersvc "github.com/hushine-tech/core-service/internal/order/service"
	"github.com/hushine-tech/core-service/internal/reconciliation"
	"github.com/hushine-tech/core-service/internal/repository"
	"github.com/hushine-tech/core-service/internal/service"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("config file %q not found; using built-in defaults + env overrides", *configPath)
			cfg = config.Default()
		} else {
			log.Fatalf("load config: %v", err)
		}
	}
	cfg.ApplyEnvOverrides()

	// ── Logger ────────────────────────────────────────────────────────────────
	if err := logger.InitWithConfig(&cfg.Log); err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Close()

	if cfg.Log.Tracing.Enabled {
		if tracerShutdown, err := elog.InitTracerFromConfig(cfg.Log.Tracing); err != nil {
			log.Printf("init tracer: %v (continuing without tracing)", err)
		} else {
			defer tracerShutdown(context.Background())
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info(ctx, "system", "core-service starting")

	// ── TimescaleDB ───────────────────────────────────────────────────────────
	repo, err := repository.NewTimescaleRepository(cfg.Database.DSN(), logger.Instance())
	if err != nil {
		log.Fatalf("init timescaledb: %v", err)
	}
	logger.Info(ctx, "system", "timescaledb connected")

	orderRepository, err := orderrepository.NewTimescaleRepository(cfg.OrderDatabase.DSN(), logger.Instance())
	if err != nil {
		log.Fatalf("init order timescaledb: %v", err)
	}
	defer func() {
		if err := orderRepository.Close(); err != nil {
			logger.Warn(context.Background(), "system", fmt.Sprintf("close order repository: %v", err))
		}
	}()
	logger.Info(ctx, "system", "order timescaledb connected")

	// ── Exchange Adapters ─────────────────────────────────────────────────────
	//
	// Adapters are environment-scoped (one per provider+environment). They do
	// NOT hold credentials; per-account api_key/api_secret is read from
	// accounts.api_key at request time (Phase A decision).
	fetchers := map[exchange.ExchangeTarget]exchange.OnlineInfoFetcher{}
	if cfg.Exchange.MockBinance {
		mock := exchange.NewIntegrationMockFetcher()
		fetchers[exchange.ExchangeTarget{Provider: exchange.ProviderBinance, Environment: exchange.EnvLive}] = mock
		fetchers[exchange.ExchangeTarget{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}] = mock
		logger.Info(ctx, "system", "mock_binance=true: using mock exchange fetcher (no real Binance)")
	} else {
		fetchers[exchange.ExchangeTarget{Provider: exchange.ProviderBinance, Environment: exchange.EnvLive}] = exchange.NewBinanceLiveAdapter(logger.Instance())
		fetchers[exchange.ExchangeTarget{Provider: exchange.ProviderBinance, Environment: exchange.EnvTestnet}] = exchange.NewBinanceTestnetAdapter(logger.Instance())
		logger.Info(ctx, "system", "binance live+testnet adapters initialized (per-account credentials from DB)")
	}

	router := exchange.NewAdapterRouter(fetchers, repo.GetAccountState)
	symbolCatalog := catalog.New(cfg.Exchange.SymbolCacheDuration(), logger.Instance())

	var credentialManager *credential.Manager
	if cfg.Credential.EncryptionKey != "" {
		credentialManager, err = credential.NewManager(cfg.Credential.EncryptionKey, cfg.Credential.KeyVersion)
		if err != nil {
			log.Fatalf("init credential manager: %v", err)
		}
	}

	// ── Phase C reconciliation (async shadow compare) ────────────────────────
	// LaunchAsync no-ops when cfg.Exchange.Reconciliation.Enabled is false,
	// so this is safe to wire unconditionally.
	reconciler := reconciliation.NewService(cfg.Exchange.Reconciliation, repo)
	if cfg.Exchange.Reconciliation.Enabled {
		logger.Info(ctx, "system", "reconciliation enabled: async shadow compare will run on mode=1/2 UpdateAccountWalletState")
	}

	// ── Notification management ──────────────────────────────────────────────
	var telegramClient *notification.TelegramClient
	var telegramSender notification.TelegramSender
	if cfg.Notification.Telegram.Enabled && cfg.Notification.Telegram.BotToken != "" {
		telegramClient = notification.NewTelegramClient(
			cfg.Notification.Telegram.BotToken,
			time.Duration(cfg.Notification.Delivery.SendTimeoutSeconds)*time.Second,
		)
		telegramSender = telegramClient
	}
	notificationSvc := notification.NewService(repo, telegramSender, notification.Config{
		BotUsername:      cfg.Notification.Telegram.BotUsername,
		BindCodeTTL:      time.Duration(cfg.Notification.Telegram.BindCodeTTLSeconds) * time.Second,
		SendTimeout:      time.Duration(cfg.Notification.Delivery.SendTimeoutSeconds) * time.Second,
		CustomRateWindow: time.Minute,
	}, time.Now)
	if cfg.Notification.Enabled {
		go func() {
			if err := notification.RunKafkaConsumer(ctx, cfg.Notification.Kafka.Brokers, cfg.Notification.Kafka.GroupID, cfg.Notification.Kafka.Topic, notificationSvc); err != nil {
				logger.Error(context.Background(), "system", fmt.Sprintf("notification kafka consumer stopped: %v", err))
			}
		}()
		logger.Info(ctx, "system", fmt.Sprintf("notification kafka consumer enabled: topic=%s brokers=%v", cfg.Notification.Kafka.Topic, cfg.Notification.Kafka.Brokers))
	}
	if cfg.Notification.Telegram.Enabled && telegramClient != nil {
		go func() {
			interval := time.Duration(cfg.Notification.Telegram.PollIntervalSeconds) * time.Second
			if err := notification.RunTelegramPolling(ctx, notificationSvc, telegramClient, interval); err != nil {
				logger.Error(context.Background(), "system", fmt.Sprintf("telegram notification poller stopped: %v", err))
			}
		}()
		logger.Info(ctx, "system", "telegram notification polling enabled")
	}

	// ── Order Service Wiring ──────────────────────────────────────────────────
	mockExecutor := orderexecutor.NewMockExecutor()
	liveExecutor := orderexecutor.NewBinanceLiveExecutor(logger.Instance())
	testnetExecutor := orderexecutor.NewBinanceTestnetExecutor(logger.Instance())
	orderRouter := orderexecutor.NewRouter(mockExecutor, liveExecutor, testnetExecutor)

	var orderPublisher ordernotify.Publisher = ordernotify.NoopPublisher{}
	var orderPublisherCloser interface{ Close() error }
	if cfg.Notification.Enabled {
		publisher, err := ordernotify.NewKafkaPublisher(cfg.Notification.Kafka.Brokers, cfg.Notification.Kafka.Topic, "core-service-order-notification")
		if err != nil {
			logger.Warn(ctx, "system", fmt.Sprintf("order notification kafka publisher disabled: %v", err))
		} else {
			orderPublisher = publisher
			orderPublisherCloser = publisher
			logger.Info(ctx, "system", fmt.Sprintf("order notification kafka publisher enabled: topic=%s brokers=%v", cfg.Notification.Kafka.Topic, cfg.Notification.Kafka.Brokers))
		}
	}
	defer func() {
		if orderPublisherCloser != nil {
			if err := orderPublisherCloser.Close(); err != nil {
				logger.Warn(context.Background(), "system", fmt.Sprintf("close order notification publisher: %v", err))
			}
		}
	}()

	orderMetaGetter := orderaccountmeta.NewAdapter(repo, credentialManager)
	orderService := ordersvc.NewOrderGRPCService(orderMetaGetter, orderRouter, orderRepository, orderPublisher)

	// ── HTTP Server ───────────────────────────────────────────────────────────
	httpAddr := cfg.Server.HTTPAddr
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	httpHandler := httpmw.Middleware(logger.Instance())(httpserver.NewMux(repo))
	httpSrv := &http.Server{
		Addr:         httpAddr,
		Handler:      httpHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go func() {
		logger.Info(ctx, "system", fmt.Sprintf("http server listening on %s", httpAddr))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	// ── gRPC Server ───────────────────────────────────────────────────────────
	grpcAddr := cfg.Server.GRPCAddr
	if grpcAddr == "" {
		grpcAddr = ":50051"
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcmw.UnaryServerInterceptor(logger.Instance())),
	)
	accountv1.RegisterAccountServiceServer(grpcSrv, service.NewAccountGRPCService(repo, router, symbolCatalog, reconciler, notificationSvc, service.WithCredentialManager(credentialManager)))
	orderv1.RegisterOrderServiceServer(grpcSrv, orderService)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen grpc: %v", err)
	}
	go func() {
		logger.Info(ctx, "system", fmt.Sprintf("grpc server listening on %s", grpcAddr))
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc server error: %v", err)
		}
	}()

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()
	logger.Info(context.Background(), "system", "shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = httpSrv.Shutdown(shutdownCtx)
	grpcStopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		logger.Warn(context.Background(), "system", "grpc graceful shutdown timed out; forcing stop")
		grpcSrv.Stop()
		<-grpcStopped
	}

	logger.Info(context.Background(), "system", "core-service stopped")
}
