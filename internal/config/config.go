package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server        ServerConfig       `yaml:"server"`
	Database      DatabaseConfig     `yaml:"database"`
	OrderDatabase DatabaseConfig     `yaml:"order_database"`
	Exchange      ExchangeConfig     `yaml:"exchange"`
	Credential    CredentialConfig   `yaml:"credential"`
	Notification  NotificationConfig `yaml:"notification"`
	Log           elog.Config        `yaml:"log"`
}

type ServerConfig struct {
	HTTPAddr string `yaml:"http_addr"`
	GRPCAddr string `yaml:"grpc_addr"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
}

type NotificationConfig struct {
	Enabled  bool                       `yaml:"enabled"`
	Kafka    NotificationKafkaConfig    `yaml:"kafka"`
	Telegram NotificationTelegramConfig `yaml:"telegram"`
	Delivery NotificationDeliveryConfig `yaml:"delivery"`
}

type NotificationKafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`
}

type NotificationTelegramConfig struct {
	Enabled             bool   `yaml:"enabled"`
	BotToken            string `yaml:"bot_token"`
	BotUsername         string `yaml:"bot_username"`
	BindCodeTTLSeconds  int    `yaml:"bind_code_ttl_seconds"`
	PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
}

type NotificationDeliveryConfig struct {
	SendTimeoutSeconds int `yaml:"send_timeout_seconds"`
}

type CredentialConfig struct {
	EncryptionKey string `yaml:"encryption_key"`
	KeyVersion    string `yaml:"key_version"`
}

// ExchangeConfig only controls process-wide exchange wiring.
// Per-account API credentials live in the accounts table and are read at
// request time; there is no global/fallback key anymore (Phase A decision).
type ExchangeConfig struct {
	MockBinance    bool                 `yaml:"mock_binance"`
	SymbolCacheTTL string               `yaml:"symbol_cache_ttl"`
	Reconciliation ReconciliationConfig `yaml:"reconciliation"`
}

// ReconciliationConfig drives Phase C shadow-compare behavior.
// When Enabled is true, core-service runs an async compare in a detached
// goroutine after every demo/live UpdateAccountWalletState; it writes to the
// reconciliation_runs table and emits metric log events through the existing
// ELK pipeline. Main request path is unaffected.
type ReconciliationConfig struct {
	Enabled bool `yaml:"enabled"`
	// Compare goroutine independent timeout. Must be > 0 when enabled.
	GoroutineTimeoutSeconds int `yaml:"goroutine_timeout_seconds"`
	// "all" means every OrderFill triggers a compare (no sampling).
	// Reserved for future sampling modes; keep at "all" for Phase C.
	OrderFillRunMode string `yaml:"order_fill_run_mode"`
	// PeriodicSample hybrid trigger thresholds (either fires first):
	// strategy-service session fires PeriodicSample when bars_since >= this
	// OR wall-clock idle >= PeriodicSampleMaxIdleSeconds.
	// These values are the source of truth; strategy-service reads them via
	// RunStrategy request parameters at session start.
	PeriodicSampleEveryBars      int `yaml:"periodic_sample_every_bars"`
	PeriodicSampleMaxIdleSeconds int `yaml:"periodic_sample_max_idle_seconds"`
	// Compare thresholds per field tier.
	Thresholds ReconciliationThresholds `yaml:"thresholds"`
}

// ReconciliationThresholds defines per-field compare tolerance.
// Hard fields use stepSize/tickSize multipliers; Soft fields use
// max(abs, ratio); Advisory fields are recorded but not gated.
type ReconciliationThresholds struct {
	// Hard tier
	PositionQtyStepTolerance float64 `yaml:"position_qty_step_tolerance"`
	EntryPriceTickTolerance  float64 `yaml:"entry_price_tick_tolerance"`
	EntryPriceRatioTolerance float64 `yaml:"entry_price_ratio_tolerance"`
	// Soft tier — ledger-driven (tight)
	WalletBalanceAbsToleranceUSDT float64 `yaml:"wallet_balance_abs_tolerance_usdt"`
	WalletBalanceRatioTolerance   float64 `yaml:"wallet_balance_ratio_tolerance"`
	// Soft tier — derived risk (looser)
	DerivedRiskAbsToleranceUSDT float64 `yaml:"derived_risk_abs_tolerance_usdt"`
	DerivedRiskRatioTolerance   float64 `yaml:"derived_risk_ratio_tolerance"`
	// Soft tier — liquidation (loosest among soft)
	LiquidationPriceAbsToleranceUSDT float64 `yaml:"liquidation_price_abs_tolerance_usdt"`
	LiquidationPriceRatioTolerance   float64 `yaml:"liquidation_price_ratio_tolerance"`
	// Advisory tier — observability only
	MarkPriceDriftTickWarn float64 `yaml:"mark_price_drift_tick_warn"`
}

// DefaultReconciliationConfig returns Phase C calibration-phase defaults.
func DefaultReconciliationConfig() ReconciliationConfig {
	return ReconciliationConfig{
		Enabled:                      false, // opt-in; turn on in config.yaml
		GoroutineTimeoutSeconds:      5,
		OrderFillRunMode:             "all",
		PeriodicSampleEveryBars:      20,
		PeriodicSampleMaxIdleSeconds: 300,
		Thresholds: ReconciliationThresholds{
			PositionQtyStepTolerance:         0.5,
			EntryPriceTickTolerance:          1.0,
			EntryPriceRatioTolerance:         0.0002,
			WalletBalanceAbsToleranceUSDT:    0.01,
			WalletBalanceRatioTolerance:      0.0002,
			DerivedRiskAbsToleranceUSDT:      0.05,
			DerivedRiskRatioTolerance:        0.002,
			LiquidationPriceAbsToleranceUSDT: 0.05,
			LiquidationPriceRatioTolerance:   0.005,
			MarkPriceDriftTickWarn:           3.0,
		},
	}
}

// GoroutineTimeout returns the timeout duration for async compare work.
// Defaults to 5s if the config value is non-positive.
func (r ReconciliationConfig) GoroutineTimeout() time.Duration {
	if r.GoroutineTimeoutSeconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.GoroutineTimeoutSeconds) * time.Second
}

// Default returns a sane baseline config so the service can still start
// in env-driven deployments when config.yaml is absent.
func Default() *Config {
	logCfg := elog.DefaultConfig()
	logCfg.OutputDir = "./logs"
	logCfg.Tracing.ServiceName = "core-service"
	if logCfg.Kafka.Topic == "" {
		logCfg.Kafka.Topic = "app-logs"
	}
	if logCfg.Kafka.TopicPrefix == "" {
		logCfg.Kafka.TopicPrefix = "app-logs"
	}
	return &Config{
		Server: ServerConfig{
			HTTPAddr: ":8080",
			GRPCAddr: ":50051",
		},
		Database: DatabaseConfig{
			Host:     "192.168.88.10",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			DBName:   "account",
			SSLMode:  "disable",
		},
		OrderDatabase: DatabaseConfig{
			Host:     "192.168.88.10",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			DBName:   "order",
			SSLMode:  "disable",
		},
		Exchange: ExchangeConfig{
			MockBinance:    false,
			SymbolCacheTTL: "6h",
			Reconciliation: DefaultReconciliationConfig(),
		},
		Credential: CredentialConfig{
			KeyVersion: "v1",
		},
		Notification: NotificationConfig{
			Enabled: false,
			Kafka: NotificationKafkaConfig{
				Brokers: []string{"192.168.88.10:19092"},
				Topic:   "notification.events",
				GroupID: "core-service-notification",
			},
			Telegram: NotificationTelegramConfig{
				Enabled:             false,
				BindCodeTTLSeconds:  600,
				PollIntervalSeconds: 2,
			},
			Delivery: NotificationDeliveryConfig{
				SendTimeoutSeconds: 5,
			},
		},
		Log: *logCfg,
	}
}

// DSN returns the Postgres connection string.
func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.DBName, d.SSLMode,
	)
}

// SymbolCacheDuration parses the TTL string; falls back to 6h on parse error.
func (e ExchangeConfig) SymbolCacheDuration() time.Duration {
	if e.SymbolCacheTTL == "" {
		return 6 * time.Hour
	}
	if d, err := time.ParseDuration(e.SymbolCacheTTL); err == nil {
		return d
	}
	return 6 * time.Hour
}

// Load reads a YAML file and parses it into a Config struct.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}

// ApplyEnvOverrides overrides config fields from environment variables.
// Legacy names (TIMESCALEDB_DSN, MOCK_BINANCE, SYMBOL_CACHE_TTL, HTTP_ADDR, GRPC_ADDR)
// are honored for backward compatibility.
func (c *Config) ApplyEnvOverrides() {
	// Server overrides
	if v := os.Getenv("SERVER_HTTP_ADDR"); v != "" {
		c.Server.HTTPAddr = v
	} else if v := os.Getenv("HTTP_ADDR"); v != "" {
		c.Server.HTTPAddr = v
	}
	if v := os.Getenv("SERVER_GRPC_ADDR"); v != "" {
		c.Server.GRPCAddr = v
	} else if v := os.Getenv("GRPC_ADDR"); v != "" {
		c.Server.GRPCAddr = v
	}

	// Database overrides via DSN (legacy) or individual fields
	if dsn := os.Getenv("TIMESCALEDB_DSN"); dsn != "" {
		c.Database.parseDSN(dsn)
	}
	if v := os.Getenv("DATABASE_HOST"); v != "" {
		c.Database.Host = v
	}
	if v := os.Getenv("DATABASE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Database.Port = n
		}
	}
	if v := os.Getenv("DATABASE_USER"); v != "" {
		c.Database.User = v
	}
	if v := os.Getenv("DATABASE_PASSWORD"); v != "" {
		c.Database.Password = v
	}
	if v := os.Getenv("DATABASE_DBNAME"); v != "" {
		c.Database.DBName = v
	}
	if v := os.Getenv("DATABASE_SSLMODE"); v != "" {
		c.Database.SSLMode = v
	}

	// Order database overrides are intentionally independent from TIMESCALEDB_DSN.
	if dsn := os.Getenv("ORDER_TIMESCALEDB_DSN"); dsn != "" {
		c.OrderDatabase.parseDSN(dsn)
	}
	if v := os.Getenv("ORDER_DATABASE_HOST"); v != "" {
		c.OrderDatabase.Host = v
	}
	if v := os.Getenv("ORDER_DATABASE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.OrderDatabase.Port = n
		}
	}
	if v := os.Getenv("ORDER_DATABASE_USER"); v != "" {
		c.OrderDatabase.User = v
	}
	if v := os.Getenv("ORDER_DATABASE_PASSWORD"); v != "" {
		c.OrderDatabase.Password = v
	}
	if v := os.Getenv("ORDER_DATABASE_DBNAME"); v != "" {
		c.OrderDatabase.DBName = v
	}
	if v := os.Getenv("ORDER_DATABASE_SSLMODE"); v != "" {
		c.OrderDatabase.SSLMode = v
	}

	// Exchange overrides
	if v := os.Getenv("MOCK_BINANCE"); v != "" {
		c.Exchange.MockBinance = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("SYMBOL_CACHE_TTL"); v != "" {
		c.Exchange.SymbolCacheTTL = v
	}
	// Binance credentials are intentionally NOT taken from env. Per-account
	// api_key / api_secret live on the accounts table.

	if v := os.Getenv("CORE_CREDENTIAL_ENCRYPTION_KEY"); v != "" {
		c.Credential.EncryptionKey = v
	}
	if v := os.Getenv("CORE_CREDENTIAL_KEY_VERSION"); v != "" {
		c.Credential.KeyVersion = v
	}

	if v := os.Getenv("NOTIFICATION_ENABLED"); v != "" {
		c.Notification.Enabled = parseBool(v)
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_BROKERS"); v != "" {
		c.Notification.Kafka.Brokers = splitCSV(v)
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_TOPIC"); v != "" {
		c.Notification.Kafka.Topic = v
	}
	if v := os.Getenv("NOTIFICATION_KAFKA_GROUP_ID"); v != "" {
		c.Notification.Kafka.GroupID = v
	}
	if v := os.Getenv("NOTIFICATION_TELEGRAM_ENABLED"); v != "" {
		c.Notification.Telegram.Enabled = parseBool(v)
	}
	if v := os.Getenv("TELEGRAM_BOT_TOKEN"); v != "" {
		c.Notification.Telegram.BotToken = v
	}
	if v := os.Getenv("TELEGRAM_BOT_USERNAME"); v != "" {
		c.Notification.Telegram.BotUsername = v
	}
	if v := os.Getenv("NOTIFICATION_BIND_CODE_TTL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Notification.Telegram.BindCodeTTLSeconds = n
		}
	}
	if v := os.Getenv("NOTIFICATION_SEND_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Notification.Delivery.SendTimeoutSeconds = n
		}
	}
}

func parseBool(v string) bool {
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseDSN parses a Postgres DSN string (key=value space-separated) into DatabaseConfig.
func (d *DatabaseConfig) parseDSN(dsn string) {
	for _, kv := range strings.Fields(dsn) {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]
		switch key {
		case "host":
			d.Host = val
		case "port":
			fmt.Sscanf(val, "%d", &d.Port)
		case "user":
			d.User = val
		case "password":
			d.Password = val
		case "dbname":
			d.DBName = val
		case "sslmode":
			d.SSLMode = val
		}
	}
}
