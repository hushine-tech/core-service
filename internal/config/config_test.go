package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNotificationDefaultsAndEnvOverrides(t *testing.T) {
	cfg := Default()
	if cfg.Notification.Kafka.Topic != "notification.events" {
		t.Fatalf("default notification topic = %q, want notification.events", cfg.Notification.Kafka.Topic)
	}
	t.Setenv("NOTIFICATION_ENABLED", "true")
	t.Setenv("NOTIFICATION_KAFKA_BROKERS", "kafka-a:9092,kafka-b:9092")
	t.Setenv("TELEGRAM_BOT_TOKEN", "token-123")
	t.Setenv("TELEGRAM_BOT_USERNAME", "hushine_bot")
	t.Setenv("NOTIFICATION_BIND_CODE_TTL_SECONDS", "120")
	cfg.ApplyEnvOverrides()

	if !cfg.Notification.Enabled {
		t.Fatalf("notification enabled = false, want true")
	}
	if got := cfg.Notification.Kafka.Brokers; len(got) != 2 || got[0] != "kafka-a:9092" || got[1] != "kafka-b:9092" {
		t.Fatalf("brokers = %#v, want two env brokers", got)
	}
	if cfg.Notification.Telegram.BotToken != "token-123" || cfg.Notification.Telegram.BotUsername != "hushine_bot" {
		t.Fatalf("telegram config = %+v, want env token and username", cfg.Notification.Telegram)
	}
	if cfg.Notification.Telegram.BindCodeTTLSeconds != 120 {
		t.Fatalf("bind ttl = %d, want 120", cfg.Notification.Telegram.BindCodeTTLSeconds)
	}
}

func TestOrderDatabaseDefault(t *testing.T) {
	cfg := Default()
	if cfg.OrderDatabase.Host != "192.168.88.10" {
		t.Fatalf("default order host = %q, want 192.168.88.10", cfg.OrderDatabase.Host)
	}
	if cfg.OrderDatabase.Port != 5432 {
		t.Fatalf("default order port = %d, want 5432", cfg.OrderDatabase.Port)
	}
	if cfg.OrderDatabase.User != "postgres" {
		t.Fatalf("default order user = %q, want postgres", cfg.OrderDatabase.User)
	}
	if cfg.OrderDatabase.Password != "postgres" {
		t.Fatalf("default order password = %q, want postgres", cfg.OrderDatabase.Password)
	}
	if cfg.OrderDatabase.DBName != "order" {
		t.Fatalf("default order dbname = %q, want order", cfg.OrderDatabase.DBName)
	}
	if cfg.OrderDatabase.SSLMode != "disable" {
		t.Fatalf("default order sslmode = %q, want disable", cfg.OrderDatabase.SSLMode)
	}
}

func TestOrderTimescaleDBDSNOnlyOverridesOrderDatabase(t *testing.T) {
	cfg := Default()
	t.Setenv("ORDER_TIMESCALEDB_DSN", "host=order-db port=15432 user=order-user password=secret dbname=orders_test sslmode=require")
	cfg.ApplyEnvOverrides()

	if cfg.Database.DBName != "account" {
		t.Fatalf("account dbname = %q, want account", cfg.Database.DBName)
	}
	if cfg.OrderDatabase.Host != "order-db" {
		t.Fatalf("order host = %q, want order-db", cfg.OrderDatabase.Host)
	}
	if cfg.OrderDatabase.Port != 15432 {
		t.Fatalf("order port = %d, want 15432", cfg.OrderDatabase.Port)
	}
	if cfg.OrderDatabase.User != "order-user" {
		t.Fatalf("order user = %q, want order-user", cfg.OrderDatabase.User)
	}
	if cfg.OrderDatabase.Password != "secret" {
		t.Fatalf("order password = %q, want secret", cfg.OrderDatabase.Password)
	}
	if cfg.OrderDatabase.DBName != "orders_test" {
		t.Fatalf("order dbname = %q, want orders_test", cfg.OrderDatabase.DBName)
	}
	if cfg.OrderDatabase.SSLMode != "require" {
		t.Fatalf("order sslmode = %q, want require", cfg.OrderDatabase.SSLMode)
	}
}

func TestTimescaleDBDSNOnlyOverridesAccountDatabase(t *testing.T) {
	cfg := Default()
	t.Setenv("TIMESCALEDB_DSN", "host=account-db port=25432 user=account-user password=secret dbname=account_test sslmode=require")
	cfg.ApplyEnvOverrides()

	if cfg.Database.DBName != "account_test" {
		t.Fatalf("account dbname = %q, want account_test", cfg.Database.DBName)
	}
	if cfg.OrderDatabase.DBName != "order" {
		t.Fatalf("order dbname = %q, want order", cfg.OrderDatabase.DBName)
	}
}

func TestLoadParsesIndependentAccountAndOrderDatabases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`
database:
  host: "account-host"
  port: 15432
  user: "account-user"
  password: "account-pass"
  dbname: "account_db"
  sslmode: "disable"
order_database:
  host: "order-host"
  port: 25432
  user: "order-user"
  password: "order-pass"
  dbname: "order_db"
  sslmode: "require"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Database.Host != "account-host" || cfg.Database.Port != 15432 || cfg.Database.User != "account-user" ||
		cfg.Database.Password != "account-pass" || cfg.Database.DBName != "account_db" || cfg.Database.SSLMode != "disable" {
		t.Fatalf("account database = %+v, want YAML account database", cfg.Database)
	}
	if cfg.OrderDatabase.Host != "order-host" || cfg.OrderDatabase.Port != 25432 || cfg.OrderDatabase.User != "order-user" ||
		cfg.OrderDatabase.Password != "order-pass" || cfg.OrderDatabase.DBName != "order_db" || cfg.OrderDatabase.SSLMode != "require" {
		t.Fatalf("order database = %+v, want YAML order database", cfg.OrderDatabase)
	}
}

func TestOrderDatabaseDBNameEnvOverride(t *testing.T) {
	cfg := Default()
	t.Setenv("ORDER_DATABASE_HOST", "order-host")
	t.Setenv("ORDER_DATABASE_PORT", "6543")
	t.Setenv("ORDER_DATABASE_USER", "order-user")
	t.Setenv("ORDER_DATABASE_PASSWORD", "order-pass")
	t.Setenv("ORDER_DATABASE_DBNAME", "order_shadow")
	t.Setenv("ORDER_DATABASE_SSLMODE", "verify-full")
	cfg.ApplyEnvOverrides()

	if cfg.OrderDatabase.Host != "order-host" {
		t.Fatalf("order host = %q, want order-host", cfg.OrderDatabase.Host)
	}
	if cfg.OrderDatabase.Port != 6543 {
		t.Fatalf("order port = %d, want 6543", cfg.OrderDatabase.Port)
	}
	if cfg.OrderDatabase.User != "order-user" {
		t.Fatalf("order user = %q, want order-user", cfg.OrderDatabase.User)
	}
	if cfg.OrderDatabase.Password != "order-pass" {
		t.Fatalf("order password = %q, want order-pass", cfg.OrderDatabase.Password)
	}
	if cfg.OrderDatabase.DBName != "order_shadow" {
		t.Fatalf("order dbname = %q, want order_shadow", cfg.OrderDatabase.DBName)
	}
	if cfg.OrderDatabase.SSLMode != "verify-full" {
		t.Fatalf("order sslmode = %q, want verify-full", cfg.OrderDatabase.SSLMode)
	}
	if cfg.Database.DBName != "account" {
		t.Fatalf("account dbname = %q, want account", cfg.Database.DBName)
	}
}
