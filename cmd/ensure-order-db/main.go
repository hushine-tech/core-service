// ensure-order-db creates the configured order database if missing and applies order migrations.
//
// Usage:
//
//	go run ./cmd/ensure-order-db -config config.yaml
//	ORDER_DATABASE_DBNAME=order_test go run ./cmd/ensure-order-db
package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/lib/pq"

	"github.com/hushine-tech/core-service/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure-order-db: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ensure-order-db: OK")
}

func run() error {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			cfg = config.Default()
		} else {
			return fmt.Errorf("load config: %w", err)
		}
	}
	cfg.ApplyEnvOverrides()

	orderDB := cfg.OrderDatabase
	if strings.TrimSpace(orderDB.DBName) == "" {
		return fmt.Errorf("order database dbname is required")
	}

	adminDB := orderDB
	adminDB.DBName = "postgres"
	admin, err := sql.Open("postgres", adminDB.DSN())
	if err != nil {
		return fmt.Errorf("open admin: %w", err)
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	if err := ensureDatabase(admin, orderDB.DBName); err != nil {
		return err
	}

	db, err := sql.Open("postgres", orderDB.DSN())
	if err != nil {
		return fmt.Errorf("open order db: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping order db %s: %w", orderDB.DBName, err)
	}

	migDir, err := orderMigrationsDir()
	if err != nil {
		return err
	}
	if err := applyMigrations(db, migDir); err != nil {
		return err
	}
	return nil
}

func ensureDatabase(admin *sql.DB, dbName string) error {
	var exists bool
	if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists); err != nil {
		return fmt.Errorf("check database %s: %w", dbName, err)
	}
	if exists {
		fmt.Println("database already exists:", dbName)
		return nil
	}
	if _, err := admin.Exec(`CREATE DATABASE ` + quoteIdentifier(dbName)); err != nil {
		return fmt.Errorf("CREATE DATABASE %s: %w", dbName, err)
	}
	fmt.Println("created database:", dbName)
	return nil
}

func applyMigrations(db *sql.DB, migDir string) error {
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations %s: %w", migDir, err)
	}
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(migDir, e.Name()))
	}
	sort.Strings(files)

	for _, f := range files {
		base := filepath.Base(f)
		applied, err := migrationApplied(db, base)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", base, err)
		}
		if applied {
			fmt.Println("skipped:", base)
			continue
		}

		body, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		sqlText := strings.TrimSpace(string(body))
		if sqlText == "" {
			continue
		}
		if _, err := db.Exec(sqlText); err != nil {
			return fmt.Errorf("exec %s: %w", base, err)
		}
		if _, err := db.Exec(
			`INSERT INTO schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`,
			base,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", base, err)
		}
		fmt.Println("applied:", base)
	}
	return nil
}

func migrationApplied(db *sql.DB, filename string) (bool, error) {
	var tableExists bool
	if err := db.QueryRow(`SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&tableExists); err != nil {
		return false, err
	}
	if !tableExists {
		return false, nil
	}

	var applied bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, filename).Scan(&applied); err != nil {
		return false, err
	}
	return applied, nil
}

func orderMigrationsDir() (string, error) {
	root, err := findModuleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "internal", "order", "storage", "migrations"), nil
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd")
		}
		dir = parent
	}
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
