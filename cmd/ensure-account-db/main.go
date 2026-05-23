// ensure-account-db connects to PostgreSQL, creates database "account" if missing, and applies SQL migrations.
//
// Usage:
//
//	go run ./cmd/ensure-account-db
//	PGHOST=192.168.88.10 PGUSER=postgres PGPASSWORD=postgres go run ./cmd/ensure-account-db
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure-account-db: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ensure-account-db: OK (database account + migrations)")
}

func run() error {
	host := getenv("PGHOST", "192.168.88.10")
	port := getenv("PGPORT", "5432")
	user := getenv("PGUSER", "postgres")
	pass := getenv("PGPASSWORD", "postgres")
	dbnameAdmin := getenv("PGDATABASE_ADMIN", "postgres")

	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbnameAdmin)

	if err := func() error {
		admin, err := sql.Open("postgres", adminDSN)
		if err != nil {
			return fmt.Errorf("open admin: %w", err)
		}
		defer admin.Close()
		if err := admin.Ping(); err != nil {
			return fmt.Errorf("ping postgres: %w", err)
		}

		var exists bool
		if err := admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'account')`).Scan(&exists); err != nil {
			return fmt.Errorf("check database: %w", err)
		}
		if !exists {
			if _, err := admin.Exec(`CREATE DATABASE account`); err != nil {
				return fmt.Errorf("CREATE DATABASE account: %w", err)
			}
			fmt.Println("created database: account")
		} else {
			fmt.Println("database account already exists")
		}
		return nil
	}(); err != nil {
		return err
	}

	accDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=account sslmode=disable",
		host, port, user, pass)
	acc, err := sql.Open("postgres", accDSN)
	if err != nil {
		return fmt.Errorf("open account db: %w", err)
	}
	defer acc.Close()
	if err := acc.Ping(); err != nil {
		return fmt.Errorf("ping account: %w", err)
	}

	root, err := findModuleRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(root, "internal", "storage", "migrations")
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(migDir, e.Name()))
	}
	sort.Strings(files)

	for _, f := range files {
		base := filepath.Base(f)
		applied, err := migrationApplied(acc, base)
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
		if _, err := acc.Exec(sqlText); err != nil {
			return fmt.Errorf("exec %s: %w", base, err)
		}
		if _, err := acc.Exec(
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

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
