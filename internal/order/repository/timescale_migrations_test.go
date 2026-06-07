package repository

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestResolveMigrationsDirUsesOrderModulePath(t *testing.T) {
	t.Setenv("ORDER_MIGRATIONS", "")

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()
	if err := os.Chdir("../../.."); err != nil {
		t.Fatalf("chdir core-service root: %v", err)
	}

	got, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	wantSuffix := filepath.Join("internal", "order", "storage", "migrations")
	if !strings.HasSuffix(filepath.Clean(got), wantSuffix) {
		t.Fatalf("migrations dir = %q, want suffix %q", got, wantSuffix)
	}
}

func TestResolveMigrationsDirSupportsOrderOverride(t *testing.T) {
	override := filepath.Join(t.TempDir(), "migrations")
	if err := os.MkdirAll(override, 0o755); err != nil {
		t.Fatalf("mkdir override: %v", err)
	}
	t.Setenv("ORDER_MIGRATIONS", override)

	got, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	if got != override {
		t.Fatalf("migrations dir = %q, want override %q", got, override)
	}
}

func TestOrderMigrationsIncludeRiskRecoveryContractColumns(t *testing.T) {
	t.Setenv("ORDER_MIGRATIONS", "")

	migrationsDir, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var combined strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		combined.Write(content)
		combined.WriteByte('\n')
	}

	requiredColumns := []string{
		"order_intents.post_only",
		"order_intents.good_till_date",
		"order_intents.reduce_only",
		"order_attempts.post_only",
		"order_attempts.good_till_date",
		"order_attempts.reduce_only",
		"order_attempts.risk_status",
		"order_attempts.risk_reasons_json",
		"orders.post_only",
		"orders.good_till_date",
		"orders.reduce_only",
		"orders.recovery_status",
		"orders.recovery_started_at",
		"orders.next_check_at",
		"orders.recovery_deadline_at",
		"orders.last_recovery_error",
		"orders.force_closed_at",
	}
	for _, required := range requiredColumns {
		table, column, ok := strings.Cut(required, ".")
		if !ok {
			t.Fatalf("invalid required column reference %q", required)
		}
		tablePattern := regexp.QuoteMeta(table)
		columnPattern := regexp.QuoteMeta(column)
		statementPattern := `(?is)(CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+` + tablePattern + `\s*\(|ALTER\s+TABLE\s+` + tablePattern + `\b)[^;]*\b` + columnPattern + `\b`
		if !regexp.MustCompile(statementPattern).MatchString(combined.String()) {
			t.Fatalf("migrations missing column reference %s", required)
		}
	}
}

func TestOrderMigrationsAllowRiskRejectedAttemptStatus(t *testing.T) {
	t.Setenv("ORDER_MIGRATIONS", "")

	migrationsDir, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var combined strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		combined.Write(content)
		combined.WriteByte('\n')
	}
	pattern := regexp.MustCompile(`(?is)ALTER\s+TABLE\s+order_attempts[^;]*ADD\s+CONSTRAINT\s+chk_order_attempts_status[^;]*status\s+IN\s*\([^)]*\b8\b[^)]*\)`)
	if !pattern.MatchString(combined.String()) {
		t.Fatal("migrations must update chk_order_attempts_status to allow RISK_REJECTED=8")
	}
}
