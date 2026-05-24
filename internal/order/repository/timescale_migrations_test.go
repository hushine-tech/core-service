package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveMigrationsDirUsesOrderModulePath(t *testing.T) {
	t.Setenv("ORDER_MIGRATIONS", "")
	t.Setenv("ORDER_SERVICE_MIGRATIONS", "")

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
	t.Setenv("ORDER_SERVICE_MIGRATIONS", "")

	got, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	if got != override {
		t.Fatalf("migrations dir = %q, want override %q", got, override)
	}
}

func TestResolveMigrationsDirSupportsLegacyOrderServiceOverride(t *testing.T) {
	override := filepath.Join(t.TempDir(), "legacy-migrations")
	if err := os.MkdirAll(override, 0o755); err != nil {
		t.Fatalf("mkdir override: %v", err)
	}
	t.Setenv("ORDER_MIGRATIONS", "")
	t.Setenv("ORDER_SERVICE_MIGRATIONS", override)

	got, err := resolveMigrationsDir()
	if err != nil {
		t.Fatalf("resolveMigrationsDir: %v", err)
	}
	if got != override {
		t.Fatalf("migrations dir = %q, want legacy override %q", got, override)
	}
}
