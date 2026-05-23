package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/hushine-tech/golang-lib/middleware/sqlmiddleware"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"github.com/hushine-tech/core-service/internal/domain"
)

// TimescaleRepository implements Repository backed by TimescaleDB (PostgreSQL).
type TimescaleRepository struct {
	db      *sql.DB
	sqlExec *sqlmiddleware.Middleware
}

// NewTimescaleRepository opens a connection to TimescaleDB, runs migrations, and returns the repo.
func NewTimescaleRepository(dsn string, logger elog.Logger) (*TimescaleRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open timescaledb: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping timescaledb: %w", err)
	}

	repo := &TimescaleRepository{
		db:      db,
		sqlExec: sqlmiddleware.New(db, logger),
	}
	if err := repo.runMigrations(); err != nil {
		return nil, fmt.Errorf("migrations: %w", err)
	}
	return repo, nil
}

func (r *TimescaleRepository) runMigrations() error {
	migrationsDir, err := resolveMigrationsDir()
	if err != nil {
		return err
	}
	if _, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	applied, err := r.appliedMigrationSet()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migrationsDir, err)
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(migrationsDir, e.Name()))
		}
	}
	sort.Strings(files)

	for _, f := range files {
		name := filepath.Base(f)
		if applied[name] {
			continue
		}
		content, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}
		tx, err := r.db.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", f, err)
		}
		if _, err := tx.ExecContext(context.Background(), string(content)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec migration %s: %w", f, err)
		}
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", f, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", f, err)
		}
	}
	return nil
}

func (r *TimescaleRepository) appliedMigrationSet() (map[string]bool, error) {
	rows, err := r.db.Query(`SELECT filename FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()

	out := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return out, nil
}

// resolveMigrationsDir finds internal/storage/migrations from module root (works when cwd is not repo root, e.g. go test).
func resolveMigrationsDir() (string, error) {
	if d := strings.TrimSpace(os.Getenv("ACCOUNT_SERVICE_MIGRATIONS")); d != "" {
		return d, nil
	}
	rel := filepath.Join("internal", "storage", "migrations")
	if _, err := os.Stat(rel); err == nil {
		return rel, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("migrations: getwd: %w", err)
	}
	for {
		candidate := filepath.Join(dir, "internal", "storage", "migrations")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("migrations: cannot find internal/storage/migrations (set ACCOUNT_SERVICE_MIGRATIONS)")
		}
		dir = parent
	}
}

// --- User management ---

func (r *TimescaleRepository) CreateUser(ctx context.Context, user domain.User) (domain.User, error) {
	var created domain.User
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO users (username, password_hash, created_at)
		VALUES ($1, $2, $3)
		RETURNING id, username, password_hash, created_at, plan_code`,
		user.Username, user.PasswordHash, user.CreatedAt,
	).Scan(&created.ID, &created.Username, &created.PasswordHash, &created.CreatedAt, &created.PlanCode)
	if err != nil {
		return domain.User{}, err
	}
	return created, nil
}

func (r *TimescaleRepository) GetUserByUsername(ctx context.Context, username string) (domain.User, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, username, password_hash, created_at, plan_code
		FROM users WHERE username = $1`, username)

	var user domain.User
	if err := row.Scan(&user.ID, &user.Username, &user.PasswordHash, &user.CreatedAt, &user.PlanCode); err != nil {
		if err == sql.ErrNoRows {
			return domain.User{}, ErrNotFound
		}
		return domain.User{}, err
	}
	return user, nil
}

// GetUser fetches a user row by id. Used by control-panel-service for plan
// resolution; password_hash is intentionally NOT scanned and stays out of
// the returned struct (caller has no use for it).
func (r *TimescaleRepository) GetUser(ctx context.Context, userID int64) (domain.User, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, username, created_at, plan_code
		FROM users WHERE id = $1`, userID)
	var user domain.User
	if err := row.Scan(&user.ID, &user.Username, &user.CreatedAt, &user.PlanCode); err != nil {
		if err == sql.ErrNoRows {
			return domain.User{}, ErrNotFound
		}
		return domain.User{}, err
	}
	return user, nil
}

// --- Account management ---

// CreateAccount inserts a new account and returns the auto-assigned BIGINT ID.
func (r *TimescaleRepository) CreateAccount(ctx context.Context, a domain.Account) (int64, error) {
	var newID int64
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO accounts (user_id, name, mode, api_key, api_secret, margin_mode, position_mode, slippage_bps, default_fee_rate, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING account_id`,
		a.UserID, a.Name, int(a.Mode), a.APIKey, a.APISecret,
		a.MarginMode, a.PositionMode, a.SlippageBps, a.DefaultFeeRate, a.CreatedAt,
	).Scan(&newID)
	if err != nil {
		return 0, err
	}
	return newID, nil
}

func (r *TimescaleRepository) GetAccount(ctx context.Context, accountID, userID int64) (domain.Account, error) {
	query := `
		SELECT account_id, user_id, name, mode, api_key, api_secret, margin_mode, position_mode,
		       slippage_bps, default_fee_rate, created_at,
		       futures_json, spot_json, total_value, wallet_balance, available_balance, state_updated_at
		FROM accounts WHERE account_id = $1`
	args := []any{accountID}
	if userID > 0 {
		query += " AND user_id = $2"
		args = append(args, userID)
	}
	row := r.db.QueryRowContext(ctx, query, args...)
	return scanAccount(row)
}

func (r *TimescaleRepository) ListAccounts(ctx context.Context, userID int64) ([]domain.Account, error) {
	query := `
		SELECT account_id, user_id, name, mode, api_key, api_secret, margin_mode, position_mode,
		       slippage_bps, default_fee_rate, created_at,
		       futures_json, spot_json, total_value, wallet_balance, available_balance, state_updated_at
		FROM accounts`
	args := []any{}
	if userID > 0 {
		args = append(args, userID)
		query += " WHERE user_id = $1"
	}
	query += " ORDER BY created_at DESC"
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []domain.Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// scanAccount scans a single account row (supports both *sql.Row and *sql.Rows via the scanner interface).
func scanAccount(s interface {
	Scan(...any) error
}) (domain.Account, error) {
	var a domain.Account
	var mode int
	var futuresRaw, spotRaw []byte
	var stateUpdatedAt sql.NullTime
	if err := s.Scan(
		&a.AccountID, &a.UserID, &a.Name, &mode, &a.APIKey, &a.APISecret,
		&a.MarginMode, &a.PositionMode, &a.SlippageBps, &a.DefaultFeeRate, &a.CreatedAt,
		&futuresRaw, &spotRaw,
		&a.TotalValue, &a.WalletBalance, &a.AvailableBalance, &stateUpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.Account{}, ErrNotFound
		}
		return domain.Account{}, err
	}
	a.Mode = domain.AccountMode(mode)
	if stateUpdatedAt.Valid {
		t := stateUpdatedAt.Time
		a.StateUpdatedAt = &t
	}
	if len(futuresRaw) > 0 {
		var fw domain.FuturesWallet
		if err := json.Unmarshal(futuresRaw, &fw); err != nil {
			return domain.Account{}, fmt.Errorf("unmarshal futures_json: %w", err)
		}
		a.FuturesJSON = &fw
	}
	if len(spotRaw) > 0 {
		var sw domain.SpotWallet
		if err := json.Unmarshal(spotRaw, &sw); err != nil {
			return domain.Account{}, fmt.Errorf("unmarshal spot_json: %w", err)
		}
		a.SpotJSON = &sw
	}
	return a, nil
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "violates unique")
}

// --- Current state management ---

// UpdateAccountState writes the current wallet state into the accounts table (O(1) PK update).
func (r *TimescaleRepository) UpdateAccountState(ctx context.Context, info domain.OnlineAccountInfo) error {
	futuresJSON, err := json.Marshal(info.Futures)
	if err != nil {
		return fmt.Errorf("marshal futures: %w", err)
	}
	spotJSON, err := json.Marshal(info.Spot)
	if err != nil {
		return fmt.Errorf("marshal spot: %w", err)
	}
	now := time.Now().UTC()
	_, err = r.sqlExec.ExecContext(ctx, `
		UPDATE accounts
		SET futures_json      = $1,
		    spot_json         = $2,
		    total_value       = $3,
		    wallet_balance    = $4,
		    available_balance = $5,
		    state_updated_at  = $6
		WHERE account_id = $7`,
		futuresJSON, spotJSON,
		info.TotalValue, info.WalletBalance, info.AvailableBalance,
		now, info.AccountID)
	return err
}

// GetAccountState reads the current wallet state from the accounts table (O(1) PK lookup).
func (r *TimescaleRepository) GetAccountState(ctx context.Context, accountID int64) (domain.OnlineAccountInfo, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT account_id, mode, futures_json, spot_json,
		       total_value, wallet_balance, available_balance, state_updated_at
		FROM accounts WHERE account_id = $1`,
		accountID)

	var info domain.OnlineAccountInfo
	var mode int
	var futuresRaw, spotRaw []byte
	var stateUpdatedAt sql.NullTime
	if err := row.Scan(
		&info.AccountID, &mode,
		&futuresRaw, &spotRaw,
		&info.TotalValue, &info.WalletBalance, &info.AvailableBalance,
		&stateUpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.OnlineAccountInfo{}, ErrNotFound
		}
		return domain.OnlineAccountInfo{}, err
	}
	info.Mode = domain.AccountMode(mode)
	if stateUpdatedAt.Valid {
		info.UpdatedAt = stateUpdatedAt.Time
	}
	if len(futuresRaw) > 0 {
		if err := json.Unmarshal(futuresRaw, &info.Futures); err != nil {
			return domain.OnlineAccountInfo{}, fmt.Errorf("unmarshal futures: %w", err)
		}
	}
	if len(spotRaw) > 0 {
		if err := json.Unmarshal(spotRaw, &info.Spot); err != nil {
			return domain.OnlineAccountInfo{}, fmt.Errorf("unmarshal spot: %w", err)
		}
	}
	return info, nil
}

// --- Snapshot (archive) management ---

// --- Strategy management ---

func (r *TimescaleRepository) CreateStrategy(ctx context.Context, s domain.Strategy) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO strategies (
			user_id, name, version, description, code, archived, created_at,
			runtime_version, runtime_profile
		)
		VALUES ($1, $2, $3, $4, $5, false, NOW(), $6, $7)
		RETURNING strategy_id`,
		s.UserID, s.Name, s.Version, s.Description, s.Code,
		normalizeRuntimeVersion(s.RuntimeVersion), normalizeRuntimeProfile(s.RuntimeProfile),
	).Scan(&id)
	return id, err
}

func (r *TimescaleRepository) GetStrategy(ctx context.Context, strategyID, userID int64) (domain.Strategy, error) {
	query := `
		SELECT strategy_id, user_id, name, version, description, code, archived, created_at,
		       runtime_version, runtime_profile
		FROM strategies WHERE strategy_id = $1`
	args := []any{strategyID}
	if userID > 0 {
		query += " AND user_id = $2"
		args = append(args, userID)
	}
	row := r.db.QueryRowContext(ctx, query, args...)
	return scanStrategy(row)
}

func (r *TimescaleRepository) ListStrategies(ctx context.Context, userID int64, namePrefix string, activeOnly bool) ([]domain.Strategy, error) {
	query := `SELECT strategy_id, user_id, name, version, description, '' AS code, archived, created_at, runtime_version, runtime_profile FROM strategies WHERE 1=1`
	args := []any{}
	if userID > 0 {
		args = append(args, userID)
		query += fmt.Sprintf(" AND user_id = $%d", len(args))
	}
	if namePrefix != "" {
		args = append(args, namePrefix+"%")
		query += fmt.Sprintf(" AND name LIKE $%d", len(args))
	}
	if activeOnly {
		query += " AND archived = false"
	}
	query += " ORDER BY name, version"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.Strategy
	for rows.Next() {
		s, err := scanStrategy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) ArchiveStrategy(ctx context.Context, strategyID int64) error {
	res, err := r.sqlExec.ExecContext(ctx,
		`UPDATE strategies SET archived = true WHERE strategy_id = $1`, strategyID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanStrategy(s interface{ Scan(...any) error }) (domain.Strategy, error) {
	var st domain.Strategy
	if err := s.Scan(&st.StrategyID, &st.UserID, &st.Name, &st.Version, &st.Description, &st.Code, &st.Archived, &st.CreatedAt, &st.RuntimeVersion, &st.RuntimeProfile); err != nil {
		if err == sql.ErrNoRows {
			return domain.Strategy{}, ErrNotFound
		}
		return domain.Strategy{}, err
	}
	st.RuntimeVersion = normalizeRuntimeVersion(st.RuntimeVersion)
	st.RuntimeProfile = normalizeRuntimeProfile(st.RuntimeProfile)
	return st, nil
}

func normalizeRuntimeVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "1.0.0"
	}
	return version
}

func normalizeRuntimeProfile(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return "platform-python-3.13"
	}
	return profile
}

func normalizeSessionType(mode int, sessionType string) string {
	sessionType = strings.TrimSpace(sessionType)
	if sessionType != "" {
		return sessionType
	}
	if mode == int(domain.AccountModeBinanceTestnet) {
		return "testnet"
	}
	return "backtest"
}

// --- Strategy session management ---

const sessionSelectColumns = `
	session_id, account_id, user_id, strategy_id, mode, status, interval,
	start_time_ms, end_time_ms, bars_processed, error,
	runtime_id, runtime_source, runtime_name, session_type, runtime_version, session_name,
	started_at, completed_at, created_at`

func (r *TimescaleRepository) SaveSession(ctx context.Context, s domain.StrategySession) error {
	res, err := r.sqlExec.ExecContext(ctx, `
		INSERT INTO strategy_sessions (
			session_id, account_id, user_id, strategy_id, mode, status, interval,
			start_time_ms, end_time_ms, bars_processed, error,
			runtime_id, runtime_source, runtime_name, session_type, runtime_version, session_name, started_at
		)
		SELECT $1, a.account_id, a.user_id, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		FROM accounts a
		WHERE a.account_id = $17`,
		s.SessionID, s.StrategyID, s.Mode, s.Status, s.Interval,
		s.StartTimeMs, s.EndTimeMs, s.BarsProcessed, s.Error,
		s.RuntimeID, s.RuntimeSource, s.RuntimeName,
		normalizeSessionType(s.Mode, s.SessionType), normalizeRuntimeVersion(s.RuntimeVersion), s.SessionName,
		s.StartedAt, s.AccountID)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) UpdateSession(ctx context.Context, sessionID string, status string, barsProcessed int, errMsg string, runtimeID string) error {
	query := `
			UPDATE strategy_sessions
			SET status = $1, bars_processed = $2, error = $3,
			    completed_at = CASE WHEN $1 IN ('completed','finished','stopped','failed','stop_failed','recoverable') THEN NOW() ELSE completed_at END
			WHERE session_id = $4
			  AND status IN ('running', 'stopping')`
	args := []any{status, barsProcessed, errMsg, sessionID}
	if runtimeID != "" {
		args = append(args, runtimeID)
		query += " AND runtime_id = $5"
	}
	res, err := r.sqlExec.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) GetSession(ctx context.Context, sessionID string, userID int64) (domain.StrategySession, error) {
	query := `SELECT ` + sessionSelectColumns + ` FROM strategy_sessions WHERE session_id = $1`
	args := []any{sessionID}
	if userID > 0 {
		query += " AND user_id = $2"
		args = append(args, userID)
	}
	row := r.db.QueryRowContext(ctx, query, args...)
	return scanSession(row)
}

func (r *TimescaleRepository) ListSessions(ctx context.Context, accountID, userID int64, limit, offset int) ([]domain.StrategySession, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := `SELECT ` + sessionSelectColumns + ` FROM strategy_sessions WHERE account_id = $1`
	args := []any{accountID}
	if userID > 0 {
		args = append(args, userID)
		query += fmt.Sprintf(" AND user_id = $%d", len(args))
	}
	args = append(args, limit, offset)
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.StrategySession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) ListRunningSessions(ctx context.Context, runtimeID string) ([]domain.StrategySession, error) {
	query := `SELECT ` + sessionSelectColumns + ` FROM strategy_sessions WHERE status IN ('running', 'stopping')`
	args := []any{}
	if runtimeID != "" {
		args = append(args, runtimeID)
		query += " AND runtime_id = $1"
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.StrategySession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) MarkRuntimeSessionsRecoverable(ctx context.Context, runtimeID string, errMsg string) (int64, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return 0, ErrNotFound
	}
	res, err := r.sqlExec.ExecContext(ctx, `
		UPDATE strategy_sessions
		SET status = 'recoverable',
		    error = $2,
		    completed_at = COALESCE(completed_at, NOW())
		WHERE runtime_id = $1
		  AND status IN ('running', 'stopping')`,
		runtimeID, errMsg)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *TimescaleRepository) ListSessionSnapshots(
	ctx context.Context,
	sessionID string,
	userID int64,
	limit, offset int,
) ([]domain.SnapshotRow, int64, bool, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	// Fetch limit+1 rows to compute ``has_more`` without scanning the page;
	// the session-wide total (used by the pager's First / Last / jump
	// controls) comes from a separate COUNT(*) query below.
	fetch := limit + 1

	whereClause := "WHERE session_id = $1"
	listArgs := []any{sessionID}
	countArgs := []any{sessionID}
	if userID > 0 {
		whereClause += " AND user_id = $2"
		listArgs = append(listArgs, userID)
		countArgs = append(countArgs, userID)
	}

	listQuery := `
		SELECT time, account_id, snapshot_reason, total_value, wallet_balance, available_balance,
		       COALESCE(futures_json::text, '{}'), COALESCE(spot_json::text, '{}'),
		       COALESCE(session_id, ''), COALESCE(strategy_id, 0)
		FROM account_snapshots ` + whereClause + ` ORDER BY time DESC`
	listQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(listArgs)+1, len(listArgs)+2)
	listArgs = append(listArgs, fetch, offset)

	rows, err := r.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()
	result := make([]domain.SnapshotRow, 0, limit)
	for rows.Next() {
		var s domain.SnapshotRow
		var reason int16
		if err := rows.Scan(&s.Time, &s.AccountID, &reason, &s.TotalValue, &s.WalletBalance,
			&s.AvailableBalance, &s.FuturesJSON, &s.SpotJSON, &s.SessionID, &s.StrategyID); err != nil {
			return nil, 0, false, err
		}
		s.SnapshotReason = domain.SnapshotReason(reason)
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}

	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM account_snapshots `+whereClause, countArgs...,
	).Scan(&total); err != nil {
		return nil, 0, false, err
	}

	return result, total, hasMore, nil
}

func scanSession(s interface{ Scan(...any) error }) (domain.StrategySession, error) {
	var sess domain.StrategySession
	var startMs, endMs sql.NullInt64
	var completedAt sql.NullTime
	var runtimeID, runtimeSource, runtimeName, sessionType, runtimeVersion, sessionName sql.NullString
	if err := s.Scan(
		&sess.SessionID, &sess.AccountID, &sess.UserID, &sess.StrategyID, &sess.Mode, &sess.Status, &sess.Interval,
		&startMs, &endMs, &sess.BarsProcessed, &sess.Error,
		&runtimeID, &runtimeSource, &runtimeName, &sessionType, &runtimeVersion, &sessionName,
		&sess.StartedAt, &completedAt, &sess.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.StrategySession{}, ErrNotFound
		}
		return domain.StrategySession{}, err
	}
	if startMs.Valid {
		v := startMs.Int64
		sess.StartTimeMs = &v
	}
	if endMs.Valid {
		v := endMs.Int64
		sess.EndTimeMs = &v
	}
	if completedAt.Valid {
		sess.CompletedAt = &completedAt.Time
	}
	if runtimeID.Valid {
		sess.RuntimeID = runtimeID.String
	}
	if runtimeSource.Valid {
		sess.RuntimeSource = runtimeSource.String
	}
	if runtimeName.Valid {
		sess.RuntimeName = runtimeName.String
	}
	if sessionType.Valid {
		sess.SessionType = sessionType.String
	}
	if runtimeVersion.Valid {
		sess.RuntimeVersion = runtimeVersion.String
	}
	if sessionName.Valid {
		sess.SessionName = sessionName.String
	}
	sess.SessionType = normalizeSessionType(sess.Mode, sess.SessionType)
	sess.RuntimeVersion = normalizeRuntimeVersion(sess.RuntimeVersion)
	return sess, nil
}

// --- Account strategy mount management ---

func (r *TimescaleRepository) MountStrategy(ctx context.Context, accountID, strategyID int64) error {
	_, err := r.sqlExec.ExecContext(ctx, `
		INSERT INTO account_strategies (account_id, strategy_id, active, mounted_at)
		VALUES ($1, $2, false, NOW())
		ON CONFLICT (account_id, strategy_id) DO NOTHING`,
		accountID, strategyID)
	return err
}

func (r *TimescaleRepository) UnmountStrategy(ctx context.Context, accountID, strategyID int64) error {
	res, err := r.sqlExec.ExecContext(ctx,
		`DELETE FROM account_strategies WHERE account_id = $1 AND strategy_id = $2`,
		accountID, strategyID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ActivateStrategy sets the specified strategy as active for the account, clearing any previous active entry.
// Uses a transaction to atomically deactivate all then activate the target.
func (r *TimescaleRepository) ActivateStrategy(ctx context.Context, accountID, strategyID int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx,
		`UPDATE account_strategies SET active = false WHERE account_id = $1`, accountID); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE account_strategies SET active = true WHERE account_id = $1 AND strategy_id = $2`,
		accountID, strategyID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return tx.Commit()
}

func (r *TimescaleRepository) DeactivateStrategy(ctx context.Context, accountID, strategyID int64) error {
	res, err := r.sqlExec.ExecContext(ctx,
		`UPDATE account_strategies SET active = false WHERE account_id = $1 AND strategy_id = $2`,
		accountID, strategyID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) ListAccountStrategies(ctx context.Context, accountID int64) ([]domain.AccountStrategy, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT s.strategy_id, s.user_id, s.name, s.version, s.description, '' AS code, s.archived, s.created_at,
		       s.runtime_version, s.runtime_profile, as2.active, as2.mounted_at
		FROM account_strategies as2
		JOIN strategies s ON s.strategy_id = as2.strategy_id
		WHERE as2.account_id = $1
		ORDER BY as2.mounted_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.AccountStrategy
	for rows.Next() {
		var entry domain.AccountStrategy
		entry.AccountID = accountID
		if err := rows.Scan(
			&entry.Strategy.StrategyID, &entry.Strategy.UserID, &entry.Strategy.Name, &entry.Strategy.Version,
			&entry.Strategy.Description, &entry.Strategy.Code, &entry.Strategy.Archived, &entry.Strategy.CreatedAt,
			&entry.Strategy.RuntimeVersion, &entry.Strategy.RuntimeProfile, &entry.Active, &entry.MountedAt,
		); err != nil {
			return nil, err
		}
		entry.Strategy.RuntimeVersion = normalizeRuntimeVersion(entry.Strategy.RuntimeVersion)
		entry.Strategy.RuntimeProfile = normalizeRuntimeProfile(entry.Strategy.RuntimeProfile)
		entry.StrategyID = entry.Strategy.StrategyID
		result = append(result, entry)
	}
	return result, rows.Err()
}

func (r *TimescaleRepository) GetActiveStrategy(ctx context.Context, accountID int64) (domain.Strategy, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT s.strategy_id, s.user_id, s.name, s.version, s.description, s.code, s.archived, s.created_at,
		       s.runtime_version, s.runtime_profile
		FROM account_strategies as2
		JOIN strategies s ON s.strategy_id = as2.strategy_id
		WHERE as2.account_id = $1 AND as2.active = true
		LIMIT 1`, accountID)
	return scanStrategy(row)
}

// SaveSnapshot reads the current wallet state from accounts table and writes a snapshot row.
// Reason > 0 indicates an event-driven snapshot; reason = 0 is used for initial_seed.
// strategyID=0 means no strategy (manual or system-triggered snapshot).
func (r *TimescaleRepository) SaveSnapshot(ctx context.Context, accountID int64, reason domain.SnapshotReason, strategyID int64, sessionID string) error {
	// Read current state from accounts table
	info, err := r.GetAccountState(ctx, accountID)
	if err != nil {
		return fmt.Errorf("get account state for snapshot: %w", err)
	}

	futuresJSON, err := json.Marshal(info.Futures)
	if err != nil {
		return fmt.Errorf("marshal futures: %w", err)
	}
	spotJSON, err := json.Marshal(info.Spot)
	if err != nil {
		return fmt.Errorf("marshal spot: %w", err)
	}

	var sid *int64
	if strategyID != 0 {
		sid = &strategyID
	}
	var sessID *string
	if sessionID != "" {
		sessID = &sessionID
	}

	recordedAt := time.Now().UTC()
	res, err := r.sqlExec.ExecContext(ctx, `
		INSERT INTO account_snapshots
			(time, account_id, user_id, mode, futures_json, spot_json, total_value, wallet_balance, available_balance, snapshot_reason, strategy_id, session_id)
		SELECT $1, a.account_id, a.user_id, $2, $3, $4, $5, $6, $7, $8, $9, $10
		FROM accounts a
		WHERE a.account_id = $11`,
		recordedAt, int(info.Mode),
		futuresJSON, spotJSON,
		info.TotalValue, info.WalletBalance, info.AvailableBalance,
		int16(reason), sid, sessID, info.AccountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SaveReconciliationRun persists one Phase C shadow-compare run.
// Both snapshots are stored as canonical JSON so future providers can reuse
// the same table layout without provider-specific schema.
//
// Unlike SaveSnapshot, this method does NOT join the accounts table — the
// caller (reconciliation goroutine) already has both snapshots in memory and
// is writing the audit record; there is no live state to re-read.
func (r *TimescaleRepository) SaveReconciliationRun(ctx context.Context, run domain.ReconciliationRun) error {
	exchangeJSON, err := json.Marshal(run.ExchangeSnapshot)
	if err != nil {
		return fmt.Errorf("marshal exchange snapshot: %w", err)
	}
	localJSON, err := json.Marshal(run.LocalSnapshot)
	if err != nil {
		return fmt.Errorf("marshal local snapshot: %w", err)
	}
	// Always serialize as a JSON array (not null) so downstream consumers
	// can unconditionally iterate.
	if run.FieldDiffs == nil {
		run.FieldDiffs = []domain.FieldDiff{}
	}
	if run.AdvisoryDiffs == nil {
		run.AdvisoryDiffs = []domain.FieldDiff{}
	}
	fieldDiffsJSON, err := json.Marshal(run.FieldDiffs)
	if err != nil {
		return fmt.Errorf("marshal field_diffs: %w", err)
	}
	advisoryDiffsJSON, err := json.Marshal(run.AdvisoryDiffs)
	if err != nil {
		return fmt.Errorf("marshal advisory_diffs: %w", err)
	}

	var sid *int64
	if run.StrategyID != 0 {
		sid = &run.StrategyID
	}
	var sessID *string
	if run.SessionID != "" {
		sessID = &run.SessionID
	}

	_, err = r.sqlExec.ExecContext(ctx, `
		INSERT INTO reconciliation_runs
			(time, account_id, user_id, session_id, strategy_id, mode, snapshot_reason, run_type,
			 exchange_snapshot, local_snapshot, field_diffs, advisory_diffs, hard_pass, soft_pass)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8,
			 $9, $10, $11, $12, $13, $14)`,
		run.Time.UTC(), run.AccountID, run.UserID, sessID, sid,
		int(run.Mode), int16(run.SnapshotReason), string(run.RunType),
		exchangeJSON, localJSON, fieldDiffsJSON, advisoryDiffsJSON,
		run.HardPass, run.SoftPass)
	return err
}

// ListReconciliationRuns returns persisted reconciliation runs for one session,
// newest first. The UI uses this to inspect checkpoint / event / sampled drift
// during Phase C3 smoke and calibration work.
//
// Offset-based paging: fetches “limit+1“ rows for the “has_more“ sentinel
// and runs a separate COUNT(*) for the session-wide “total“ (used by the
// pager's First / Last / jump-to-page controls).
func (r *TimescaleRepository) ListReconciliationRuns(
	ctx context.Context,
	sessionID string,
	userID int64,
	limit, offset int,
) ([]domain.ReconciliationRun, int64, bool, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	fetch := limit + 1

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			time,
			run_id::text,
			account_id,
			user_id,
			COALESCE(session_id, ''),
			COALESCE(strategy_id, 0),
			mode,
			snapshot_reason,
			run_type,
			exchange_snapshot,
			local_snapshot,
			field_diffs,
			advisory_diffs,
			hard_pass,
			soft_pass
		FROM reconciliation_runs
		WHERE user_id = $1
		  AND session_id = $2
		ORDER BY time DESC, run_id DESC
		LIMIT $3 OFFSET $4
	`, userID, sessionID, fetch, offset)
	if err != nil {
		return nil, 0, false, fmt.Errorf("query reconciliation runs: %w", err)
	}
	defer rows.Close()

	out := make([]domain.ReconciliationRun, 0)
	for rows.Next() {
		var run domain.ReconciliationRun
		var mode int
		var reason int16
		var exchangeJSON []byte
		var localJSON []byte
		var fieldDiffsJSON []byte
		var advisoryDiffsJSON []byte
		var runType string
		if err := rows.Scan(
			&run.Time,
			&run.RunID,
			&run.AccountID,
			&run.UserID,
			&run.SessionID,
			&run.StrategyID,
			&mode,
			&reason,
			&runType,
			&exchangeJSON,
			&localJSON,
			&fieldDiffsJSON,
			&advisoryDiffsJSON,
			&run.HardPass,
			&run.SoftPass,
		); err != nil {
			return nil, 0, false, fmt.Errorf("scan reconciliation run: %w", err)
		}
		run.Mode = domain.AccountMode(mode)
		run.SnapshotReason = domain.SnapshotReason(reason)
		run.RunType = domain.ReconciliationRunType(runType)
		if err := json.Unmarshal(exchangeJSON, &run.ExchangeSnapshot); err != nil {
			return nil, 0, false, fmt.Errorf("decode exchange_snapshot for run %s: %w", run.RunID, err)
		}
		if err := json.Unmarshal(localJSON, &run.LocalSnapshot); err != nil {
			return nil, 0, false, fmt.Errorf("decode local_snapshot for run %s: %w", run.RunID, err)
		}
		if err := json.Unmarshal(fieldDiffsJSON, &run.FieldDiffs); err != nil {
			return nil, 0, false, fmt.Errorf("decode field_diffs for run %s: %w", run.RunID, err)
		}
		if err := json.Unmarshal(advisoryDiffsJSON, &run.AdvisoryDiffs); err != nil {
			return nil, 0, false, fmt.Errorf("decode advisory_diffs for run %s: %w", run.RunID, err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("iterate reconciliation runs: %w", err)
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}

	var total int64
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM reconciliation_runs
		WHERE user_id = $1 AND session_id = $2
	`, userID, sessionID).Scan(&total); err != nil {
		return nil, 0, false, fmt.Errorf("count reconciliation runs: %w", err)
	}

	return out, total, hasMore, nil
}

// GetSessionReconciliationSummary returns session-wide aggregates over
// “reconciliation_runs“ so the SessionDetailPage tile can render the real
// total / hard fail / soft fail counts instead of the current-page slice.
func (r *TimescaleRepository) GetSessionReconciliationSummary(
	ctx context.Context,
	sessionID string,
	userID int64,
) (totalRuns, hardFailRuns, softFailRuns int64, err error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE NOT hard_pass),
			COUNT(*) FILTER (WHERE NOT soft_pass)
		FROM reconciliation_runs
		WHERE user_id = $1 AND session_id = $2
	`, userID, sessionID)
	if scanErr := row.Scan(&totalRuns, &hardFailRuns, &softFailRuns); scanErr != nil {
		return 0, 0, 0, fmt.Errorf("scan reconciliation summary: %w", scanErr)
	}
	return totalRuns, hardFailRuns, softFailRuns, nil
}

// --- Notification management ---

func defaultNotificationSettings(userID int64) domain.NotificationSettings {
	return domain.NotificationSettings{
		UserID:          userID,
		SystemEnabled:   true,
		StrategyEnabled: true,
		CustomEnabled:   true,
	}
}

func defaultNotificationChannel(userID int64, channel string) domain.NotificationChannel {
	if strings.TrimSpace(channel) == "" {
		channel = domain.NotificationChannelTelegram
	}
	return domain.NotificationChannel{
		UserID:  userID,
		Channel: channel,
		Status:  domain.NotificationChannelStatusUnbound,
	}
}

func (r *TimescaleRepository) GetNotificationSettings(ctx context.Context, userID int64) (domain.NotificationSettings, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT user_id, system_enabled, strategy_enabled, custom_enabled,
		       last_delivery_status, last_delivery_error, last_delivery_at,
		       last_test_message_at, created_at, updated_at
		FROM notification_settings
		WHERE user_id = $1`, userID)
	settings, err := scanNotificationSettings(row)
	if err == nil {
		return settings, nil
	}
	if err == ErrNotFound {
		return defaultNotificationSettings(userID), nil
	}
	return domain.NotificationSettings{}, err
}

func (r *TimescaleRepository) UpsertNotificationSettings(ctx context.Context, settings domain.NotificationSettings) (domain.NotificationSettings, error) {
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO notification_settings (
			user_id, system_enabled, strategy_enabled, custom_enabled, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (user_id) DO UPDATE SET
			system_enabled = EXCLUDED.system_enabled,
			strategy_enabled = EXCLUDED.strategy_enabled,
			custom_enabled = EXCLUDED.custom_enabled,
			updated_at = NOW()
		RETURNING user_id, system_enabled, strategy_enabled, custom_enabled,
		          last_delivery_status, last_delivery_error, last_delivery_at,
		          last_test_message_at, created_at, updated_at`,
		settings.UserID, settings.SystemEnabled, settings.StrategyEnabled, settings.CustomEnabled)
	return scanNotificationSettings(row)
}

func (r *TimescaleRepository) GetNotificationChannel(ctx context.Context, userID int64, channel string) (domain.NotificationChannel, error) {
	channel = normalizeNotificationChannel(channel)
	row := r.db.QueryRowContext(ctx, `
		SELECT id, user_id, channel, status, target_id, target_type, target_label,
		       bind_code_hash, bind_code_expires_at, bound_at, revoked_at,
		       last_delivery_status, last_delivery_error, last_delivery_at,
		       created_at, updated_at
		FROM notification_channels
		WHERE user_id = $1 AND channel = $2`, userID, channel)
	ch, err := scanNotificationChannel(row)
	if err == nil {
		return ch, nil
	}
	if err == ErrNotFound {
		return defaultNotificationChannel(userID, channel), nil
	}
	return domain.NotificationChannel{}, err
}

func (r *TimescaleRepository) FindNotificationChannelByBindCodeHash(ctx context.Context, codeHash string, at time.Time) (domain.NotificationChannel, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, user_id, channel, status, target_id, target_type, target_label,
		       bind_code_hash, bind_code_expires_at, bound_at, revoked_at,
		       last_delivery_status, last_delivery_error, last_delivery_at,
		       created_at, updated_at
		FROM notification_channels
		WHERE bind_code_hash = $1
		  AND bind_code_expires_at > $2
		  AND status IN ($3, $4)
		ORDER BY bind_code_expires_at DESC
		LIMIT 1`, codeHash, at.UTC(), domain.NotificationChannelStatusPending, domain.NotificationChannelStatusBound)
	return scanNotificationChannel(row)
}

func (r *TimescaleRepository) UpsertNotificationBindCode(ctx context.Context, userID int64, channel string, codeHash string, expiresAt time.Time) (domain.NotificationChannel, error) {
	channel = normalizeNotificationChannel(channel)
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO notification_channels (
			user_id, channel, status, bind_code_hash, bind_code_expires_at, revoked_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, NULL, NOW(), NOW())
		ON CONFLICT (user_id, channel) DO UPDATE SET
			status = CASE
				WHEN notification_channels.status = 'bound'
				     AND COALESCE(notification_channels.target_id, '') <> ''
				THEN notification_channels.status
				ELSE EXCLUDED.status
			END,
			bind_code_hash = EXCLUDED.bind_code_hash,
			bind_code_expires_at = EXCLUDED.bind_code_expires_at,
			revoked_at = NULL,
			updated_at = NOW()
		RETURNING id, user_id, channel, status, target_id, target_type, target_label,
		          bind_code_hash, bind_code_expires_at, bound_at, revoked_at,
		          last_delivery_status, last_delivery_error, last_delivery_at,
		          created_at, updated_at`,
		userID, channel, domain.NotificationChannelStatusPending, codeHash, expiresAt.UTC())
	return scanNotificationChannel(row)
}

func (r *TimescaleRepository) BindNotificationChannel(ctx context.Context, userID int64, channel string, targetID string, targetType string, targetLabel string, now time.Time) (domain.NotificationChannel, error) {
	channel = normalizeNotificationChannel(channel)
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO notification_channels (
			user_id, channel, status, target_id, target_type, target_label,
			bind_code_hash, bind_code_expires_at, bound_at, revoked_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, $7, NULL, NOW(), NOW())
		ON CONFLICT (user_id, channel) DO UPDATE SET
			status = EXCLUDED.status,
			target_id = EXCLUDED.target_id,
			target_type = EXCLUDED.target_type,
			target_label = EXCLUDED.target_label,
			bind_code_hash = NULL,
			bind_code_expires_at = NULL,
			bound_at = EXCLUDED.bound_at,
			revoked_at = NULL,
			updated_at = NOW()
		RETURNING id, user_id, channel, status, target_id, target_type, target_label,
		          bind_code_hash, bind_code_expires_at, bound_at, revoked_at,
		          last_delivery_status, last_delivery_error, last_delivery_at,
		          created_at, updated_at`,
		userID, channel, domain.NotificationChannelStatusBound, targetID, targetType, targetLabel, now.UTC())
	return scanNotificationChannel(row)
}

func (r *TimescaleRepository) RevokeNotificationChannel(ctx context.Context, userID int64, channel string, now time.Time) error {
	channel = normalizeNotificationChannel(channel)
	_, err := r.sqlExec.ExecContext(ctx, `
		INSERT INTO notification_channels (
			user_id, channel, status, target_id, target_type, target_label,
			bind_code_hash, bind_code_expires_at, revoked_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, NULL, $4, NOW(), NOW())
		ON CONFLICT (user_id, channel) DO UPDATE SET
			status = EXCLUDED.status,
			target_id = NULL,
			target_type = NULL,
			target_label = NULL,
			bind_code_hash = NULL,
			bind_code_expires_at = NULL,
			revoked_at = EXCLUDED.revoked_at,
			updated_at = NOW()`,
		userID, channel, domain.NotificationChannelStatusRevoked, now.UTC())
	return err
}

func (r *TimescaleRepository) UpdateNotificationDeliveryStatus(ctx context.Context, userID int64, channel string, statusText string, errText string, at time.Time) error {
	channel = normalizeNotificationChannel(channel)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO notification_settings (
			user_id, system_enabled, strategy_enabled, custom_enabled,
			last_delivery_status, last_delivery_error, last_delivery_at, created_at, updated_at
		)
		VALUES ($1, TRUE, TRUE, TRUE, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (user_id) DO UPDATE SET
			last_delivery_status = EXCLUDED.last_delivery_status,
			last_delivery_error = EXCLUDED.last_delivery_error,
			last_delivery_at = EXCLUDED.last_delivery_at,
			updated_at = NOW()`,
		userID, statusText, errText, at.UTC()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO notification_channels (
			user_id, channel, status, last_delivery_status, last_delivery_error,
			last_delivery_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (user_id, channel) DO UPDATE SET
			last_delivery_status = EXCLUDED.last_delivery_status,
			last_delivery_error = EXCLUDED.last_delivery_error,
			last_delivery_at = EXCLUDED.last_delivery_at,
			updated_at = NOW()`,
		userID, channel, domain.NotificationChannelStatusUnbound, statusText, errText, at.UTC()); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func (r *TimescaleRepository) GetNotificationPlan(ctx context.Context, planCode string) (domain.NotificationPlan, error) {
	planCode = strings.TrimSpace(planCode)
	if planCode == "" {
		planCode = "free"
	}
	plan, err := r.getNotificationPlan(ctx, planCode)
	if err == nil {
		return plan, nil
	}
	if err != ErrNotFound || planCode == "free" {
		if err == ErrNotFound && planCode == "free" {
			return domain.NotificationPlan{PlanCode: "free"}, nil
		}
		return domain.NotificationPlan{}, err
	}
	plan, err = r.getNotificationPlan(ctx, "free")
	if err == ErrNotFound {
		return domain.NotificationPlan{PlanCode: "free"}, nil
	}
	return plan, err
}

func (r *TimescaleRepository) getNotificationPlan(ctx context.Context, planCode string) (domain.NotificationPlan, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT plan_code, notification_enabled, allow_system, allow_strategy, allow_custom,
		       custom_rate_limit_per_minute, custom_rate_limit_burst, updated_at
		FROM notification_plans
		WHERE plan_code = $1`, planCode)
	var plan domain.NotificationPlan
	if err := row.Scan(
		&plan.PlanCode,
		&plan.NotificationEnabled,
		&plan.AllowSystem,
		&plan.AllowStrategy,
		&plan.AllowCustom,
		&plan.CustomRateLimitPerMinute,
		&plan.CustomRateLimitBurst,
		&plan.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.NotificationPlan{}, ErrNotFound
		}
		return domain.NotificationPlan{}, err
	}
	return plan, nil
}

func normalizeNotificationChannel(channel string) string {
	channel = strings.ToLower(strings.TrimSpace(channel))
	if channel == "" {
		return domain.NotificationChannelTelegram
	}
	return channel
}

func scanNotificationSettings(s interface{ Scan(...any) error }) (domain.NotificationSettings, error) {
	var settings domain.NotificationSettings
	var lastStatus, lastErr sql.NullString
	var lastAt, lastTestAt sql.NullTime
	if err := s.Scan(
		&settings.UserID,
		&settings.SystemEnabled,
		&settings.StrategyEnabled,
		&settings.CustomEnabled,
		&lastStatus,
		&lastErr,
		&lastAt,
		&lastTestAt,
		&settings.CreatedAt,
		&settings.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.NotificationSettings{}, ErrNotFound
		}
		return domain.NotificationSettings{}, err
	}
	settings.LastDeliveryStatus = nullString(lastStatus)
	settings.LastDeliveryError = nullString(lastErr)
	settings.LastDeliveryAt = nullTimePtr(lastAt)
	settings.LastTestMessageAt = nullTimePtr(lastTestAt)
	return settings, nil
}

func scanNotificationChannel(s interface{ Scan(...any) error }) (domain.NotificationChannel, error) {
	var ch domain.NotificationChannel
	var targetID, targetType, targetLabel, codeHash sql.NullString
	var lastStatus, lastErr sql.NullString
	var codeExpires, boundAt, revokedAt, lastAt sql.NullTime
	if err := s.Scan(
		&ch.ID,
		&ch.UserID,
		&ch.Channel,
		&ch.Status,
		&targetID,
		&targetType,
		&targetLabel,
		&codeHash,
		&codeExpires,
		&boundAt,
		&revokedAt,
		&lastStatus,
		&lastErr,
		&lastAt,
		&ch.CreatedAt,
		&ch.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return domain.NotificationChannel{}, ErrNotFound
		}
		return domain.NotificationChannel{}, err
	}
	ch.TargetID = nullString(targetID)
	ch.TargetType = nullString(targetType)
	ch.TargetLabel = nullString(targetLabel)
	ch.BindCodeHash = nullString(codeHash)
	ch.BindCodeExpiresAt = nullTimePtr(codeExpires)
	ch.BoundAt = nullTimePtr(boundAt)
	ch.RevokedAt = nullTimePtr(revokedAt)
	ch.LastDeliveryStatus = nullString(lastStatus)
	ch.LastDeliveryError = nullString(lastErr)
	ch.LastDeliveryAt = nullTimePtr(lastAt)
	return ch, nil
}

func nullString(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return ""
}

func nullTimePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time
	return &t
}
