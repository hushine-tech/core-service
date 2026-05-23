package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/hushine-tech/golang-lib/middleware/sqlmiddleware"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

// TimescaleRepository implements Repository backed by TimescaleDB.
type TimescaleRepository struct {
	db      *sql.DB
	sqlExec *sqlmiddleware.Middleware
}

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

	repo := &TimescaleRepository{db: db, sqlExec: sqlmiddleware.New(db, logger)}
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

func resolveMigrationsDir() (string, error) {
	if d := strings.TrimSpace(os.Getenv("ORDER_MIGRATIONS")); d != "" {
		return d, nil
	}
	if d := strings.TrimSpace(os.Getenv("ORDER_SERVICE_MIGRATIONS")); d != "" {
		return d, nil
	}
	rel := filepath.Join("internal", "order", "storage", "migrations")
	if _, err := os.Stat(rel); err == nil {
		return rel, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("migrations: getwd: %w", err)
	}
	for {
		candidate := filepath.Join(dir, "internal", "order", "storage", "migrations")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("migrations: cannot find internal/order/storage/migrations (set ORDER_MIGRATIONS)")
		}
		dir = parent
	}
}

// Close releases the database connection pool.
func (r *TimescaleRepository) Close() error {
	return r.db.Close()
}

func (r *TimescaleRepository) UpsertOrderIntent(ctx context.Context, intent OrderIntent) error {
	_, err := r.sqlExec.ExecContext(ctx, `
		INSERT INTO order_intents (
			intent_id, time, updated_at, account_id, user_id, strategy_id, session_id,
			market, symbol, side, requested_qty, requested_price
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (intent_id) DO UPDATE SET
			updated_at = EXCLUDED.updated_at,
			account_id = EXCLUDED.account_id,
			user_id = EXCLUDED.user_id,
			strategy_id = EXCLUDED.strategy_id,
			session_id = EXCLUDED.session_id,
			market = EXCLUDED.market,
			symbol = EXCLUDED.symbol,
			side = EXCLUDED.side,
			requested_qty = EXCLUDED.requested_qty,
			requested_price = EXCLUDED.requested_price`,
		intent.IntentID, intent.Time, intent.Time, intent.AccountID, intent.UserID, nullableInt64(intent.StrategyID),
		nullableString(intent.SessionID), nullableString(intent.Market), intent.Symbol, intent.Side,
		intent.RequestedQty, nullableFloat64(intent.RequestedPrice),
	)
	return err
}

func (r *TimescaleRepository) CreateOrderAttempt(ctx context.Context, attempt OrderAttempt) error {
	_, err := r.sqlExec.ExecContext(ctx, `
			INSERT INTO order_attempts (
				attempt_id, intent_id, time, updated_at, account_id, user_id, strategy_id, session_id,
				market, symbol, side, requested_qty, requested_price, mark_price, status,
				error_message, client_order_id, recovery_error, order_id, exchange_order_id, mode
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`,
		attempt.AttemptID, attempt.IntentID, attempt.Time, attempt.Time, attempt.AccountID, attempt.UserID,
		nullableInt64(attempt.StrategyID), nullableString(attempt.SessionID), nullableString(attempt.Market),
		attempt.Symbol, attempt.Side, attempt.RequestedQty, nullableFloat64(attempt.RequestedPrice),
		attempt.MarkPrice, attempt.Status, attempt.ErrorMessage, nullableString(attempt.ClientOrderID),
		attempt.RecoveryError, nullableString(attempt.OrderID), nullableString(attempt.ExchangeOrderID), attempt.Mode,
	)
	return err
}

func (r *TimescaleRepository) FinalizeOrderAttempt(ctx context.Context, attempt OrderAttempt, order *Order, fills []OrderFill) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize attempt: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
			UPDATE order_attempts
			SET updated_at = $2,
			    status = $3,
			    error_message = $4,
			    client_order_id = $5,
			    recovery_error = $6,
			    order_id = $7,
			    exchange_order_id = $8
			WHERE attempt_id = $1`,
		attempt.AttemptID, attempt.Time, attempt.Status, attempt.ErrorMessage,
		nullableString(attempt.ClientOrderID), attempt.RecoveryError,
		nullableString(attempt.OrderID), nullableString(attempt.ExchangeOrderID),
	); err != nil {
		return fmt.Errorf("update order_attempts: %w", err)
	}

	if order != nil {
		if _, err = tx.ExecContext(ctx, `
				INSERT INTO orders (
					order_id, exchange_order_id, attempt_id, intent_id, time, updated_at,
					account_id, user_id, strategy_id, session_id, market, symbol, side, client_order_id,
					orig_qty, executed_qty, remaining_qty, avg_price, price, status, error_message, mode
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
				ON CONFLICT (order_id) DO UPDATE SET
					updated_at = EXCLUDED.updated_at,
					exchange_order_id = EXCLUDED.exchange_order_id,
					client_order_id = EXCLUDED.client_order_id,
					executed_qty = EXCLUDED.executed_qty,
					remaining_qty = EXCLUDED.remaining_qty,
					avg_price = EXCLUDED.avg_price,
				price = EXCLUDED.price,
				status = EXCLUDED.status,
				error_message = EXCLUDED.error_message`,
			order.OrderID, nullableString(order.ExchangeOrderID), order.AttemptID, order.IntentID,
			order.Time, order.Time, order.AccountID, order.UserID, nullableInt64(order.StrategyID),
			nullableString(order.SessionID), nullableString(order.Market), order.Symbol, order.Side, nullableString(order.ClientOrderID),
			order.OrigQty, order.ExecutedQty, order.RemainingQty, order.AvgPrice,
			nullableFloat64(order.Price), order.Status, order.ErrorMessage, order.Mode,
		); err != nil {
			return fmt.Errorf("upsert orders: %w", err)
		}
	}

	for _, fill := range fills {
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO order_fills (
				time, fill_id, exchange_trade_id, order_id, exchange_order_id, attempt_id, intent_id,
				account_id, user_id, symbol, side, qty, fill_price, fee, status, mode, strategy_id, market, session_id
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
			fill.Time, fill.FillID, nullableString(fill.ExchangeTradeID), fill.OrderID,
			nullableString(fill.ExchangeOrderID), fill.AttemptID, fill.IntentID, fill.AccountID, fill.UserID,
			fill.Symbol, fill.Side, fill.Qty, fill.FillPrice, fill.Fee, fill.Status, fill.Mode,
			nullableInt64(fill.StrategyID), nullableString(fill.Market), nullableString(fill.SessionID),
		); err != nil {
			return fmt.Errorf("insert order_fills: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize attempt: %w", err)
	}
	return nil
}

func (r *TimescaleRepository) FindOrderAttempt(ctx context.Context, userID, accountID int64, intentID, attemptID, clientOrderID string) (OrderAttempt, error) {
	if userID <= 0 {
		return OrderAttempt{}, fmt.Errorf("userID is required")
	}
	base := `
		SELECT time, attempt_id, intent_id, account_id, user_id, symbol, side, requested_qty,
		       COALESCE(requested_price, 0), mark_price, status, mode, error_message,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, ''),
		       COALESCE(client_order_id, ''), COALESCE(recovery_error, ''), COALESCE(order_id, ''), COALESCE(exchange_order_id, '')
		FROM order_attempts
		WHERE user_id = $1 AND account_id = $2`
	args := []any{userID, accountID}
	if strings.TrimSpace(attemptID) != "" {
		args = append(args, attemptID)
		base += fmt.Sprintf(" AND attempt_id = $%d", len(args))
	} else if strings.TrimSpace(clientOrderID) != "" {
		args = append(args, clientOrderID)
		base += fmt.Sprintf(" AND client_order_id = $%d", len(args))
	} else if strings.TrimSpace(intentID) != "" {
		args = append(args, intentID)
		base += fmt.Sprintf(" AND intent_id = $%d", len(args))
	} else {
		return OrderAttempt{}, fmt.Errorf("intent_id, attempt_id, or client_order_id is required")
	}
	base += " ORDER BY time DESC LIMIT 1"

	var item OrderAttempt
	err := r.db.QueryRowContext(ctx, base, args...).Scan(
		&item.Time, &item.AttemptID, &item.IntentID, &item.AccountID, &item.UserID, &item.Symbol, &item.Side,
		&item.RequestedQty, &item.RequestedPrice, &item.MarkPrice, &item.Status, &item.Mode, &item.ErrorMessage,
		&item.StrategyID, &item.Market, &item.SessionID, &item.ClientOrderID, &item.RecoveryError, &item.OrderID, &item.ExchangeOrderID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return OrderAttempt{}, ErrNotFound
	}
	if err != nil {
		return OrderAttempt{}, fmt.Errorf("find order attempt: %w", err)
	}
	return item, nil
}

func (r *TimescaleRepository) FindOrderByAttempt(ctx context.Context, attemptID string) (Order, error) {
	query := `
		SELECT time, order_id, COALESCE(exchange_order_id, ''), COALESCE(client_order_id, ''), attempt_id, intent_id, account_id, user_id,
		       symbol, side, orig_qty, executed_qty, remaining_qty, avg_price, status, mode, error_message,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, ''), COALESCE(price, 0)
		FROM orders
		WHERE attempt_id = $1
		LIMIT 1`
	var item Order
	err := r.db.QueryRowContext(ctx, query, attemptID).Scan(
		&item.Time, &item.OrderID, &item.ExchangeOrderID, &item.ClientOrderID, &item.AttemptID, &item.IntentID, &item.AccountID, &item.UserID,
		&item.Symbol, &item.Side, &item.OrigQty, &item.ExecutedQty, &item.RemainingQty, &item.AvgPrice, &item.Status, &item.Mode, &item.ErrorMessage,
		&item.StrategyID, &item.Market, &item.SessionID, &item.Price,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("find order by attempt: %w", err)
	}
	return item, nil
}

func (r *TimescaleRepository) ListOrderFillsByAttempt(ctx context.Context, attemptID string) ([]OrderFill, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT time, fill_id, COALESCE(exchange_trade_id, ''), order_id, COALESCE(exchange_order_id, ''),
		       attempt_id, intent_id, account_id, user_id, symbol, side, qty, fill_price, fee, status, mode,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, '')
		FROM order_fills
		WHERE attempt_id = $1
		ORDER BY time ASC, fill_id ASC`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("list order fills by attempt: %w", err)
	}
	defer rows.Close()
	out := []OrderFill{}
	for rows.Next() {
		var item OrderFill
		if err := rows.Scan(
			&item.Time, &item.FillID, &item.ExchangeTradeID, &item.OrderID, &item.ExchangeOrderID, &item.AttemptID,
			&item.IntentID, &item.AccountID, &item.UserID, &item.Symbol, &item.Side, &item.Qty, &item.FillPrice,
			&item.Fee, &item.Status, &item.Mode, &item.StrategyID, &item.Market, &item.SessionID,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func buildScopedWhere(userID, accountID, strategyID int64, sessionID string) (string, []any, error) {
	if userID <= 0 {
		return "", nil, fmt.Errorf("userID is required")
	}
	where := "WHERE user_id = $1"
	args := []any{userID}
	if sessionID != "" {
		args = append(args, sessionID)
		where += fmt.Sprintf(" AND session_id = $%d", len(args))
	} else if accountID > 0 {
		args = append(args, accountID)
		where += fmt.Sprintf(" AND account_id = $%d", len(args))
	}
	if strategyID > 0 {
		args = append(args, strategyID)
		where += fmt.Sprintf(" AND strategy_id = $%d", len(args))
	}
	return where, args, nil
}

// ancestorFilter is one optional WHERE-clause equality filter (column, value).
type ancestorFilter struct {
	col string
	val string
}

// appendAncestorFilters extends an existing WHERE clause with optional
// ancestor-ID equality filters in a stable order. Each non-empty value adds an
// `AND <col> = $N` predicate; empty values are skipped, preserving the legacy
// behavior.
func appendAncestorFilters(where string, args []any, filters []ancestorFilter) (string, []any) {
	for _, f := range filters {
		if f.val == "" {
			continue
		}
		args = append(args, f.val)
		where += fmt.Sprintf(" AND %s = $%d", f.col, len(args))
	}
	return where, args
}

func applyLimitOffset(query string, args []any, limit, offset int) (string, []any) {
	pageArgs := append([]any(nil), args...)
	if limit > 0 {
		pageArgs = append(pageArgs, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(pageArgs))
	}
	if offset > 0 {
		pageArgs = append(pageArgs, offset)
		query += fmt.Sprintf(" OFFSET $%d", len(pageArgs))
	}
	return query, pageArgs
}

func (r *TimescaleRepository) QueryOrderIntentsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID string, limit, offset int) ([]OrderIntent, int64, error) {
	where, args, err := buildScopedWhere(userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_intents "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count order_intents: %w", err)
	}
	query := `
		SELECT time, intent_id, account_id, user_id, COALESCE(strategy_id, 0),
		       COALESCE(session_id, ''), COALESCE(market, ''), symbol, side,
		       requested_qty, COALESCE(requested_price, 0)
		FROM order_intents ` + where + " ORDER BY time DESC, intent_id DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query order_intents: %w", err)
	}
	defer rows.Close()

	out := []OrderIntent{}
	for rows.Next() {
		var item OrderIntent
		if err := rows.Scan(
			&item.Time, &item.IntentID, &item.AccountID, &item.UserID, &item.StrategyID,
			&item.SessionID, &item.Market, &item.Symbol, &item.Side,
			&item.RequestedQty, &item.RequestedPrice,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrderAttemptsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID string, limit, offset int) ([]OrderAttempt, int64, error) {
	where, args, err := buildScopedWhere(userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "intent_id", val: intentID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_attempts "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count order_attempts: %w", err)
	}
	query := `
		SELECT time, attempt_id, intent_id, account_id, user_id, symbol, side, requested_qty,
		       COALESCE(requested_price, 0), mark_price, status, mode, error_message,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, ''),
		       COALESCE(client_order_id, ''), COALESCE(recovery_error, ''), COALESCE(order_id, ''), COALESCE(exchange_order_id, '')
		FROM order_attempts ` + where + " ORDER BY time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query order_attempts: %w", err)
	}
	defer rows.Close()

	out := []OrderAttempt{}
	for rows.Next() {
		var item OrderAttempt
		if err := rows.Scan(
			&item.Time, &item.AttemptID, &item.IntentID, &item.AccountID, &item.UserID, &item.Symbol, &item.Side,
			&item.RequestedQty, &item.RequestedPrice, &item.MarkPrice, &item.Status, &item.Mode, &item.ErrorMessage,
			&item.StrategyID, &item.Market, &item.SessionID, &item.ClientOrderID, &item.RecoveryError, &item.OrderID, &item.ExchangeOrderID,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrdersPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID string, limit, offset int) ([]Order, int64, error) {
	where, args, err := buildScopedWhere(userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "intent_id", val: intentID},
		{col: "attempt_id", val: attemptID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count orders: %w", err)
	}
	query := `
		SELECT time, order_id, COALESCE(exchange_order_id, ''), COALESCE(client_order_id, ''), attempt_id, intent_id, account_id, user_id,
		       symbol, side, orig_qty, executed_qty, remaining_qty, avg_price, status, mode, error_message,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, ''), COALESCE(price, 0)
		FROM orders ` + where + " ORDER BY time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	out := []Order{}
	for rows.Next() {
		var item Order
		if err := rows.Scan(
			&item.Time, &item.OrderID, &item.ExchangeOrderID, &item.ClientOrderID, &item.AttemptID, &item.IntentID, &item.AccountID, &item.UserID,
			&item.Symbol, &item.Side, &item.OrigQty, &item.ExecutedQty, &item.RemainingQty, &item.AvgPrice,
			&item.Status, &item.Mode, &item.ErrorMessage, &item.StrategyID, &item.Market, &item.SessionID, &item.Price,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrderFillsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID, orderID string, limit, offset int) ([]OrderFill, int64, error) {
	where, args, err := buildScopedWhere(userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "intent_id", val: intentID},
		{col: "attempt_id", val: attemptID},
		{col: "order_id", val: orderID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_fills "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count order_fills: %w", err)
	}
	query := `
		SELECT time, fill_id, COALESCE(exchange_trade_id, ''), order_id, COALESCE(exchange_order_id, ''),
		       attempt_id, intent_id, account_id, user_id, symbol, side, qty, fill_price, fee, status, mode,
		       COALESCE(strategy_id, 0), COALESCE(market, ''), COALESCE(session_id, '')
		FROM order_fills ` + where + " ORDER BY time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query order_fills: %w", err)
	}
	defer rows.Close()

	out := []OrderFill{}
	for rows.Next() {
		var item OrderFill
		if err := rows.Scan(
			&item.Time, &item.FillID, &item.ExchangeTradeID, &item.OrderID, &item.ExchangeOrderID, &item.AttemptID,
			&item.IntentID, &item.AccountID, &item.UserID, &item.Symbol, &item.Side, &item.Qty, &item.FillPrice,
			&item.Fee, &item.Status, &item.Mode, &item.StrategyID, &item.Market, &item.SessionID,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableFloat64(v float64) any {
	if v == 0 {
		return nil
	}
	return v
}
