package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/hushine-tech/core-service/internal/order/lifecycle"
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

const (
	orderSideBuy  int16 = 1
	orderSideSell int16 = 2

	orderTypeMarket int16 = 1
	orderTypeLimit  int16 = 2

	intentStatusRequested int16 = 1
	intentStatusRejected  int16 = 2

	attemptStatusPending        int16 = 1
	attemptStatusFailed         int16 = 2
	attemptStatusAccepted       int16 = 3
	attemptStatusUnknown        int16 = 4
	attemptStatusRecovering     int16 = 5
	attemptStatusRecovered      int16 = 6
	attemptStatusRecoveryFailed int16 = 7
	attemptStatusRiskRejected   int16 = 8

	orderStatusNew             int16 = 1
	orderStatusPartiallyFilled int16 = 2
	orderStatusFilled          int16 = 3
	orderStatusCanceled        int16 = 4
	orderStatusFailed          int16 = 5
	orderStatusExpired         int16 = 6

	fillStatusFilled     int16 = 1
	fillStatusFeeMissing int16 = 2
)

func (r *TimescaleRepository) UpsertOrderIntent(ctx context.Context, intent OrderIntent) error {
	sideCode, err := orderSideCode(intent.Side)
	if err != nil {
		return err
	}
	orderType := int16(intent.OrderType)
	if orderType == 0 {
		orderType = orderTypeMarket
	}
	statusCode := intentStatusCode(intent.Status)
	_, err = r.sqlExec.ExecContext(ctx, `
		INSERT INTO order_intents (
			intent_id, time, updated_at, account_id, venue_id, user_id, strategy_id, session_id,
			environment, exchange, market, symbol, side, position_side, order_type,
			requested_qty, requested_price, post_only, good_till_date, reduce_only,
			status, reject_code, reject_message
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
		ON CONFLICT (intent_id) DO UPDATE SET
			updated_at = EXCLUDED.updated_at,
			account_id = EXCLUDED.account_id,
			venue_id = EXCLUDED.venue_id,
			user_id = EXCLUDED.user_id,
			strategy_id = EXCLUDED.strategy_id,
			session_id = EXCLUDED.session_id,
			environment = EXCLUDED.environment,
			exchange = EXCLUDED.exchange,
			market = EXCLUDED.market,
			symbol = EXCLUDED.symbol,
			side = EXCLUDED.side,
			position_side = EXCLUDED.position_side,
			order_type = EXCLUDED.order_type,
			requested_qty = EXCLUDED.requested_qty,
			requested_price = EXCLUDED.requested_price,
			post_only = EXCLUDED.post_only,
			good_till_date = EXCLUDED.good_till_date,
			reduce_only = EXCLUDED.reduce_only,
			status = EXCLUDED.status,
			reject_code = EXCLUDED.reject_code,
			reject_message = EXCLUDED.reject_message`,
		intent.IntentID, intent.Time, intent.Time, intent.AccountID, intent.VenueID, intent.UserID, nullableInt64(intent.StrategyID),
		nullableString(intent.SessionID), int16(intent.Environment), int16(intent.Exchange), int16(intent.Market),
		intent.Symbol, sideCode, int16(intent.PositionSide), orderType,
		intent.RequestedQty, nullableFloat64(intent.RequestedPrice), intent.PostOnly, nullableTimePtr(intent.GoodTillDate), intent.ReduceOnly, statusCode,
		intent.RejectCode, intent.RejectMessage,
	)
	return err
}

func (r *TimescaleRepository) CreateOrderAttempt(ctx context.Context, attempt OrderAttempt) error {
	_, err := r.sqlExec.ExecContext(ctx, `
			INSERT INTO order_attempts (
				attempt_id, intent_id, time, updated_at, mark_price, client_order_id, status,
				error_message, exchange_order_id, recovery_error, post_only, good_till_date,
				reduce_only, risk_status, risk_reasons_json
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)`,
		attempt.AttemptID, attempt.IntentID, attempt.Time, attempt.Time,
		nullableFloat64(attempt.MarkPrice), nullableString(attempt.ClientOrderID),
		attemptStatusCode(attempt.Status), attempt.ErrorMessage,
		nullableString(attempt.ExchangeOrderID), attempt.RecoveryError,
		attempt.PostOnly, nullableTimePtr(attempt.GoodTillDate), attempt.ReduceOnly,
		attempt.RiskStatus, riskReasonsJSON(attempt.RiskReasonsJSON),
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
			    exchange_order_id = $7,
			    post_only = $8,
			    good_till_date = $9,
			    reduce_only = $10,
			    risk_status = $11,
			    risk_reasons_json = $12::jsonb
			WHERE attempt_id = $1`,
		attempt.AttemptID, attempt.Time, attemptStatusCode(attempt.Status), attempt.ErrorMessage,
		nullableString(attempt.ClientOrderID), attempt.RecoveryError,
		nullableString(attempt.ExchangeOrderID),
		attempt.PostOnly, nullableTimePtr(attempt.GoodTillDate), attempt.ReduceOnly,
		attempt.RiskStatus, riskReasonsJSON(attempt.RiskReasonsJSON),
	); err != nil {
		return fmt.Errorf("update order_attempts: %w", err)
	}

	if order != nil {
		if _, err = tx.ExecContext(ctx, `
				INSERT INTO orders (
					order_id, exchange_order_id, attempt_id, intent_id, time, updated_at,
					client_order_id, orig_qty, executed_qty, remaining_qty, avg_price, price,
					post_only, good_till_date, reduce_only, status, error_message,
					recovery_status, recovery_started_at, next_check_at, recovery_deadline_at,
					last_recovery_error, force_closed_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
				ON CONFLICT (order_id) DO UPDATE SET
					updated_at = EXCLUDED.updated_at,
					exchange_order_id = EXCLUDED.exchange_order_id,
					client_order_id = EXCLUDED.client_order_id,
					executed_qty = EXCLUDED.executed_qty,
					remaining_qty = EXCLUDED.remaining_qty,
					avg_price = EXCLUDED.avg_price,
					price = EXCLUDED.price,
					post_only = EXCLUDED.post_only,
					good_till_date = EXCLUDED.good_till_date,
					reduce_only = EXCLUDED.reduce_only,
					status = EXCLUDED.status,
					error_message = EXCLUDED.error_message,
					recovery_status = EXCLUDED.recovery_status,
					recovery_started_at = EXCLUDED.recovery_started_at,
					next_check_at = EXCLUDED.next_check_at,
					recovery_deadline_at = EXCLUDED.recovery_deadline_at,
					last_recovery_error = EXCLUDED.last_recovery_error,
					force_closed_at = EXCLUDED.force_closed_at`,
			order.OrderID, nullableString(order.ExchangeOrderID), order.AttemptID, order.IntentID,
			order.Time, order.Time, nullableString(order.ClientOrderID),
			order.OrigQty, order.ExecutedQty, order.RemainingQty, order.AvgPrice,
			nullableFloat64(order.Price), order.PostOnly, nullableTimePtr(order.GoodTillDate), order.ReduceOnly,
			orderStatusCode(order.Status), order.ErrorMessage,
			order.RecoveryStatus, nullableTimePtr(order.RecoveryStartedAt), nullableTimePtr(order.NextCheckAt),
			nullableTimePtr(order.RecoveryDeadlineAt), order.LastRecoveryError, nullableTimePtr(order.ForceClosedAt),
		); err != nil {
			return fmt.Errorf("upsert orders: %w", err)
		}
	}

	for _, fill := range fills {
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO order_fills (
				time, fill_id, exchange_trade_id, order_id, exchange_order_id, attempt_id, intent_id,
				qty, fill_price, fee, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			fill.Time, fill.FillID, nullableString(fill.ExchangeTradeID), fill.OrderID,
			nullableString(fill.ExchangeOrderID), fill.AttemptID, fill.IntentID,
			fill.Qty, fill.FillPrice, fill.Fee, fillStatusCode(fill.Status),
		); err != nil {
			return fmt.Errorf("insert order_fills: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize attempt: %w", err)
	}
	return nil
}

func (r *TimescaleRepository) FinalizeRiskRejectedAttempt(ctx context.Context, intent OrderIntent, attempt OrderAttempt) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize risk rejected attempt: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
			UPDATE order_intents
			SET updated_at = $2,
			    status = $3,
			    reject_code = $4,
			    reject_message = $5,
			    post_only = $6,
			    good_till_date = $7,
			    reduce_only = $8
			WHERE intent_id = $1`,
		intent.IntentID, attempt.Time, intentStatusCode(intent.Status), intent.RejectCode, intent.RejectMessage,
		intent.PostOnly, nullableTimePtr(intent.GoodTillDate), intent.ReduceOnly,
	); err != nil {
		return fmt.Errorf("update risk rejected order_intents: %w", err)
	}

	if _, err = tx.ExecContext(ctx, `
			UPDATE order_attempts
			SET updated_at = $2,
			    status = $3,
			    error_message = $4,
			    client_order_id = $5,
			    recovery_error = $6,
			    exchange_order_id = $7,
			    post_only = $8,
			    good_till_date = $9,
			    reduce_only = $10,
			    risk_status = $11,
			    risk_reasons_json = $12::jsonb
			WHERE attempt_id = $1`,
		attempt.AttemptID, attempt.Time, attemptStatusCode(attempt.Status), attempt.ErrorMessage,
		nullableString(attempt.ClientOrderID), attempt.RecoveryError,
		nullableString(attempt.ExchangeOrderID),
		attempt.PostOnly, nullableTimePtr(attempt.GoodTillDate), attempt.ReduceOnly,
		attempt.RiskStatus, riskReasonsJSON(attempt.RiskReasonsJSON),
	); err != nil {
		return fmt.Errorf("update risk rejected order_attempts: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize risk rejected attempt: %w", err)
	}
	return nil
}

func (r *TimescaleRepository) FindOrderAttempt(ctx context.Context, userID, accountID int64, intentID, attemptID, clientOrderID string) (OrderAttempt, error) {
	base := `
		SELECT a.time, a.attempt_id, a.intent_id, i.account_id, i.venue_id, i.user_id,
		       i.symbol, i.side, i.requested_qty, COALESCE(i.requested_price, 0),
		       COALESCE(a.mark_price, 0), a.status, a.error_message,
		       COALESCE(i.strategy_id, 0), COALESCE(i.session_id, ''),
		       COALESCE(a.client_order_id, ''), COALESCE(a.recovery_error, ''),
		       COALESCE(o.order_id, ''), COALESCE(a.exchange_order_id, ''),
		       i.environment, i.exchange, i.market, i.position_side, i.order_type,
		       a.post_only, a.good_till_date, a.reduce_only, a.risk_status, a.risk_reasons_json::text
		FROM order_attempts a
		JOIN order_intents i ON i.intent_id = a.intent_id
		LEFT JOIN orders o ON o.attempt_id = a.attempt_id
		WHERE i.account_id = $1`
	args := []any{accountID}
	if userID > 0 {
		args = append(args, userID)
		base += fmt.Sprintf(" AND i.user_id = $%d", len(args))
	}
	if strings.TrimSpace(attemptID) != "" {
		args = append(args, attemptID)
		base += fmt.Sprintf(" AND a.attempt_id = $%d", len(args))
	} else if strings.TrimSpace(clientOrderID) != "" {
		args = append(args, clientOrderID)
		base += fmt.Sprintf(" AND a.client_order_id = $%d", len(args))
	} else if strings.TrimSpace(intentID) != "" {
		args = append(args, intentID)
		base += fmt.Sprintf(" AND a.intent_id = $%d", len(args))
	} else {
		return OrderAttempt{}, fmt.Errorf("intent_id, attempt_id, or client_order_id is required")
	}
	base += " ORDER BY a.time DESC LIMIT 1"

	var item OrderAttempt
	var sideCode, statusCode, env, exchange, market, positionSide, orderType int16
	var goodTillDate sql.NullTime
	err := r.db.QueryRowContext(ctx, base, args...).Scan(
		&item.Time, &item.AttemptID, &item.IntentID, &item.AccountID, &item.VenueID, &item.UserID, &item.Symbol, &sideCode,
		&item.RequestedQty, &item.RequestedPrice, &item.MarkPrice, &statusCode, &item.ErrorMessage,
		&item.StrategyID, &item.SessionID, &item.ClientOrderID, &item.RecoveryError, &item.OrderID, &item.ExchangeOrderID,
		&env, &exchange, &market, &positionSide, &orderType,
		&item.PostOnly, &goodTillDate, &item.ReduceOnly, &item.RiskStatus, &item.RiskReasonsJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return OrderAttempt{}, ErrNotFound
	}
	if err != nil {
		return OrderAttempt{}, fmt.Errorf("find order attempt: %w", err)
	}
	item.Side = orderSideText(sideCode)
	item.Status = attemptStatusText(statusCode)
	item.Environment = int32(env)
	item.Exchange = int32(exchange)
	item.Market = int32(market)
	item.PositionSide = int32(positionSide)
	item.OrderType = int32(orderType)
	item.GoodTillDate = timePtrFromNull(goodTillDate)
	return item, nil
}

func (r *TimescaleRepository) FindOrderByAttempt(ctx context.Context, attemptID string) (Order, error) {
	query := `
		SELECT o.time, o.order_id, COALESCE(o.exchange_order_id, ''), COALESCE(o.client_order_id, ''),
		       o.attempt_id, o.intent_id, i.account_id, i.venue_id, i.user_id,
		       i.symbol, i.side, o.orig_qty, o.executed_qty, o.remaining_qty, o.avg_price,
		       o.status, o.error_message, COALESCE(i.strategy_id, 0), i.environment,
		       i.exchange, i.market, i.position_side, COALESCE(i.session_id, ''), COALESCE(o.price, 0),
		       o.post_only, o.good_till_date, o.reduce_only, o.recovery_status, o.recovery_started_at,
		       o.next_check_at, o.recovery_deadline_at, o.last_recovery_error, o.force_closed_at
		FROM orders o
		JOIN order_intents i ON i.intent_id = o.intent_id
		WHERE o.attempt_id = $1
		LIMIT 1`
	var item Order
	var sideCode, statusCode, env, exchange, market, positionSide int16
	var goodTillDate, recoveryStartedAt, nextCheckAt, recoveryDeadlineAt, forceClosedAt sql.NullTime
	err := r.db.QueryRowContext(ctx, query, attemptID).Scan(
		&item.Time, &item.OrderID, &item.ExchangeOrderID, &item.ClientOrderID, &item.AttemptID, &item.IntentID, &item.AccountID, &item.VenueID, &item.UserID,
		&item.Symbol, &sideCode, &item.OrigQty, &item.ExecutedQty, &item.RemainingQty, &item.AvgPrice, &statusCode, &item.ErrorMessage,
		&item.StrategyID, &env, &exchange, &market, &positionSide, &item.SessionID, &item.Price,
		&item.PostOnly, &goodTillDate, &item.ReduceOnly, &item.RecoveryStatus, &recoveryStartedAt,
		&nextCheckAt, &recoveryDeadlineAt, &item.LastRecoveryError, &forceClosedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("find order by attempt: %w", err)
	}
	item.Side = orderSideText(sideCode)
	item.Status = orderStatusText(statusCode)
	item.Environment = int32(env)
	item.Exchange = int32(exchange)
	item.Market = int32(market)
	item.PositionSide = int32(positionSide)
	item.GoodTillDate = timePtrFromNull(goodTillDate)
	item.RecoveryStartedAt = timePtrFromNull(recoveryStartedAt)
	item.NextCheckAt = timePtrFromNull(nextCheckAt)
	item.RecoveryDeadlineAt = timePtrFromNull(recoveryDeadlineAt)
	item.ForceClosedAt = timePtrFromNull(forceClosedAt)
	return item, nil
}

func (r *TimescaleRepository) ListOrderFillsByAttempt(ctx context.Context, attemptID string) ([]OrderFill, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT f.time, f.fill_id, COALESCE(f.exchange_trade_id, ''), f.order_id, COALESCE(f.exchange_order_id, ''),
		       f.attempt_id, f.intent_id, i.account_id, i.venue_id, i.user_id, i.symbol, i.side,
		       f.qty, f.fill_price, f.fee, f.status, COALESCE(i.strategy_id, 0),
		       i.environment, i.exchange, i.market, i.position_side, COALESCE(i.session_id, '')
		FROM order_fills f
		JOIN order_intents i ON i.intent_id = f.intent_id
		WHERE f.attempt_id = $1
		ORDER BY f.time ASC, f.fill_id ASC`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("list order fills by attempt: %w", err)
	}
	defer rows.Close()
	out := []OrderFill{}
	for rows.Next() {
		var item OrderFill
		var sideCode, statusCode, env, exchange, market, positionSide int16
		if err := rows.Scan(
			&item.Time, &item.FillID, &item.ExchangeTradeID, &item.OrderID, &item.ExchangeOrderID, &item.AttemptID,
			&item.IntentID, &item.AccountID, &item.VenueID, &item.UserID, &item.Symbol, &sideCode, &item.Qty, &item.FillPrice,
			&item.Fee, &statusCode, &item.StrategyID, &env, &exchange, &market, &positionSide, &item.SessionID,
		); err != nil {
			return nil, err
		}
		item.Side = orderSideText(sideCode)
		item.Status = fillStatusText(statusCode)
		item.Environment = int32(env)
		item.Exchange = int32(exchange)
		item.Market = int32(market)
		item.PositionSide = int32(positionSide)
		out = append(out, item)
	}
	return out, rows.Err()
}

func buildScopedWhere(userID, accountID, strategyID int64, sessionID string) (string, []any, error) {
	return buildIntentScopedWhere("", userID, accountID, strategyID, sessionID)
}

func buildIntentScopedWhere(alias string, userID, accountID, strategyID int64, sessionID string) (string, []any, error) {
	if userID <= 0 {
		return "", nil, fmt.Errorf("userID is required")
	}
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	where := "WHERE " + prefix + "user_id = $1"
	args := []any{userID}
	if sessionID != "" {
		args = append(args, sessionID)
		where += fmt.Sprintf(" AND %ssession_id = $%d", prefix, len(args))
	} else if accountID > 0 {
		args = append(args, accountID)
		where += fmt.Sprintf(" AND %saccount_id = $%d", prefix, len(args))
	}
	if strategyID > 0 {
		args = append(args, strategyID)
		where += fmt.Sprintf(" AND %sstrategy_id = $%d", prefix, len(args))
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
		SELECT time, intent_id, account_id, venue_id, user_id, COALESCE(strategy_id, 0),
		       COALESCE(session_id, ''), environment, exchange, market, symbol, side,
		       position_side, order_type, requested_qty, COALESCE(requested_price, 0),
		       post_only, good_till_date, reduce_only, status, reject_code, reject_message
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
		var sideCode, statusCode, env, exchange, market, positionSide, orderType int16
		var goodTillDate sql.NullTime
		if err := rows.Scan(
			&item.Time, &item.IntentID, &item.AccountID, &item.VenueID, &item.UserID, &item.StrategyID,
			&item.SessionID, &env, &exchange, &market, &item.Symbol, &sideCode,
			&positionSide, &orderType, &item.RequestedQty, &item.RequestedPrice,
			&item.PostOnly, &goodTillDate, &item.ReduceOnly, &statusCode, &item.RejectCode, &item.RejectMessage,
		); err != nil {
			return nil, 0, err
		}
		item.Environment = int32(env)
		item.Exchange = int32(exchange)
		item.Market = int32(market)
		item.Side = orderSideText(sideCode)
		item.PositionSide = int32(positionSide)
		item.OrderType = int32(orderType)
		item.GoodTillDate = timePtrFromNull(goodTillDate)
		item.Status = intentStatusText(statusCode)
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrderAttemptsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID string, limit, offset int) ([]OrderAttempt, int64, error) {
	where, args, err := buildIntentScopedWhere("i", userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "a.intent_id", val: intentID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_attempts a JOIN order_intents i ON i.intent_id = a.intent_id "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count order_attempts: %w", err)
	}
	query := `
		SELECT a.time, a.attempt_id, a.intent_id, i.account_id, i.venue_id, i.user_id, i.symbol, i.side, i.requested_qty,
		       COALESCE(i.requested_price, 0), COALESCE(a.mark_price, 0), a.status, a.error_message,
		       COALESCE(i.strategy_id, 0), i.environment, i.exchange, i.market, i.position_side, i.order_type,
		       COALESCE(i.session_id, ''), COALESCE(a.client_order_id, ''), COALESCE(a.recovery_error, ''),
		       COALESCE(o.order_id, ''), COALESCE(a.exchange_order_id, ''),
		       a.post_only, a.good_till_date, a.reduce_only, a.risk_status, a.risk_reasons_json::text
		FROM order_attempts a
		JOIN order_intents i ON i.intent_id = a.intent_id
		LEFT JOIN orders o ON o.attempt_id = a.attempt_id ` + where + " ORDER BY a.time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query order_attempts: %w", err)
	}
	defer rows.Close()

	out := []OrderAttempt{}
	for rows.Next() {
		var item OrderAttempt
		var sideCode, statusCode, env, exchange, market, positionSide, orderType int16
		var goodTillDate sql.NullTime
		if err := rows.Scan(
			&item.Time, &item.AttemptID, &item.IntentID, &item.AccountID, &item.VenueID, &item.UserID, &item.Symbol, &sideCode,
			&item.RequestedQty, &item.RequestedPrice, &item.MarkPrice, &statusCode, &item.ErrorMessage,
			&item.StrategyID, &env, &exchange, &market, &positionSide, &orderType,
			&item.SessionID, &item.ClientOrderID, &item.RecoveryError, &item.OrderID, &item.ExchangeOrderID,
			&item.PostOnly, &goodTillDate, &item.ReduceOnly, &item.RiskStatus, &item.RiskReasonsJSON,
		); err != nil {
			return nil, 0, err
		}
		item.Side = orderSideText(sideCode)
		item.Status = attemptStatusText(statusCode)
		item.Environment = int32(env)
		item.Exchange = int32(exchange)
		item.Market = int32(market)
		item.PositionSide = int32(positionSide)
		item.OrderType = int32(orderType)
		item.GoodTillDate = timePtrFromNull(goodTillDate)
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrdersPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID string, limit, offset int) ([]Order, int64, error) {
	where, args, err := buildIntentScopedWhere("i", userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "o.intent_id", val: intentID},
		{col: "o.attempt_id", val: attemptID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders o JOIN order_intents i ON i.intent_id = o.intent_id "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count orders: %w", err)
	}
	query := `
		SELECT o.time, o.order_id, COALESCE(o.exchange_order_id, ''), COALESCE(o.client_order_id, ''),
		       o.attempt_id, o.intent_id, i.account_id, i.venue_id, i.user_id,
		       i.symbol, i.side, o.orig_qty, o.executed_qty, o.remaining_qty, o.avg_price,
		       o.status, o.error_message, COALESCE(i.strategy_id, 0), i.environment, i.exchange, i.market,
		       i.position_side, COALESCE(i.session_id, ''), COALESCE(o.price, 0),
		       o.post_only, o.good_till_date, o.reduce_only, o.recovery_status, o.recovery_started_at,
		       o.next_check_at, o.recovery_deadline_at, o.last_recovery_error, o.force_closed_at
		FROM orders o
		JOIN order_intents i ON i.intent_id = o.intent_id ` + where + " ORDER BY o.time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	out := []Order{}
	for rows.Next() {
		var item Order
		var sideCode, statusCode, env, exchange, market, positionSide int16
		var goodTillDate, recoveryStartedAt, nextCheckAt, recoveryDeadlineAt, forceClosedAt sql.NullTime
		if err := rows.Scan(
			&item.Time, &item.OrderID, &item.ExchangeOrderID, &item.ClientOrderID, &item.AttemptID, &item.IntentID, &item.AccountID, &item.VenueID, &item.UserID,
			&item.Symbol, &sideCode, &item.OrigQty, &item.ExecutedQty, &item.RemainingQty, &item.AvgPrice,
			&statusCode, &item.ErrorMessage, &item.StrategyID, &env, &exchange, &market, &positionSide, &item.SessionID, &item.Price,
			&item.PostOnly, &goodTillDate, &item.ReduceOnly, &item.RecoveryStatus, &recoveryStartedAt,
			&nextCheckAt, &recoveryDeadlineAt, &item.LastRecoveryError, &forceClosedAt,
		); err != nil {
			return nil, 0, err
		}
		item.Side = orderSideText(sideCode)
		item.Status = orderStatusText(statusCode)
		item.Environment = int32(env)
		item.Exchange = int32(exchange)
		item.Market = int32(market)
		item.PositionSide = int32(positionSide)
		item.GoodTillDate = timePtrFromNull(goodTillDate)
		item.RecoveryStartedAt = timePtrFromNull(recoveryStartedAt)
		item.NextCheckAt = timePtrFromNull(nextCheckAt)
		item.RecoveryDeadlineAt = timePtrFromNull(recoveryDeadlineAt)
		item.ForceClosedAt = timePtrFromNull(forceClosedAt)
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) QueryOrderFillsPaginated(ctx context.Context, userID, accountID, strategyID int64, sessionID, intentID, attemptID, orderID string, limit, offset int) ([]OrderFill, int64, error) {
	where, args, err := buildIntentScopedWhere("i", userID, accountID, strategyID, sessionID)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendAncestorFilters(where, args, []ancestorFilter{
		{col: "f.intent_id", val: intentID},
		{col: "f.attempt_id", val: attemptID},
		{col: "f.order_id", val: orderID},
	})
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_fills f JOIN order_intents i ON i.intent_id = f.intent_id "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count order_fills: %w", err)
	}
	query := `
		SELECT f.time, f.fill_id, COALESCE(f.exchange_trade_id, ''), f.order_id, COALESCE(f.exchange_order_id, ''),
		       f.attempt_id, f.intent_id, i.account_id, i.venue_id, i.user_id,
		       i.symbol, i.side, f.qty, f.fill_price, f.fee, f.status,
		       COALESCE(i.strategy_id, 0), i.environment, i.exchange, i.market, i.position_side,
		       COALESCE(i.session_id, '')
		FROM order_fills f
		JOIN order_intents i ON i.intent_id = f.intent_id ` + where + " ORDER BY f.time DESC"
	query, pageArgs := applyLimitOffset(query, args, limit, offset)
	rows, err := r.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query order_fills: %w", err)
	}
	defer rows.Close()

	out := []OrderFill{}
	for rows.Next() {
		var item OrderFill
		var sideCode, statusCode, env, exchange, market, positionSide int16
		if err := rows.Scan(
			&item.Time, &item.FillID, &item.ExchangeTradeID, &item.OrderID, &item.ExchangeOrderID, &item.AttemptID,
			&item.IntentID, &item.AccountID, &item.VenueID, &item.UserID, &item.Symbol, &sideCode, &item.Qty, &item.FillPrice,
			&item.Fee, &statusCode, &item.StrategyID, &env, &exchange, &market, &positionSide, &item.SessionID,
		); err != nil {
			return nil, 0, err
		}
		item.Side = orderSideText(sideCode)
		item.Status = fillStatusText(statusCode)
		item.Environment = int32(env)
		item.Exchange = int32(exchange)
		item.Market = int32(market)
		item.PositionSide = int32(positionSide)
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (r *TimescaleRepository) ListOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error) {
	return r.listOpenOrders(ctx, limit, false)
}

func (r *TimescaleRepository) ListDueOpenOrders(ctx context.Context, limit int) ([]lifecycle.OpenOrder, error) {
	return r.listOpenOrders(ctx, limit, true)
}

func (r *TimescaleRepository) ResolveOpenOrderByExchangeRef(ctx context.Context, venueID int64, exchangeOrderID, clientOrderID string) (lifecycle.OpenOrder, error) {
	exchangeOrderID = strings.TrimSpace(exchangeOrderID)
	clientOrderID = strings.TrimSpace(clientOrderID)
	if venueID <= 0 || (exchangeOrderID == "" && clientOrderID == "") {
		return lifecycle.OpenOrder{}, lifecycle.ErrOpenOrderNotFound
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT COALESCE(i.session_id, ''), i.account_id, i.venue_id, i.intent_id,
		       i.environment, i.exchange, i.market, i.position_side,
		       o.attempt_id, o.order_id, COALESCE(o.exchange_order_id, ''),
		       COALESCE(NULLIF(o.client_order_id, ''), NULLIF(a.client_order_id, ''), ''),
		       i.symbol, i.side,
		       COALESCE(NULLIF(o.recovery_status, ''), CASE
		           WHEN o.status = $1 THEN 'OPEN'
		           WHEN o.status = $2 THEN 'PARTIALLY_FILLED'
		           ELSE ''
		       END) AS recovery_status,
		       o.recovery_started_at, o.next_check_at, o.recovery_deadline_at,
		       COALESCE(o.last_recovery_error, '')
		FROM orders o
		JOIN order_intents i ON i.intent_id = o.intent_id
		JOIN order_attempts a ON a.attempt_id = o.attempt_id
		WHERE COALESCE(NULLIF(o.recovery_status, ''), CASE
		           WHEN o.status = $1 THEN 'OPEN'
		           WHEN o.status = $2 THEN 'PARTIALLY_FILLED'
		           ELSE ''
		       END) IN ('OPEN', 'PARTIALLY_FILLED', 'FILL_PENDING', 'FEE_MISSING', 'RECOVERING')
		  AND i.venue_id = $3
		  AND (
		      ($4 <> '' AND COALESCE(o.exchange_order_id, '') = $4)
		      OR ($5 <> '' AND COALESCE(NULLIF(o.client_order_id, ''), NULLIF(a.client_order_id, ''), '') = $5)
		  )
		ORDER BY o.updated_at ASC, o.time ASC, o.order_id ASC
		LIMIT 1`,
		orderStatusNew, orderStatusPartiallyFilled, venueID, exchangeOrderID, clientOrderID,
	)
	item, err := scanOpenOrder(row)
	if errors.Is(err, sql.ErrNoRows) {
		return lifecycle.OpenOrder{}, lifecycle.ErrOpenOrderNotFound
	}
	if err != nil {
		return lifecycle.OpenOrder{}, fmt.Errorf("resolve open order by exchange ref: %w", err)
	}
	return item, nil
}

func (r *TimescaleRepository) listOpenOrders(ctx context.Context, limit int, dueOnly bool) ([]lifecycle.OpenOrder, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	dueFilter := ""
	if dueOnly {
		dueFilter = "AND (o.next_check_at IS NULL OR o.next_check_at <= NOW())"
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT COALESCE(i.session_id, ''), i.account_id, i.venue_id, i.intent_id,
		       i.environment, i.exchange, i.market, i.position_side,
		       o.attempt_id, o.order_id, COALESCE(o.exchange_order_id, ''),
		       COALESCE(NULLIF(o.client_order_id, ''), NULLIF(a.client_order_id, ''), ''),
		       i.symbol, i.side,
		       COALESCE(NULLIF(o.recovery_status, ''), CASE
		           WHEN o.status = $1 THEN 'OPEN'
		           WHEN o.status = $2 THEN 'PARTIALLY_FILLED'
		           ELSE ''
		       END) AS recovery_status,
		       o.recovery_started_at, o.next_check_at, o.recovery_deadline_at,
		       COALESCE(o.last_recovery_error, '')
		FROM orders o
		JOIN order_intents i ON i.intent_id = o.intent_id
		JOIN order_attempts a ON a.attempt_id = o.attempt_id
		WHERE COALESCE(NULLIF(o.recovery_status, ''), CASE
		           WHEN o.status = $1 THEN 'OPEN'
		           WHEN o.status = $2 THEN 'PARTIALLY_FILLED'
			           ELSE ''
			       END) IN ('OPEN', 'PARTIALLY_FILLED', 'FILL_PENDING', 'FEE_MISSING', 'RECOVERING')
		  `+dueFilter+`
		  AND i.venue_id > 0
		  AND (
		      COALESCE(o.exchange_order_id, '') <> ''
		      OR COALESCE(o.client_order_id, '') <> ''
		      OR COALESCE(a.client_order_id, '') <> ''
		  )
		ORDER BY o.updated_at ASC, o.time ASC, o.order_id ASC
		LIMIT $3`,
		orderStatusNew, orderStatusPartiallyFilled, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list open orders: %w", err)
	}
	defer rows.Close()

	out := make([]lifecycle.OpenOrder, 0)
	for rows.Next() {
		item, err := scanOpenOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

type openOrderScanner interface {
	Scan(dest ...any) error
}

func scanOpenOrder(scanner openOrderScanner) (lifecycle.OpenOrder, error) {
	var item lifecycle.OpenOrder
	var sideCode int16
	var recoveryStartedAt, nextCheckAt, recoveryDeadlineAt sql.NullTime
	if err := scanner.Scan(
		&item.SessionID,
		&item.AccountID,
		&item.VenueID,
		&item.IntentID,
		&item.Environment,
		&item.Exchange,
		&item.Market,
		&item.PositionSide,
		&item.AttemptID,
		&item.OrderID,
		&item.ExchangeOrderID,
		&item.ClientOrderID,
		&item.Symbol,
		&sideCode,
		&item.RecoveryStatus,
		&recoveryStartedAt,
		&nextCheckAt,
		&recoveryDeadlineAt,
		&item.LastRecoveryError,
	); err != nil {
		return lifecycle.OpenOrder{}, err
	}
	item.Side = orderSideText(sideCode)
	if recoveryStartedAt.Valid {
		item.RecoveryStartedAt = recoveryStartedAt.Time
	}
	if nextCheckAt.Valid {
		item.NextCheckAt = nextCheckAt.Time
	}
	if recoveryDeadlineAt.Valid {
		item.RecoveryDeadlineAt = recoveryDeadlineAt.Time
	}
	return item, nil
}

func (r *TimescaleRepository) MarkRecoveryExpired(ctx context.Context, orderID string, forceClosedAt time.Time, lastError string) error {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if forceClosedAt.IsZero() {
		forceClosedAt = time.Now().UTC()
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE orders
		SET recovery_status = 'RECOVERY_EXPIRED',
		    next_check_at = NULL,
		    force_closed_at = $2,
		    last_recovery_error = $3,
		    updated_at = $2
		WHERE order_id = $1`,
		orderID, forceClosedAt.UTC(), strings.TrimSpace(lastError),
	)
	if err != nil {
		return fmt.Errorf("mark recovery expired: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) MarkRecoveryResolved(ctx context.Context, orderID string, state lifecycle.OrderState, resolvedAt time.Time) error {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return fmt.Errorf("order_id is required")
	}
	status := strings.ToUpper(strings.TrimSpace(state.Status))
	if status == "" && state.OrigQty > 0 && state.RemainingQty <= 0 {
		status = "FILLED"
	}
	if status == "" {
		return fmt.Errorf("order status is required")
	}
	if resolvedAt.IsZero() {
		resolvedAt = state.UpdatedAt
	}
	if resolvedAt.IsZero() {
		resolvedAt = time.Now().UTC()
	}
	hasQtyState := state.OrigQty > 0
	hasAvgPrice := state.AvgPrice > 0
	res, err := r.db.ExecContext(ctx, `
		UPDATE orders
		SET updated_at = $2,
		    status = $3,
		    exchange_order_id = COALESCE(NULLIF($4, ''), exchange_order_id),
		    client_order_id = COALESCE(NULLIF($5, ''), client_order_id),
		    orig_qty = CASE WHEN $6 THEN $7 ELSE orig_qty END,
		    executed_qty = CASE WHEN $6 THEN $8 ELSE executed_qty END,
		    remaining_qty = CASE WHEN $6 THEN $9 ELSE remaining_qty END,
		    avg_price = CASE WHEN $10 THEN $11 ELSE avg_price END,
		    recovery_status = $12,
		    next_check_at = NULL,
		    last_recovery_error = ''
		WHERE order_id = $1`,
		orderID,
		resolvedAt.UTC(),
		orderStatusCode(status),
		strings.TrimSpace(state.ExchangeOrderID),
		strings.TrimSpace(state.ClientOrderID),
		hasQtyState,
		state.OrigQty,
		state.ExecutedQty,
		state.RemainingQty,
		hasAvgPrice,
		state.AvgPrice,
		status,
	)
	if err != nil {
		return fmt.Errorf("mark recovery resolved: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *TimescaleRepository) SaveLifecycleEvent(ctx context.Context, event lifecycle.Event) (lifecycle.Event, error) {
	event.EventType = strings.TrimSpace(event.EventType)
	event.EventSource = strings.TrimSpace(event.EventSource)
	event.ExchangeOrderID = strings.TrimSpace(firstNonEmptyString(
		event.ExchangeOrderID,
		event.FillDelta.ExchangeOrderID,
		event.OrderState.ExchangeOrderID,
	))
	event.ExchangeTradeID = strings.TrimSpace(firstNonEmptyString(event.ExchangeTradeID, event.FillDelta.ExchangeTradeID))
	if event.EventType == "" {
		return lifecycle.Event{}, fmt.Errorf("event_type is required")
	}
	if err := lifecycle.ValidateEventRouteFacts(event); err != nil {
		return lifecycle.Event{}, fmt.Errorf("invalid lifecycle event route facts: %w", err)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	fillJSON, err := json.Marshal(event.FillDelta)
	if err != nil {
		return lifecycle.Event{}, fmt.Errorf("marshal fill delta: %w", err)
	}
	stateJSON, err := json.Marshal(event.OrderState)
	if err != nil {
		return lifecycle.Event{}, fmt.Errorf("marshal order state: %w", err)
	}
	eventIdentity := lifecycle.EventIdentityKey(event)

	query := `
		INSERT INTO order_lifecycle_events (
			session_id, account_id, venue_id, environment, exchange, market, position_side, side,
			intent_id, attempt_id, order_id,
			exchange_order_id, exchange_trade_id, event_type, event_source, order_status,
			fill_delta_json, order_state_json, occurred_at, event_identity
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,$18::jsonb,$19,$20)
	`
	conflictUpdate := `
		DO UPDATE SET
			session_id = COALESCE(EXCLUDED.session_id, order_lifecycle_events.session_id),
			account_id = EXCLUDED.account_id,
			environment = EXCLUDED.environment,
			exchange = EXCLUDED.exchange,
			market = EXCLUDED.market,
			position_side = EXCLUDED.position_side,
			side = EXCLUDED.side,
			intent_id = COALESCE(EXCLUDED.intent_id, order_lifecycle_events.intent_id),
			attempt_id = COALESCE(EXCLUDED.attempt_id, order_lifecycle_events.attempt_id),
			order_id = COALESCE(EXCLUDED.order_id, order_lifecycle_events.order_id),
			exchange_order_id = COALESCE(EXCLUDED.exchange_order_id, order_lifecycle_events.exchange_order_id),
			exchange_trade_id = COALESCE(EXCLUDED.exchange_trade_id, order_lifecycle_events.exchange_trade_id),
			event_type = EXCLUDED.event_type,
			event_source = EXCLUDED.event_source,
			order_status = EXCLUDED.order_status,
			fill_delta_json = EXCLUDED.fill_delta_json,
			order_state_json = EXCLUDED.order_state_json,
			occurred_at = EXCLUDED.occurred_at,
			event_identity = COALESCE(EXCLUDED.event_identity, order_lifecycle_events.event_identity)
	`
	switch {
	case event.ExchangeOrderID != "" && event.ExchangeTradeID != "":
		query += `
		ON CONFLICT (venue_id, exchange_order_id, exchange_trade_id)
			WHERE exchange_order_id IS NOT NULL AND exchange_trade_id IS NOT NULL
		` + conflictUpdate
	case eventIdentity != "":
		query += `
		ON CONFLICT (venue_id, event_identity)
			WHERE event_identity IS NOT NULL
		` + conflictUpdate
	}
	query += `
		RETURNING event_id, created_at`

	err = r.db.QueryRowContext(ctx, query,
		nullableString(event.SessionID),
		event.AccountID,
		event.VenueID,
		event.Environment,
		event.Exchange,
		event.Market,
		event.PositionSide,
		event.Side,
		nullableString(event.IntentID),
		nullableString(event.AttemptID),
		nullableString(event.OrderID),
		nullableString(event.ExchangeOrderID),
		nullableString(event.ExchangeTradeID),
		event.EventType,
		event.EventSource,
		event.OrderStatus,
		string(fillJSON),
		string(stateJSON),
		event.OccurredAt,
		nullableString(eventIdentity),
	).Scan(&event.EventID, &event.CreatedAt)
	if err != nil {
		return lifecycle.Event{}, fmt.Errorf("save lifecycle event: %w", err)
	}
	if err := r.backfillOrderFillFromLifecycleEvent(ctx, event); err != nil {
		return lifecycle.Event{}, err
	}
	return event, nil
}

func (r *TimescaleRepository) backfillOrderFillFromLifecycleEvent(ctx context.Context, event lifecycle.Event) error {
	if !shouldBackfillOrderFill(event) {
		return nil
	}
	orderID := strings.TrimSpace(event.OrderID)
	tradeID := strings.TrimSpace(firstNonEmptyString(event.ExchangeTradeID, event.FillDelta.ExchangeTradeID))
	exchangeOrderID := strings.TrimSpace(firstNonEmptyString(event.ExchangeOrderID, event.FillDelta.ExchangeOrderID, event.OrderState.ExchangeOrderID))
	fillTime := event.FillDelta.TradeTime
	if fillTime.IsZero() {
		fillTime = event.OccurredAt
	}
	if fillTime.IsZero() {
		fillTime = time.Now().UTC()
	}
	status := "FILLED"
	if event.FillDelta.FeeMissing {
		status = "FEE_MISSING"
	}

	res, err := r.db.ExecContext(ctx, `
		UPDATE order_fills
		SET exchange_order_id = COALESCE(NULLIF($3, ''), exchange_order_id),
		    qty = $4,
		    fill_price = $5,
		    fee = $6,
		    fee_asset = $7,
		    status = $8
		WHERE order_id = $1 AND exchange_trade_id = $2`,
		orderID,
		tradeID,
		exchangeOrderID,
		event.FillDelta.Qty,
		event.FillDelta.FillPrice,
		event.FillDelta.Fee,
		event.FillDelta.FeeAsset,
		fillStatusCode(status),
	)
	if err != nil {
		return fmt.Errorf("update lifecycle order fill: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows > 0 {
		return nil
	}

	fillID := fmt.Sprintf("lifecycle-%s-%s", orderID, tradeID)
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO order_fills (
			time, fill_id, exchange_trade_id, order_id, exchange_order_id, attempt_id, intent_id,
			qty, fill_price, fee, fee_asset, status
		)
		SELECT $1, $2, $3, o.order_id, COALESCE(NULLIF($4, ''), o.exchange_order_id), o.attempt_id, o.intent_id,
		       $5, $6, $7, $8, $9
		FROM orders o
		WHERE o.order_id = $10
		  AND NOT EXISTS (
		      SELECT 1 FROM order_fills f
		      WHERE f.order_id = o.order_id AND f.exchange_trade_id = $3
		  )`,
		fillTime.UTC(),
		fillID,
		tradeID,
		exchangeOrderID,
		event.FillDelta.Qty,
		event.FillDelta.FillPrice,
		event.FillDelta.Fee,
		event.FillDelta.FeeAsset,
		fillStatusCode(status),
		orderID,
	); err != nil {
		return fmt.Errorf("insert lifecycle order fill: %w", err)
	}
	return nil
}

func shouldBackfillOrderFill(event lifecycle.Event) bool {
	if !strings.EqualFold(strings.TrimSpace(event.EventType), "fill") {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(event.EventSource), lifecycle.EventSourcePlaceOrder) {
		return false
	}
	if strings.TrimSpace(event.OrderID) == "" {
		return false
	}
	if strings.TrimSpace(firstNonEmptyString(event.ExchangeTradeID, event.FillDelta.ExchangeTradeID)) == "" {
		return false
	}
	return event.FillDelta.Qty > 0
}

func (r *TimescaleRepository) ListLifecycleEvents(ctx context.Context, sessionID string, afterEventID int64, limit int) ([]lifecycle.Event, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("session_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT event_id, COALESCE(session_id, ''), account_id, venue_id,
		       environment, exchange, market, position_side, side,
		       COALESCE(intent_id, ''), COALESCE(attempt_id, ''), COALESCE(order_id, ''),
		       COALESCE(exchange_order_id, ''), COALESCE(exchange_trade_id, ''),
		       event_type, COALESCE(event_source, ''), order_status, fill_delta_json, order_state_json, occurred_at, created_at
		FROM order_lifecycle_events
		WHERE session_id = $1 AND event_id > $2
		ORDER BY event_id ASC
		LIMIT $3`, sessionID, afterEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("list lifecycle events: %w", err)
	}
	defer rows.Close()

	out := []lifecycle.Event{}
	for rows.Next() {
		var event lifecycle.Event
		var fillJSON, stateJSON []byte
		if err := rows.Scan(
			&event.EventID,
			&event.SessionID,
			&event.AccountID,
			&event.VenueID,
			&event.Environment,
			&event.Exchange,
			&event.Market,
			&event.PositionSide,
			&event.Side,
			&event.IntentID,
			&event.AttemptID,
			&event.OrderID,
			&event.ExchangeOrderID,
			&event.ExchangeTradeID,
			&event.EventType,
			&event.EventSource,
			&event.OrderStatus,
			&fillJSON,
			&stateJSON,
			&event.OccurredAt,
			&event.CreatedAt,
		); err != nil {
			return nil, err
		}
		if len(fillJSON) > 0 {
			if err := json.Unmarshal(fillJSON, &event.FillDelta); err != nil {
				return nil, fmt.Errorf("unmarshal fill delta for event %d: %w", event.EventID, err)
			}
		}
		if len(stateJSON) > 0 {
			if err := json.Unmarshal(stateJSON, &event.OrderState); err != nil {
				return nil, fmt.Errorf("unmarshal order state for event %d: %w", event.EventID, err)
			}
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lifecycle events: %w", err)
	}
	return out, nil
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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

func nullableTimePtr(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return *value
}

func timePtrFromNull(value sql.NullTime) *time.Time {
	if !value.Valid || value.Time.IsZero() {
		return nil
	}
	t := value.Time
	return &t
}

func riskReasonsJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return "[]"
	}
	return value
}

func orderSideCode(side string) (int16, error) {
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "BUY":
		return orderSideBuy, nil
	case "SELL":
		return orderSideSell, nil
	default:
		return 0, fmt.Errorf("unsupported order side: %q", side)
	}
}

func orderSideText(code int16) string {
	switch code {
	case orderSideBuy:
		return "BUY"
	case orderSideSell:
		return "SELL"
	default:
		return ""
	}
}

func intentStatusCode(status string) int16 {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "REJECTED", "FAILED":
		return intentStatusRejected
	default:
		return intentStatusRequested
	}
}

func intentStatusText(code int16) string {
	switch code {
	case intentStatusRejected:
		return "REJECTED"
	default:
		return "REQUESTED"
	}
}

func attemptStatusCode(status string) int16 {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "FAILED":
		return attemptStatusFailed
	case "ACCEPTED":
		return attemptStatusAccepted
	case "UNKNOWN":
		return attemptStatusUnknown
	case "RECOVERING":
		return attemptStatusRecovering
	case "RECOVERED":
		return attemptStatusRecovered
	case "RECOVERY_FAILED":
		return attemptStatusRecoveryFailed
	case "RISK_REJECTED":
		return attemptStatusRiskRejected
	default:
		return attemptStatusPending
	}
}

func attemptStatusText(code int16) string {
	switch code {
	case attemptStatusFailed:
		return "FAILED"
	case attemptStatusAccepted:
		return "ACCEPTED"
	case attemptStatusUnknown:
		return "UNKNOWN"
	case attemptStatusRecovering:
		return "RECOVERING"
	case attemptStatusRecovered:
		return "RECOVERED"
	case attemptStatusRecoveryFailed:
		return "RECOVERY_FAILED"
	case attemptStatusRiskRejected:
		return "RISK_REJECTED"
	default:
		return "PENDING"
	}
}

func orderStatusCode(status string) int16 {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PARTIALLY_FILLED":
		return orderStatusPartiallyFilled
	case "FILLED":
		return orderStatusFilled
	case "CANCELED", "CANCELLED":
		return orderStatusCanceled
	case "FAILED", "REJECTED":
		return orderStatusFailed
	case "EXPIRED":
		return orderStatusExpired
	default:
		return orderStatusNew
	}
}

func orderStatusText(code int16) string {
	switch code {
	case orderStatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case orderStatusFilled:
		return "FILLED"
	case orderStatusCanceled:
		return "CANCELED"
	case orderStatusFailed:
		return "FAILED"
	case orderStatusExpired:
		return "EXPIRED"
	default:
		return "NEW"
	}
}

func fillStatusCode(status string) int16 {
	if strings.EqualFold(strings.TrimSpace(status), "FEE_MISSING") {
		return fillStatusFeeMissing
	}
	return fillStatusFilled
}

func fillStatusText(code int16) string {
	if code == fillStatusFeeMissing {
		return "FEE_MISSING"
	}
	return "FILLED"
}
