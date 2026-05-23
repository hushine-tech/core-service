package reconciliation

import (
	"context"
	"encoding/json"

	"github.com/hushine-tech/core-service/internal/logger"
)

// Metric counter names (consumed downstream by ELK / Kibana).
const (
	MetricRunsTotal     = "reconciliation_runs_total"
	MetricHardFailTotal = "reconciliation_hard_fail_total"
	MetricSoftFailTotal = "reconciliation_soft_fail_total"
	MetricErrorTotal    = "reconciliation_error_total"
)

// metricEvent is the JSON payload shape emitted through the existing
// golang-lib log pipeline (Kafka → Elasticsearch). Kibana queries filter on
// `metric` + `kind="counter"` to aggregate.
type metricEvent struct {
	Kind      string         `json:"kind"` // always "counter"
	Metric    string         `json:"metric"`
	Delta     int64          `json:"delta"`
	AccountID int64          `json:"account_id,omitempty"`
	UserID    int64          `json:"user_id,omitempty"`
	RunType   string         `json:"run_type,omitempty"`
	Extra     map[string]any `json:"extra,omitempty"`
}

// CounterHook is called for every successful counter increment. Tests set
// this to observe counter activity (e.g. to assert a panic recovery path
// still emits reconciliation_error_total). Nil in production; must be reset
// to nil after use to avoid cross-test leakage.
//
// Concurrency: reads/writes happen from reconciliation goroutines, so tests
// that read the hook output should serialize via their own sync primitives.
var CounterHook func(name string, accountID, userID int64, runType string, extra map[string]any)

// emitCounter sends one counter increment through the system log type.
// Failure to marshal is silently dropped — metrics must never affect the
// reconciliation flow (and definitely must never panic into the goroutine's
// recover path).
func emitCounter(ctx context.Context, name string, accountID, userID int64, runType string, extra map[string]any) {
	ev := metricEvent{
		Kind:      "counter",
		Metric:    name,
		Delta:     1,
		AccountID: accountID,
		UserID:    userID,
		RunType:   runType,
		Extra:     extra,
	}
	data, err := json.Marshal(ev)
	if err == nil {
		logger.Info(ctx, "system", string(data))
	}
	if hook := CounterHook; hook != nil {
		hook(name, accountID, userID, runType, extra)
	}
}
