package logger

import (
	"context"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

// globalAdapter routes all Logger interface calls through elog global functions,
// so that logs from middleware go through all backends (file + kafka + ES).
type globalAdapter struct{}

func (globalAdapter) Info(ctx context.Context, logType, msg string)           { elog.Info(ctx, logType, msg) }
func (globalAdapter) Debug(ctx context.Context, logType, msg string)          { elog.Debug(ctx, logType, msg) }
func (globalAdapter) Warn(ctx context.Context, logType, msg string)           { elog.Warn(ctx, logType, msg) }
func (globalAdapter) Error(ctx context.Context, logType, msg string)          { elog.Error(ctx, logType, msg) }
func (globalAdapter) Fatal(ctx context.Context, logType, msg string)          { elog.Fatal(ctx, logType, msg) }
func (globalAdapter) Access(ctx context.Context, e elog.AccessLogEntry)       { elog.Access(ctx, e) }
func (globalAdapter) ExtAPI(ctx context.Context, e elog.ExtAPILogEntry)       { elog.ExtAPI(ctx, e) }
func (globalAdapter) WebSocket(ctx context.Context, e elog.WebSocketLogEntry) { elog.WebSocket(ctx, e) }
func (globalAdapter) SQL(ctx context.Context, e elog.SQLLogEntry)             { elog.SQL(ctx, e) }
func (globalAdapter) GRPCAccess(ctx context.Context, e elog.GRPCAccessLogEntry) {
	elog.GRPCAccess(ctx, e)
}
func (globalAdapter) GRPCExt(ctx context.Context, e elog.GRPCExtLogEntry)     { elog.GRPCExt(ctx, e) }
func (globalAdapter) KafkaSent(ctx context.Context, e elog.KafkaSentLogEntry) { elog.KafkaSent(ctx, e) }
func (globalAdapter) KafkaRecv(ctx context.Context, e elog.KafkaRecvLogEntry) { elog.KafkaRecv(ctx, e) }
func (globalAdapter) Close() error                                            { return nil } // elog.Close() called separately

var adapter globalAdapter

// Init initializes the global logger via elog.InitLog.
// configPath points to a JSON config file (e.g. "log-config.json").
func Init(configPath string) error {
	return elog.InitLog(configPath)
}

// InitWithConfig initializes the global logger from an in-memory Config.
// Prefer this over Init when config is loaded from YAML.
func InitWithConfig(cfg *elog.Config) error {
	return elog.InitLogWithConfig(cfg)
}

// Close flushes and closes all loggers.
func Close() error {
	return elog.Close()
}

// Instance returns a Logger that routes through all global backends (file + kafka + ES).
// Use this for middleware interfaces (accessLogger, grpcAccessLogger, etc.).
func Instance() elog.Logger {
	return adapter
}

func Info(ctx context.Context, logType, msg string) {
	elog.Info(ctx, logType, msg)
}

func Warn(ctx context.Context, logType, msg string) {
	elog.Warn(ctx, logType, msg)
}

func Error(ctx context.Context, logType, msg string) {
	elog.Error(ctx, logType, msg)
}

func Debug(ctx context.Context, logType, msg string) {
	elog.Debug(ctx, logType, msg)
}
