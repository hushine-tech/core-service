ACCOUNT_PROTO_FILE=proto/account_service.proto
ACCOUNT_PROTO_OUT=gen/accountv1
ORDER_PROTO_FILE=proto/order_service.proto
ORDER_PROTO_OUT=gen/orderv1
BIN=bin/core-service
CONFIG?=./config.yaml
PID_FILE=.run.pid

.PHONY: proto proto-account proto-order test test-integration e2e ensure-db ensure-order-db tidy build run dev start stop clean

proto: proto-account proto-order

proto-account:
	mkdir -p $(ACCOUNT_PROTO_OUT)
	PATH="$(HOME)/go/bin:$$PATH" protoc -I proto \
		--go_out=$(ACCOUNT_PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(ACCOUNT_PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(ACCOUNT_PROTO_FILE)

proto-order:
	mkdir -p $(ORDER_PROTO_OUT)
	PATH="$(HOME)/go/bin:$$PATH" protoc -I proto \
		--go_out=$(ORDER_PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(ORDER_PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(ORDER_PROTO_FILE)

tidy:
	go mod tidy

test:
	go test ./...

# Requires TimescaleDB (default DSN host 192.168.88.10 dbname account). Skips if DB unreachable.
test-integration:
	go test -tags=integration -v ./tests/integration/...

# Full shell e2e: curl POST /accounts + gRPC client; set MOCK_BINANCE=1 in script. Uses same DSN default.
e2e:
	bash scripts/integration_e2e.sh

# Create database "account" on PGHOST (default 192.168.88.10) and apply internal/storage/migrations/*.sql
ensure-db:
	go run ./cmd/ensure-account-db

ensure-order-db:
	go run ./cmd/ensure-order-db -config $(CONFIG)

# ── Build / Dev / Start / Stop ──
build:
	mkdir -p bin
	go build -o $(BIN) ./cmd/core-service

dev:
	go run ./cmd/core-service -config $(CONFIG)

run: dev

start: build
	mkdir -p logs
	python3 -c 'import subprocess; out=open("logs/core-service.out","ab",buffering=0); p=subprocess.Popen(["./$(BIN)","-config","$(CONFIG)"], stdout=out, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True); open("$(PID_FILE)","w").write(str(p.pid)+"\n")'
	@echo "✓ core-service started (pid=$$(cat $(PID_FILE))), logs at core-service/logs/core-service.out"

stop:
	@if [ -f $(PID_FILE) ]; then kill $$(cat $(PID_FILE)) 2>/dev/null || true; rm -f $(PID_FILE); echo "✓ core-service stopped"; else echo "(no $(PID_FILE), nothing to stop)"; fi

clean:
	rm -rf bin $(PID_FILE)
