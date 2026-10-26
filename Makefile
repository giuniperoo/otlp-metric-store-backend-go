MODULE := dash0.com/otlp-metric-store-backend

.PHONY: build run test test-integration test-all bench load up down fmt vet lint tidy clean

build:
	go build ./...

run:
	go run .

test:
	go test -race -count=1 ./...

# Uses the ClickHouse at CLICKHOUSE_ADDR if set, otherwise starts one with testcontainers.
test-integration:
	go test -tags integration -count=1 -v ./...

test-all: test test-integration

bench:
	go test -run xxx -bench . -benchmem ./internal/...

# Sends synthetic load to a running service (see tools/loadgen for flags).
load:
	go run ./tools/loadgen -concurrency 16 -duration 20s

up:
	docker compose up --build -d

down:
	docker compose down -v

fmt:
	go fmt ./...

vet:
	go vet ./... && go vet -tags integration ./...

lint: vet
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy:
	go mod tidy

clean:
	go clean ./...
