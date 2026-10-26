module dash0.com/otlp-metric-store-backend

go 1.26

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.43.0
	github.com/cespare/xxhash/v2 v2.3.0
	go.opentelemetry.io/contrib/bridges/otelslog v0.16.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.66.0
	go.opentelemetry.io/otel v1.41.0
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.41.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.41.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.41.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.41.0
	go.opentelemetry.io/otel/log v0.17.0
	go.opentelemetry.io/otel/metric v1.41.0
	go.opentelemetry.io/otel/sdk v1.41.0
	go.opentelemetry.io/otel/sdk/log v0.17.0
	go.opentelemetry.io/otel/sdk/metric v1.41.0
	go.opentelemetry.io/otel/trace v1.41.0
	go.opentelemetry.io/proto/otlp v1.9.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/ClickHouse/ch-go v0.71.0 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/paulmach/orb v0.12.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.41.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260209200024-4cfbd4190f57 // indirect
)
