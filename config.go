package main

import (
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"dash0.com/otlp-metric-store-backend/internal/ingest"
	"dash0.com/otlp-metric-store-backend/internal/mapping"
	"dash0.com/otlp-metric-store-backend/internal/storage"
	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

// config is the complete configuration of the service.
type config struct {
	ListenAddr            string
	MaxReceiveMessageSize int
	ShutdownTimeout       time.Duration

	ClickHouse    storage.ClickHouseConfig
	Migrate       bool
	RetentionDays int

	Writer          ingest.Config
	SeriesCacheSize int
	// TimeLimits bound the accepted data point times. RetentionDays is copied from the
	// retention, so that data points the TTL would delete right away are rejected.
	TimeLimits mapping.TimeLimits

	Telemetry telemetry.Config
}

// loadConfig reads the configuration from flags. Every flag can also be set with an environment
// variable named after it in upper snake case (listenAddr -> LISTEN_ADDR); flags win.
// The ClickHouse password can only be set with CLICKHOUSE_PASSWORD, so it never shows up in
// process listings.
func loadConfig(args []string, getenv func(string) string) (config, error) {
	var (
		cfg      config
		exporter string
		logLevel string
	)
	writer := ingest.DefaultConfig()
	defaultExporter := string(telemetry.ExporterStdout)
	if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		defaultExporter = string(telemetry.ExporterOTLP)
	}

	fs := flag.NewFlagSet("otlp-metric-store-backend", flag.ContinueOnError)
	fs.StringVar(&cfg.ListenAddr, "listenAddr", "localhost:4317", "The listen address of the OTLP/gRPC endpoint")
	fs.IntVar(&cfg.MaxReceiveMessageSize, "maxReceiveMessageSize", 16<<20, "The max message size in bytes the server can receive")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdownTimeout", 30*time.Second, "How long to wait for in-flight requests and pending batches on shutdown")

	fs.StringVar(&cfg.ClickHouse.Addr, "clickhouseAddr", "localhost:9000", "ClickHouse native protocol address")
	fs.StringVar(&cfg.ClickHouse.Database, "clickhouseDatabase", "default", "ClickHouse database")
	fs.StringVar(&cfg.ClickHouse.Username, "clickhouseUsername", "default", "ClickHouse user")
	fs.IntVar(&cfg.ClickHouse.MaxOpenConns, "clickhouseMaxOpenConns", 10, "Maximum open ClickHouse connections")
	fs.DurationVar(&cfg.ClickHouse.DialTimeout, "clickhouseDialTimeout", 5*time.Second, "ClickHouse connection timeout")
	fs.BoolVar(&cfg.Migrate, "migrate", true, "Create missing tables and views on startup")
	fs.IntVar(&cfg.RetentionDays, "retentionDays", 30, "Days to keep series and data points when creating tables (0 keeps data forever)")

	fs.IntVar(&writer.MaxBatchPoints, "batchMaxDataPoints", writer.MaxBatchPoints, "Data points at which a batch is stored")
	fs.DurationVar(&writer.MaxBatchDelay, "batchMaxDelay", writer.MaxBatchDelay, "Longest time a request waits for its batch to fill up")
	fs.IntVar(&writer.QueueSize, "writeQueueSize", writer.QueueSize, "Export requests that may wait for a batch before new ones are rejected as overloaded")
	fs.IntVar(&writer.MaxPendingPoints, "maxPendingDataPoints", writer.MaxPendingPoints, "Data points accepted but not yet stored before new requests are rejected as overloaded (bounds memory)")
	fs.IntVar(&writer.FlushWorkers, "flushWorkers", writer.FlushWorkers, "Batches stored concurrently")
	fs.DurationVar(&writer.FlushTimeout, "flushTimeout", writer.FlushTimeout, "Timeout for storing one batch")
	fs.IntVar(&cfg.SeriesCacheSize, "seriesCacheSize", 1_000_000, "Series lookup rows remembered as written (0 disables the cache)")
	fs.DurationVar(&cfg.TimeLimits.MaxAge, "maxDataPointAge", 7*24*time.Hour, "Oldest accepted data point, relative to the time of the request; older ones are rejected")
	fs.DurationVar(&cfg.TimeLimits.MaxClockSkew, "maxClockSkew", time.Hour, "Furthest accepted data point in the future, for clients whose clocks run ahead")

	fs.StringVar(&exporter, "telemetryExporter", defaultExporter, "Where to send the service's own traces, metrics and logs: otlp, stdout or none (default otlp if OTEL_EXPORTER_OTLP_ENDPOINT is set)")
	fs.StringVar(&logLevel, "logLevel", "info", "Log level: debug, info, warn or error")

	var envErr error
	fs.VisitAll(func(f *flag.Flag) {
		if v := getenv(envName(f.Name)); v != "" {
			if err := f.Value.Set(v); err != nil && envErr == nil {
				envErr = fmt.Errorf("invalid %s=%q: %w", envName(f.Name), v, err)
			}
		}
	})
	if envErr != nil {
		return config{}, envErr
	}
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg.ClickHouse.Password = getenv("CLICKHOUSE_PASSWORD")
	cfg.Writer = writer
	cfg.Telemetry.Exporter = telemetry.Exporter(exporter)
	cfg.Telemetry.ServiceVersion = version
	if err := cfg.Telemetry.LogLevel.UnmarshalText([]byte(logLevel)); err != nil {
		return config{}, fmt.Errorf("invalid log level %q: %w", logLevel, err)
	}
	if cfg.RetentionDays < 0 {
		return config{}, fmt.Errorf("retentionDays must not be negative, got %d", cfg.RetentionDays)
	}
	cfg.TimeLimits.RetentionDays = cfg.RetentionDays
	if err := cfg.TimeLimits.Validate(); err != nil {
		return config{}, fmt.Errorf("invalid maxDataPointAge or maxClockSkew: %w", err)
	}
	return cfg, nil
}

// envName converts a flag name to its environment variable: clickhouseAddr -> CLICKHOUSE_ADDR.
func envName(flagName string) string {
	var b strings.Builder
	for i, r := range flagName {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

// logValue lists the configuration for the startup log, without the password.
func (c config) logValue() slog.Value {
	return slog.GroupValue(
		slog.String("listen_addr", c.ListenAddr),
		slog.String("clickhouse_addr", c.ClickHouse.Addr),
		slog.String("clickhouse_database", c.ClickHouse.Database),
		slog.Bool("migrate", c.Migrate),
		slog.Int("retention_days", c.RetentionDays),
		slog.Int("batch_max_data_points", c.Writer.MaxBatchPoints),
		slog.String("batch_max_delay", c.Writer.MaxBatchDelay.String()),
		slog.Int("write_queue_size", c.Writer.QueueSize),
		slog.Int("max_pending_data_points", c.Writer.MaxPendingPoints),
		slog.Int("flush_workers", c.Writer.FlushWorkers),
		slog.Int("series_cache_size", c.SeriesCacheSize),
		slog.String("max_data_point_age", c.TimeLimits.MaxAge.String()),
		slog.String("max_clock_skew", c.TimeLimits.MaxClockSkew.String()),
		slog.String("telemetry_exporter", string(c.Telemetry.Exporter)),
	)
}
