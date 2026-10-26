package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Exporter selects where the service sends its own telemetry.
type Exporter string

const (
	// ExporterOTLP sends traces, metrics and logs over OTLP/gRPC, configured with the standard
	// OTEL_EXPORTER_OTLP_* environment variables (endpoint, headers, TLS), e.g. to Dash0.
	ExporterOTLP Exporter = "otlp"
	// ExporterStdout writes traces and metrics to stdout, for local development.
	ExporterStdout Exporter = "stdout"
	// ExporterNone disables traces and metrics. Logs are still written to stderr.
	ExporterNone Exporter = "none"
)

// Config configures Setup.
type Config struct {
	Exporter       Exporter
	ServiceVersion string
	LogLevel       slog.Level
}

// Setup installs the global tracer, meter and logger providers and returns the logger the
// service should use. Logs always go to stderr; with the OTLP exporter they are also exported,
// correlated with the active trace. Call shutdown to flush telemetry before exiting.
func Setup(ctx context.Context, cfg Config) (logger *slog.Logger, shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error
	shutdown = func(ctx context.Context) error {
		var err error
		for i := len(shutdownFuncs) - 1; i >= 0; i-- {
			err = errors.Join(err, shutdownFuncs[i](ctx))
		}
		shutdownFuncs = nil
		return err
	}
	fail := func(inErr error) (*slog.Logger, func(context.Context) error, error) {
		return nil, nil, errors.Join(inErr, shutdown(ctx))
	}

	stderr := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})
	logger = slog.New(stderr)

	res, err := resource.New(ctx,
		resource.WithFromEnv(), // OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME win
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName("otlp-metric-store-backend"),
			semconv.ServiceNamespace("dash0-exercise"),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return fail(fmt.Errorf("creating telemetry resource: %w", err))
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	var (
		spanExporter   sdktrace.SpanExporter
		metricExporter sdkmetric.Exporter
	)
	switch cfg.Exporter {
	case ExporterOTLP:
		if spanExporter, err = otlptracegrpc.New(ctx); err != nil {
			return fail(fmt.Errorf("creating OTLP span exporter: %w", err))
		}
		if metricExporter, err = otlpmetricgrpc.New(ctx); err != nil {
			return fail(fmt.Errorf("creating OTLP metric exporter: %w", err))
		}
		logExporter, err := otlploggrpc.New(ctx)
		if err != nil {
			return fail(fmt.Errorf("creating OTLP log exporter: %w", err))
		}
		loggerProvider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)))
		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
		logger = slog.New(slog.NewMultiHandler(stderr, otelslog.NewHandler(ScopeName, otelslog.WithLoggerProvider(loggerProvider))))
	case ExporterStdout:
		if spanExporter, err = stdouttrace.New(); err != nil {
			return fail(fmt.Errorf("creating stdout span exporter: %w", err))
		}
		if metricExporter, err = stdoutmetric.New(); err != nil {
			return fail(fmt.Errorf("creating stdout metric exporter: %w", err))
		}
	case ExporterNone:
		return logger, shutdown, nil
	default:
		return fail(fmt.Errorf("unknown telemetry exporter %q (want otlp, stdout or none)", cfg.Exporter))
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(spanExporter),
		// Sampling follows OTEL_TRACES_SAMPLER (default: parent-based, always on).
	)
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		// The export interval follows OTEL_METRIC_EXPORT_INTERVAL (default 60s).
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return logger, shutdown, nil
}
