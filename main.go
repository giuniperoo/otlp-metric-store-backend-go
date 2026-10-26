// Command otlp-metric-store-backend receives OTLP metrics over gRPC and stores gauges and sums
// in ClickHouse, with series metadata in a lookup table and data points referring to it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // OTLP exporters commonly send gzip-compressed requests
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dash0.com/otlp-metric-store-backend/internal/ingest"
	"dash0.com/otlp-metric-store-backend/internal/series"
	"dash0.com/otlp-metric-store-backend/internal/server"
	"dash0.com/otlp-metric-store-backend/internal/storage"
	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "otlp-metric-store-backend:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string) (err error) {
	cfg, err := loadConfig(args, getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, shutdownTelemetry, err := telemetry.Setup(ctx, cfg.Telemetry)
	if err != nil {
		return err
	}
	defer func() {
		// Flush the service's own telemetry last, so the shutdown is visible in it.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = errors.Join(err, shutdownTelemetry(shutdownCtx))
	}()
	slog.SetDefault(logger)
	logger.Info("Starting otlp-metric-store-backend", slog.String("version", version), slog.Any("config", cfg.logValue()))

	a, err := newApp(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to start", slog.Any("error", err))
		return err
	}

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return errors.Join(fmt.Errorf("listening on %s: %w", cfg.ListenAddr, err), a.shutdown(context.Background()))
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- a.grpcServer.Serve(listener) }()
	logger.Info("Accepting OTLP metrics", slog.String("listen_addr", listener.Addr().String()))

	select {
	case <-ctx.Done():
		logger.Info("Shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
	case err = <-serveErr:
		logger.Error("gRPC server stopped", slog.Any("error", err))
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if shutdownErr := a.shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("Unclean shutdown", slog.Any("error", shutdownErr))
		err = errors.Join(err, shutdownErr)
	} else {
		logger.Info("Shut down cleanly")
	}
	return err
}

// app is the wired-up service, without the listener, so tests can serve it on any listener.
type app struct {
	store      *storage.ClickHouseStore
	writer     *ingest.Writer
	health     *health.Server
	grpcServer *grpc.Server
	log        *slog.Logger
}

func newApp(ctx context.Context, cfg config, logger *slog.Logger) (*app, error) {
	store, err := storage.NewClickHouseStore(ctx, cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	if cfg.Migrate {
		if err := store.Migrate(ctx, cfg.RetentionDays); err != nil {
			return nil, errors.Join(fmt.Errorf("migrating schema: %w", err), store.Close())
		}
	}
	cache := series.NewCache(cfg.SeriesCacheSize)
	writer, err := ingest.NewWriter(cfg.Writer, store, cache, logger)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	metricsService, err := server.NewMetricsService(writer, cache, logger)
	if err != nil {
		return nil, errors.Join(err, writer.Close(ctx), store.Close())
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(cfg.MaxReceiveMessageSize),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, metricsService)
	healthServer := health.NewServer() // reports SERVING; ClickHouse was reachable at startup
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	return &app{store: store, writer: writer, health: healthServer, grpcServer: grpcServer, log: logger}, nil
}

// shutdown stops in order: stop taking new requests and let in-flight ones finish (they wait
// for their batch), store everything still pending, then close ClickHouse connections.
func (a *app) shutdown(ctx context.Context) error {
	a.health.Shutdown() // NOT_SERVING, so load balancers stop sending traffic

	stopped := make(chan struct{})
	go func() {
		a.grpcServer.GracefulStop()
		close(stopped)
	}()
	var err error
	select {
	case <-stopped:
	case <-ctx.Done():
		a.grpcServer.Stop()
		err = errors.New("in-flight export requests did not finish before the shutdown deadline")
	}
	return errors.Join(err, a.writer.Close(ctx), a.store.Close())
}
