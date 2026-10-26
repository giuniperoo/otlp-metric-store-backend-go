// Package server implements the OTLP/gRPC MetricsService.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"dash0.com/otlp-metric-store-backend/internal/ingest"
	"dash0.com/otlp-metric-store-backend/internal/mapping"
	"dash0.com/otlp-metric-store-backend/internal/storage"
	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

// Writer stores mapped rows. It is implemented by *ingest.Writer.
type Writer interface {
	Write(ctx context.Context, newSeries []storage.SeriesRow, points []storage.NumberPointRow) error
}

// MetricsService receives OTLP metrics, maps and validates them, and hands them to a Writer.
type MetricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer

	writer Writer
	known  mapping.KnownSeries
	log    *slog.Logger
	tel    instruments
}

// NewMetricsService returns a MetricsService. known may be nil to disable the series cache.
func NewMetricsService(writer Writer, known mapping.KnownSeries, log *slog.Logger) (*MetricsService, error) {
	tel, err := newInstruments()
	if err != nil {
		return nil, err
	}
	return &MetricsService{writer: writer, known: known, log: log, tel: tel}, nil
}

// retryDelay is the delay suggested to clients that are throttled or hit a storage error.
const retryDelay = time.Second

// Export implements the OTLP MetricsService.
//
// Following the OTLP specification:
//   - invalid or unsupported data points are dropped and reported in a partial success with
//     the number of rejected data points and the reasons; the rest of the request is stored,
//   - when the service is overloaded or ClickHouse is unavailable, the whole request fails with
//     UNAVAILABLE and a RetryInfo, which OTLP exporters retry with backoff,
//   - a success response is only sent after the data points are stored.
func (s *MetricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	res := mapping.Map(req.GetResourceMetrics(), s.known)

	s.tel.metricsReceived.Add(ctx, int64(res.Metrics))
	s.tel.pointsReceived.Add(ctx, int64(res.DataPoints))
	for _, reason := range res.Rejected.Reasons() {
		s.tel.pointsRejected.Add(ctx, res.Rejected.Count(reason), metric.WithAttributes(attribute.String("reason", string(reason))))
	}
	s.tel.cacheLookups.Add(ctx, int64(res.SeriesKeys-len(res.NewSeries)), metric.WithAttributes(attribute.String("result", "hit")))
	s.tel.cacheLookups.Add(ctx, int64(len(res.NewSeries)), metric.WithAttributes(attribute.String("result", "miss")))

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.Int("otlp.metrics", res.Metrics),
		attribute.Int("otlp.data_points", res.DataPoints),
		attribute.Int64("otlp.data_points.rejected", res.Rejected.Total()),
		attribute.Int("series.new", len(res.NewSeries)),
	)

	if err := s.writer.Write(ctx, res.NewSeries, res.Points); err != nil {
		outcome, st := s.statusFor(ctx, err, len(res.Points))
		s.tel.exports.Add(ctx, 1, metric.WithAttributes(telemetry.Outcome(outcome)))
		return nil, st.Err()
	}

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	outcome := telemetry.OutcomeSuccess
	if rejected := res.Rejected.Total(); rejected > 0 {
		outcome = telemetry.OutcomePartialSuccess
		msg := res.Rejected.Message()
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{RejectedDataPoints: rejected, ErrorMessage: msg}
		s.log.WarnContext(ctx, "Rejected data points", slog.Int64("rejected", rejected),
			slog.Int("received", res.DataPoints), slog.String("reasons", msg))
	}
	s.tel.exports.Add(ctx, 1, metric.WithAttributes(telemetry.Outcome(outcome)))
	return resp, nil
}

// statusFor maps a write error to the gRPC status returned to the client.
func (s *MetricsService) statusFor(ctx context.Context, err error, points int) (outcome string, st *status.Status) {
	switch {
	case errors.Is(err, ingest.ErrOverloaded):
		s.log.WarnContext(ctx, "Rejected export: write queue is full", slog.Int("data_points", points))
		return telemetry.OutcomeOverloaded, retryable("server is overloaded, retry later")
	case errors.Is(err, ingest.ErrClosed):
		return telemetry.OutcomeError, retryable("server is shutting down, retry later")
	case errors.Is(err, context.DeadlineExceeded):
		return telemetry.OutcomeError, status.New(codes.DeadlineExceeded, "deadline exceeded before data points were stored")
	case errors.Is(err, context.Canceled):
		return telemetry.OutcomeError, status.New(codes.Canceled, "request canceled before data points were stored")
	default:
		// The writer logs the storage error once per batch at error level; logging it again for
		// every request of the batch would multiply each failure by the batch's request count.
		s.log.DebugContext(ctx, "Failed export: storing data points failed", slog.Any("error", err), slog.Int("data_points", points))
		return telemetry.OutcomeError, retryable("storing data points failed, retry later")
	}
}

// retryable returns an UNAVAILABLE status with a RetryInfo, which OTLP clients retry.
func retryable(msg string) *status.Status {
	st := status.New(codes.Unavailable, msg)
	if withDetails, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(retryDelay)}); err == nil {
		return withDetails
	}
	return st
}

type instruments struct {
	exports         metric.Int64Counter
	metricsReceived metric.Int64Counter
	pointsReceived  metric.Int64Counter
	pointsRejected  metric.Int64Counter
	cacheLookups    metric.Int64Counter
}

func newInstruments() (instruments, error) {
	b := telemetry.NewBuilder(otel.Meter(telemetry.ScopeName + "/internal/server"))
	tel := instruments{
		exports: b.Counter("exports", "{request}",
			"Export requests handled, by outcome (success, partial_success, overloaded, error)"),
		metricsReceived: b.Counter("metrics.received", "{metric}",
			"Metrics received in export requests"),
		pointsReceived: b.Counter("data_points.received", "{data_point}",
			"Data points received in export requests, accepted or rejected"),
		pointsRejected: b.Counter("data_points.rejected", "{data_point}",
			"Data points rejected because they are invalid or of an unsupported metric type, by reason"),
		cacheLookups: b.Counter("series.cache.lookups", "{lookup}",
			"Series cache lookups for accepted data points, by result (hit, miss)"),
	}
	return tel, b.Err()
}
