package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"dash0.com/otlp-metric-store-backend/internal/ingest"
	. "dash0.com/otlp-metric-store-backend/internal/otlptest"
	"dash0.com/otlp-metric-store-backend/internal/storage"
)

type fakeWriter struct {
	err    error
	series []storage.SeriesRow
	points []storage.NumberPointRow
}

func (f *fakeWriter) Write(_ context.Context, s []storage.SeriesRow, p []storage.NumberPointRow) error {
	if f.err != nil {
		return f.err
	}
	f.series = append(f.series, s...)
	f.points = append(f.points, p...)
	return nil
}

// client serves a MetricsService on an in-memory connection.
func client(t *testing.T, w Writer) colmetricspb.MetricsServiceClient {
	t.Helper()
	svc, err := NewMetricsService(w, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return colmetricspb.NewMetricsServiceClient(conn)
}

var now = uint64(time.Now().UnixNano())

func request(metrics ...*metricspb.Metric) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{Resource(Attrs(Str("service.name", "checkout")), metrics...)},
	}
}

func TestExportStoresDataPoints(t *testing.T) {
	w := &fakeWriter{}
	resp, err := client(t, w).Export(context.Background(), request(
		Gauge("cpu.utilization", "1", Point(now, 0.5)),
		CumulativeSum("http.server.requests", "{request}", Point(now, 3)),
	))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("unexpected partial success: %v", resp.GetPartialSuccess())
	}
	if len(w.points) != 2 || len(w.series) != 2 {
		t.Fatalf("stored %d points and %d series, want 2 and 2", len(w.points), len(w.series))
	}
}

func TestExportReportsRejectedDataPointsAsPartialSuccess(t *testing.T) {
	w := &fakeWriter{}
	resp, err := client(t, w).Export(context.Background(), request(
		Gauge("cpu.utilization", "1", Point(now, 0.5)),
		Histogram("http.server.duration", now, 4),
	))
	if err != nil {
		t.Fatal(err)
	}
	ps := resp.GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 4 || !strings.Contains(ps.GetErrorMessage(), "unsupported_metric_type=4 (histogram") {
		t.Fatalf("got partial success %v, want 4 rejected histogram data points", ps)
	}
	if len(w.points) != 1 {
		t.Fatalf("the valid gauge must still be stored, stored %d points", len(w.points))
	}
}

func TestExportEmptyRequest(t *testing.T) {
	resp, err := client(t, &fakeWriter{}).Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err != nil || resp.GetPartialSuccess() != nil {
		t.Fatalf("got %v, %v; want an empty success", resp, err)
	}
}

func TestExportMapsWriteErrorsToStatusCodes(t *testing.T) {
	tests := map[string]struct {
		err       error
		code      codes.Code
		retryInfo bool
	}{
		"overloaded":    {ingest.ErrOverloaded, codes.Unavailable, true},
		"shutting down": {ingest.ErrClosed, codes.Unavailable, true},
		"storage error": {errors.New("clickhouse: connection refused"), codes.Unavailable, true},
		"deadline":      {context.DeadlineExceeded, codes.DeadlineExceeded, false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := client(t, &fakeWriter{err: tt.err}).Export(context.Background(), request(Gauge("g", "1", Point(now, 1))))
			st := status.Convert(err)
			if st.Code() != tt.code {
				t.Fatalf("got %v, want code %v", err, tt.code)
			}
			hasRetryInfo := false
			for _, d := range st.Details() {
				if _, ok := d.(*errdetails.RetryInfo); ok {
					hasRetryInfo = true
				}
			}
			if hasRetryInfo != tt.retryInfo {
				t.Fatalf("RetryInfo present = %v, want %v", hasRetryInfo, tt.retryInfo)
			}
			if strings.Contains(st.Message(), "connection refused") {
				t.Fatalf("internal error details leaked to the client: %q", st.Message())
			}
		})
	}
}
