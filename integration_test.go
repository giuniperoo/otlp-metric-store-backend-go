//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	. "dash0.com/otlp-metric-store-backend/internal/otlptest"
	"dash0.com/otlp-metric-store-backend/internal/storage"
)

// The integration tests run against the ClickHouse at CLICKHOUSE_ADDR (with CLICKHOUSE_USERNAME
// and CLICKHOUSE_PASSWORD) if set, otherwise against a ClickHouse container started with
// testcontainers. Every test gets its own database.

var clickhouseAddr, clickhouseUser, clickhousePassword string

func TestMain(m *testing.M) {
	os.Exit(func() int {
		clickhouseAddr = os.Getenv("CLICKHOUSE_ADDR")
		clickhouseUser = envOr("CLICKHOUSE_USERNAME", "default")
		clickhousePassword = os.Getenv("CLICKHOUSE_PASSWORD")
		if clickhouseAddr == "" {
			ctx := context.Background()
			ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
				testcontainers.WithExposedPorts("9000/tcp"),
				testcontainers.WithEnv(map[string]string{"CLICKHOUSE_USER": "default", "CLICKHOUSE_PASSWORD": "test"}),
				testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second)),
			)
			if err != nil {
				fmt.Fprintln(os.Stderr, "starting clickhouse container:", err)
				return 1
			}
			defer func() { _ = ctr.Terminate(ctx) }()
			host, err := ctr.Host(ctx)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			port, err := ctr.MappedPort(ctx, "9000/tcp")
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			clickhouseAddr, clickhouseUser, clickhousePassword = net.JoinHostPort(host, port.Port()), "default", "test"
		}
		return m.Run()
	}())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// testDatabase creates an empty database for one test and returns its config.
func testDatabase(t *testing.T) storage.ClickHouseConfig {
	t.Helper()
	cfg := storage.ClickHouseConfig{Addr: clickhouseAddr, Database: "default", Username: clickhouseUser,
		Password: clickhousePassword, MaxOpenConns: 5, DialTimeout: 5 * time.Second}
	admin, err := storage.NewClickHouseStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	cfg.Database = fmt.Sprintf("it_%s_%d", strings.ToLower(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name())), rand.Uint32())
	if err := admin.Conn().Exec(context.Background(), "CREATE DATABASE "+cfg.Database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin, err := storage.NewClickHouseStore(context.Background(), cfg)
		if err == nil {
			_ = admin.Conn().Exec(context.Background(), "DROP DATABASE IF EXISTS "+cfg.Database)
			_ = admin.Close()
		}
	})
	return cfg
}

// testApp is the wired service from newApp, served on an in-memory listener.
type testApp struct {
	*app
	client colmetricspb.MetricsServiceClient
	conn   driver.Conn
}

// startApp starts the service with the default configuration, changed by the given flags.
func startApp(t *testing.T, ch storage.ClickHouseConfig, flags ...string) *testApp {
	t.Helper()
	cfg, err := loadConfig(flags, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.ClickHouse = ch
	cfg.Writer.MaxBatchDelay = 10 * time.Millisecond
	a, err := newApp(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = a.grpcServer.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		if err := a.shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return &testApp{app: a, client: colmetricspb.NewMetricsServiceClient(conn), conn: a.store.Conn()}
}

func (a *testApp) export(t *testing.T, resourceMetrics ...*metricspb.ResourceMetrics) *colmetricspb.ExportMetricsServiceResponse {
	t.Helper()
	resp, err := a.client.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: resourceMetrics})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return resp
}

func (a *testApp) count(t *testing.T, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := a.conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

var (
	t0      = time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	at      = func(d time.Duration) uint64 { return uint64(t0.Add(d).UnixNano()) }
	webHost = Attrs(Str("service.name", "web"), Str("host.name", "web-1"))
)

func TestMigrateIsIdempotentAndCreatesViews(t *testing.T) {
	ch := testDatabase(t)
	store, err := storage.NewClickHouseStore(context.Background(), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for range 2 {
		if err := store.Migrate(context.Background(), 7); err != nil {
			t.Fatal(err)
		}
	}
	a := &testApp{conn: store.Conn()}
	for _, name := range []string{storage.SeriesTable, storage.NumberPointsTable, storage.SeriesInRangeView, storage.NumberPointsInRangeView} {
		if n := a.count(t, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = $1", name); n != 1 {
			t.Errorf("%s not created", name)
		}
	}
	if n := a.count(t, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = $1 AND create_table_query LIKE '%TTL toDate(TimeUnix) + toIntervalDay(7)%'", storage.NumberPointsTable); n != 1 {
		t.Error("retention TTL not applied")
	}
}

func TestMigrateRejectsIncompatibleTable(t *testing.T) {
	ch := testDatabase(t)
	store, err := storage.NewClickHouseStore(context.Background(), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Conn().Exec(context.Background(),
		"CREATE TABLE otel_metrics_number_points (TimeUnix DateTime64(9), Value Float64) ENGINE = MergeTree ORDER BY TimeUnix"); err != nil {
		t.Fatal(err)
	}
	err = store.Migrate(context.Background(), 0)
	if err == nil || !strings.Contains(err.Error(), "lacks columns [SeriesId StartTimeUnix Flags]") {
		t.Fatalf("got %v, want an incompatible table error", err)
	}
}

func TestExportStoresSeriesOncePerDayAndAllDataPoints(t *testing.T) {
	a := startApp(t, testDatabase(t))
	request := func(offset time.Duration) *metricspb.ResourceMetrics {
		return Resource(webHost,
			Gauge("system.cpu.utilization", "1", Point(at(offset), 0.5, Str("cpu", "0")), Point(at(offset), 0.7, Str("cpu", "1"))),
			CumulativeSum("http.server.requests", "{request}", Point(at(offset), float64(offset/time.Minute), Str("method", "GET"))),
		)
	}
	for i := range 5 {
		if ps := a.export(t, request(time.Duration(i)*time.Minute)).GetPartialSuccess(); ps != nil {
			t.Fatalf("unexpected partial success: %v", ps)
		}
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_number_points"); n != 15 {
		t.Errorf("stored %d data points, want 15", n)
	}
	// The cache means the series rows were written exactly once, not five times.
	if n := a.count(t, "SELECT count() FROM otel_metrics_series"); n != 3 {
		t.Errorf("stored %d series rows, want 3", n)
	}
	// Every data point refers to a stored series.
	if n := a.count(t, "SELECT count() FROM otel_metrics_number_points WHERE SeriesId NOT IN (SELECT SeriesId FROM otel_metrics_series)"); n != 0 {
		t.Errorf("%d data points refer to missing series", n)
	}
}

func TestSeriesRowsAreIdempotentAcrossRestarts(t *testing.T) {
	ch := testDatabase(t)
	req := Resource(webHost, Gauge("queue.depth", "1", Point(at(0), 1)))
	for range 2 { // a restart (or a second replica) starts with an empty cache
		a := startApp(t, ch)
		a.export(t, req)
	}
	a := startApp(t, ch)
	if n := a.count(t, "SELECT count() FROM otel_metrics_series"); n != 2 {
		t.Fatalf("got %d raw series rows, want 2 (one per process)", n)
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_series FINAL"); n != 1 {
		t.Fatalf("got %d deduplicated series rows, want 1", n)
	}
}

func TestResourceAttributeChangeStartsNewSeries(t *testing.T) {
	a := startApp(t, testDatabase(t))
	a.export(t, Resource(Attrs(Str("service.name", "web"), Str("service.version", "1.0")), Gauge("queue.depth", "1", Point(at(0), 1))))
	a.export(t, Resource(Attrs(Str("service.name", "web"), Str("service.version", "1.1")), Gauge("queue.depth", "1", Point(at(time.Minute), 2))))
	if n := a.count(t, "SELECT uniqExact(SeriesId) FROM otel_metrics_series WHERE MetricName = 'queue.depth'"); n != 2 {
		t.Fatalf("got %d series, want 2", n)
	}
}

func TestDataPointsRoundTripExactly(t *testing.T) {
	a := startApp(t, testDatabase(t))
	p := Point(at(1234567891*time.Nanosecond), 1234567.000001, Str("k", "v"))
	p.StartTimeUnixNano = at(0)
	p.Flags = 1
	a.export(t, Resource(webHost, CumulativeSum("bytes.sent", "By", p)))

	var (
		start, ts time.Time
		value     float64
		flags     uint32
		unit      string
		monotonic bool
	)
	err := a.conn.QueryRow(context.Background(), `
		SELECT p.StartTimeUnix, p.TimeUnix, p.Value, p.Flags, s.MetricUnit, s.IsMonotonic
		FROM otel_metrics_number_points AS p INNER JOIN otel_metrics_series AS s ON p.SeriesId = s.SeriesId`,
	).Scan(&start, &ts, &value, &flags, &unit, &monotonic)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(start.UnixNano()) != p.StartTimeUnixNano || uint64(ts.UnixNano()) != p.TimeUnixNano {
		t.Errorf("timestamps changed: got %d/%d, want %d/%d", start.UnixNano(), ts.UnixNano(), p.StartTimeUnixNano, p.TimeUnixNano)
	}
	if value != 1234567.000001 || flags != 1 || unit != "By" || !monotonic {
		t.Errorf("got value %v flags %d unit %q monotonic %v", value, flags, unit, monotonic)
	}
}

func TestUnsupportedMetricTypesArePartiallyRejected(t *testing.T) {
	a := startApp(t, testDatabase(t))
	resp := a.export(t, Resource(webHost, Gauge("ok", "1", Point(at(0), 1)), Histogram("latency", at(0), 3)))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 3 {
		t.Fatalf("got partial success %v, want 3 rejected data points", resp.GetPartialSuccess())
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_number_points"); n != 1 {
		t.Fatalf("stored %d data points, want 1", n)
	}
}

// TestDataPointsOutsideTheAcceptedWindowArePartiallyRejected sends what used to fail or get
// lost: data points on more than 100 days, which ClickHouse refuses to insert in one batch
// (too many partitions), and a data point after 2149, which the Date column cannot hold.
func TestDataPointsOutsideTheAcceptedWindowArePartiallyRejected(t *testing.T) {
	a := startApp(t, testDatabase(t)) // defaults: maxDataPointAge 7 days, maxClockSkew 1 hour
	now := time.Now()
	var points []*metricspb.NumberDataPoint
	for day := range 101 {
		points = append(points, Point(uint64(now.Add(-time.Duration(day)*24*time.Hour).UnixNano()), float64(day)))
	}
	points = append(points, Point(uint64(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()), 1))

	ps := a.export(t, Resource(webHost, Gauge("queue.depth", "1", points...))).GetPartialSuccess()
	// Days 0 to 6 are within 7 days; day 7 is just older than the service's own now minus 7 days.
	if ps.GetRejectedDataPoints() != 95 ||
		!strings.Contains(ps.GetErrorMessage(), "timestamp_too_old=94") ||
		!strings.Contains(ps.GetErrorMessage(), "timestamp_in_future=1") {
		t.Fatalf("got partial success %v, want 94 data points too old and 1 in the future", ps)
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_number_points"); n != 7 {
		t.Errorf("stored %d data points, want 7", n)
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_series"); n != 7 {
		t.Errorf("stored %d series rows, want 7 (one per day)", n)
	}
}

// TestOldestAcceptedDataPointIsKeptByTheTTL checks the lower bound of the window against the
// TTL: a data point older than the retention would be acknowledged and then deleted at once.
func TestOldestAcceptedDataPointIsKeptByTheTTL(t *testing.T) {
	a := startApp(t, testDatabase(t), "-retentionDays=2")
	yesterday := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	oldest := uint64(yesterday.UnixNano())

	ps := a.export(t, Resource(webHost, Gauge("queue.depth", "1", Point(oldest, 1), Point(oldest-1, 2)))).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "timestamp_too_old=1") {
		t.Fatalf("got partial success %v, want the data point before yesterday rejected", ps)
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_number_points"); n != 1 {
		t.Errorf("%d data points visible, want 1: the TTL deleted an accepted data point", n)
	}
	if n := a.count(t, "SELECT count() FROM otel_metrics_series"); n != 1 {
		t.Errorf("%d series rows visible, want 1: the TTL deleted an accepted series row", n)
	}
}

// TestDocumentedQueriesNeverScanFullTables runs every query in queries/ with
// force_index_by_date and force_primary_key: ClickHouse refuses to run a query that cannot
// prune by partition and by primary key, so passing proves the queries never scan full tables.
func TestDocumentedQueriesNeverScanFullTables(t *testing.T) {
	a := startApp(t, testDatabase(t))
	// Data on three days, so that pruning has something to skip.
	for day := range 3 {
		offset := time.Duration(day-2) * 24 * time.Hour
		a.export(t,
			Resource(webHost,
				Gauge("system.cpu.utilization", "1", Point(at(offset), 0.5, Str("cpu", "0")), Point(at(offset+time.Minute), 0.6, Str("cpu", "0"))),
				CumulativeSum("http.server.requests", "{request}", Point(at(offset), 10), Point(at(offset+time.Minute), 25)),
			),
			Resource(Attrs(Str("service.name", "db"), Str("host.name", "db-1")),
				Gauge("system.cpu.utilization", "1", Point(at(offset), 0.9, Str("cpu", "0")))),
		)
	}

	guarded := clickhouse.Context(context.Background(),
		clickhouse.WithSettings(clickhouse.Settings{"force_index_by_date": 1, "force_primary_key": 1}),
		clickhouse.WithParameters(clickhouse.Parameters{
			"from":   t0.Format("2006-01-02 15:04:05.000000000"),
			"to":     t0.Add(time.Hour).Format("2006-01-02 15:04:05.000000000"),
			"metric": "system.cpu.utilization",
			"host":   "web-1",
		}))

	// The guard is real: a query without a time range is refused. (Not count(), which
	// ClickHouse answers from part metadata without reading the table.)
	if err := a.conn.QueryRow(guarded, "SELECT sum(Value) FROM otel_metrics_number_points").Scan(new(float64)); err == nil {
		t.Fatal("force_index_by_date did not refuse a query without a time range")
	}

	wantRows := map[string]int{
		"points_of_metric.sql":       3, // two web-1 points and one db-1 point, from the latest day only
		"data_points_per_metric.sql": 3, // cpu for web and db, requests for web
		"series_of_host.sql":         2,
		"increase_of_sums.sql":       1,
	}
	files, err := filepath.Glob("queries/*.sql")
	if err != nil || len(files) != len(wantRows) {
		t.Fatalf("found queries %v (%v), want %d", files, err, len(wantRows))
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			sql, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := a.conn.Query(guarded, string(sql))
			if err != nil {
				t.Fatalf("query refused or failed: %v", err)
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if n != wantRows[filepath.Base(file)] {
				t.Errorf("got %d rows, want %d", n, wantRows[filepath.Base(file)])
			}
		})
	}
}
