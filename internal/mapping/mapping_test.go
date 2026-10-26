package mapping

import (
	"strings"
	"testing"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	. "dash0.com/otlp-metric-store-backend/internal/otlptest"
	"dash0.com/otlp-metric-store-backend/internal/series"
	"dash0.com/otlp-metric-store-backend/internal/storage"
)

var (
	day1 = uint64(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC).UnixNano())
	day2 = uint64(time.Date(2026, 10, 26, 0, 0, 1, 0, time.UTC).UnixNano())
)

func checkoutResource(metrics ...*metricspb.Metric) *metricspb.ResourceMetrics {
	return Resource(Attrs(Str("service.name", "checkout"), Str("host.name", "a")), metrics...)
}

func TestMapGaugeAndSum(t *testing.T) {
	res := Map([]*metricspb.ResourceMetrics{checkoutResource(
		Gauge("cpu.utilization", "1", Point(day1, 0.5, Str("cpu", "0")), Point(day1, 0.25, Str("cpu", "1"))),
		CumulativeSum("http.server.requests", "{request}", Point(day1, 10, Str("method", "GET"))),
	)}, nil)

	if res.Metrics != 2 || res.DataPoints != 3 || len(res.Points) != 3 || res.Rejected.Total() != 0 {
		t.Fatalf("got %d metrics, %d data points, %d accepted, %d rejected; want 2, 3, 3, 0",
			res.Metrics, res.DataPoints, len(res.Points), res.Rejected.Total())
	}
	if len(res.NewSeries) != 3 || res.SeriesKeys != 3 {
		t.Fatalf("got %d new series of %d keys, want 3 of 3", len(res.NewSeries), res.SeriesKeys)
	}

	byID := map[series.ID]storage.SeriesRow{}
	for _, s := range res.NewSeries {
		byID[s.SeriesID] = s
	}
	for _, p := range res.Points {
		s, ok := byID[p.SeriesID]
		if !ok {
			t.Fatalf("data point refers to series %d, which has no lookup row", p.SeriesID)
		}
		if s.ServiceName != "checkout" || s.ResourceAttributes["host.name"] != "a" || s.ScopeName != "otlptest" {
			t.Errorf("series row lacks resource or scope identity: %+v", s)
		}
		if !p.TimeUnix.Equal(time.Unix(0, int64(day1))) || s.Day != series.DayOf(day1) {
			t.Errorf("data point time %v / series day %v do not match the input", p.TimeUnix, s.Day)
		}
	}

	sum := findSeries(t, res.NewSeries, "http.server.requests")
	if sum.MetricType != series.MetricTypeSum || !sum.IsMonotonic ||
		sum.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("sum identity not mapped: %+v", sum)
	}
	if sum.Attributes["method"] != "GET" || sum.MetricUnit != "{request}" {
		t.Errorf("sum data point attributes or unit not mapped: %+v", sum)
	}
}

func TestMapWritesOneSeriesRowPerSeriesAndDay(t *testing.T) {
	gauge := Gauge("queue.depth", "1",
		Point(day1, 1), Point(day1+uint64(time.Second), 2), // same series, same day
		Point(day2, 3), // same series, next day
	)
	res := Map([]*metricspb.ResourceMetrics{checkoutResource(gauge)}, nil)
	if len(res.Points) != 3 {
		t.Fatalf("got %d data points, want 3", len(res.Points))
	}
	if len(res.NewSeries) != 2 {
		t.Fatalf("got %d series rows, want 2 (one per day)", len(res.NewSeries))
	}
	if res.NewSeries[0].SeriesID != res.NewSeries[1].SeriesID {
		t.Fatal("the same series on two days must keep its series ID")
	}
}

func TestMapSkipsKnownSeries(t *testing.T) {
	req := []*metricspb.ResourceMetrics{checkoutResource(Gauge("queue.depth", "1", Point(day1, 1), Point(day1, 2, Str("queue", "b"))))}
	cache := series.NewCache(100)
	first := Map(req, cache)
	cache.Add([]series.Key{first.NewSeries[0].Key()})

	second := Map(req, cache)
	if len(second.Points) != 2 || second.SeriesKeys != 2 {
		t.Fatalf("got %d points and %d series keys, want 2 and 2", len(second.Points), second.SeriesKeys)
	}
	if len(second.NewSeries) != 1 || second.NewSeries[0].SeriesID == first.NewSeries[0].SeriesID {
		t.Fatalf("want only the uncached series as new, got %+v", second.NewSeries)
	}
}

func TestResourceAttributeChangeCreatesNewSeries(t *testing.T) {
	a := Map([]*metricspb.ResourceMetrics{Resource(Attrs(Str("service.name", "checkout"), Str("service.version", "1.0")),
		Gauge("queue.depth", "1", Point(day1, 1)))}, nil)
	b := Map([]*metricspb.ResourceMetrics{Resource(Attrs(Str("service.name", "checkout"), Str("service.version", "1.1")),
		Gauge("queue.depth", "1", Point(day1, 1)))}, nil)
	if a.Points[0].SeriesID == b.Points[0].SeriesID {
		t.Fatal("a new service.version must start a new series")
	}
}

func TestMetricDescriptionIsNotPartOfIdentity(t *testing.T) {
	m1 := Gauge("queue.depth", "1", Point(day1, 1))
	m1.Description = "old"
	m2 := Gauge("queue.depth", "1", Point(day1, 1))
	m2.Description = "new"
	a := Map([]*metricspb.ResourceMetrics{checkoutResource(m1)}, nil)
	b := Map([]*metricspb.ResourceMetrics{checkoutResource(m2)}, nil)
	if a.Points[0].SeriesID != b.Points[0].SeriesID {
		t.Fatal("a description change must not split the series")
	}
	if b.NewSeries[0].MetricDescription != "new" {
		t.Fatalf("description not stored: %q", b.NewSeries[0].MetricDescription)
	}
}

func TestMapRejectsInvalidDataPoints(t *testing.T) {
	noValue := Point(day1, 0)
	noValue.Value = nil
	tooLate := Point(1<<63, 1)
	badStart := Point(day1, 1)
	badStart.StartTimeUnixNano = 1 << 63

	res := Map([]*metricspb.ResourceMetrics{checkoutResource(
		Gauge("ok", "1", Point(day1, 1)),
		Gauge("invalid", "1", Point(0, 1), tooLate, badStart, noValue),
		Gauge("", "1", Point(day1, 1), Point(day1, 2)),
		Histogram("latency", day1, 3),
		&metricspb.Metric{Name: "no.data"},
	)}, nil)

	want := map[Reason]int64{
		ReasonInvalidTimestamp:      3,
		ReasonMissingValue:          1,
		ReasonMissingMetricName:     2,
		ReasonUnsupportedMetricType: 3,
	}
	for reason, n := range want {
		if got := res.Rejected.Count(reason); got != n {
			t.Errorf("rejected %d data points for %s, want %d", got, reason, n)
		}
	}
	if res.Rejected.Total() != 9 || len(res.Points) != 1 || res.DataPoints != 10 || res.Metrics != 5 {
		t.Fatalf("got %d rejected, %d accepted of %d data points in %d metrics; want 9, 1, 10, 5",
			res.Rejected.Total(), len(res.Points), res.DataPoints, res.Metrics)
	}
	msg := res.Rejected.Message()
	for _, part := range []string{"rejected 9 data points", "invalid_timestamp=3", "unsupported_metric_type=3 (histogram;"} {
		if !strings.Contains(msg, part) {
			t.Errorf("message %q lacks %q", msg, part)
		}
	}
}

func TestMapEmptyRequest(t *testing.T) {
	res := Map(nil, nil)
	if res.DataPoints != 0 || len(res.Points) != 0 || res.Rejected.Message() != "" {
		t.Fatalf("unexpected result for empty request: %+v", res)
	}
}

func findSeries(t *testing.T, rows []storage.SeriesRow, metric string) storage.SeriesRow {
	t.Helper()
	for _, r := range rows {
		if r.MetricName == metric {
			return r
		}
	}
	t.Fatalf("no series for metric %s", metric)
	return storage.SeriesRow{}
}

// BenchmarkMap maps a request of 10 resources x 20 metrics x 50 data points (10,000 points)
// with a warm cache, the steady state of a production ingest path.
func BenchmarkMap(b *testing.B) {
	req := LoadRequest(10, 20, 50, day1)
	cache := series.NewCache(1 << 20)
	for _, s := range Map(req, nil).NewSeries {
		cache.Add([]series.Key{s.Key()})
	}
	b.ReportAllocs()
	for b.Loop() {
		res := Map(req, cache)
		if len(res.NewSeries) != 0 {
			b.Fatal("cache should be warm")
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/10000, "ns/datapoint")
}
