package mapping

import (
	"math"
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

	// anyTime accepts every representable data point time.
	anyTime = Window{From: 0, To: math.MaxUint64}
)

func checkoutResource(metrics ...*metricspb.Metric) *metricspb.ResourceMetrics {
	return Resource(Attrs(Str("service.name", "checkout"), Str("host.name", "a")), metrics...)
}

func TestMapGaugeAndSum(t *testing.T) {
	res := Map([]*metricspb.ResourceMetrics{checkoutResource(
		Gauge("cpu.utilization", "1", Point(day1, 0.5, Str("cpu", "0")), Point(day1, 0.25, Str("cpu", "1"))),
		CumulativeSum("http.server.requests", "{request}", Point(day1, 10, Str("method", "GET"))),
	)}, anyTime, nil)

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
	res := Map([]*metricspb.ResourceMetrics{checkoutResource(gauge)}, anyTime, nil)
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
	first := Map(req, anyTime, cache)
	cache.Add([]series.Key{first.NewSeries[0].Key()})

	second := Map(req, anyTime, cache)
	if len(second.Points) != 2 || second.SeriesKeys != 2 {
		t.Fatalf("got %d points and %d series keys, want 2 and 2", len(second.Points), second.SeriesKeys)
	}
	if len(second.NewSeries) != 1 || second.NewSeries[0].SeriesID == first.NewSeries[0].SeriesID {
		t.Fatalf("want only the uncached series as new, got %+v", second.NewSeries)
	}
}

func TestResourceAttributeChangeCreatesNewSeries(t *testing.T) {
	a := Map([]*metricspb.ResourceMetrics{Resource(Attrs(Str("service.name", "checkout"), Str("service.version", "1.0")),
		Gauge("queue.depth", "1", Point(day1, 1)))}, anyTime, nil)
	b := Map([]*metricspb.ResourceMetrics{Resource(Attrs(Str("service.name", "checkout"), Str("service.version", "1.1")),
		Gauge("queue.depth", "1", Point(day1, 1)))}, anyTime, nil)
	if a.Points[0].SeriesID == b.Points[0].SeriesID {
		t.Fatal("a new service.version must start a new series")
	}
}

func TestMetricDescriptionIsNotPartOfIdentity(t *testing.T) {
	m1 := Gauge("queue.depth", "1", Point(day1, 1))
	m1.Description = "old"
	m2 := Gauge("queue.depth", "1", Point(day1, 1))
	m2.Description = "new"
	a := Map([]*metricspb.ResourceMetrics{checkoutResource(m1)}, anyTime, nil)
	b := Map([]*metricspb.ResourceMetrics{checkoutResource(m2)}, anyTime, nil)
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
	)}, anyTime, nil)

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

func TestMapRejectsDataPointsOutsideTheWindow(t *testing.T) {
	window := Window{From: day1, To: day2}
	res := Map([]*metricspb.ResourceMetrics{checkoutResource(
		Gauge("queue.depth", "1", Point(day1-1, 1), Point(day1, 2), Point(day2, 3), Point(day2+1, 4), Point(day2+1, 5)),
	)}, window, nil)

	if len(res.Points) != 2 || len(res.NewSeries) != 2 {
		t.Fatalf("got %d data points and %d series rows, want 2 and 2 (both bounds are accepted)", len(res.Points), len(res.NewSeries))
	}
	if res.Rejected.Count(ReasonTimestampTooOld) != 1 || res.Rejected.Count(ReasonTimestampInFuture) != 2 {
		t.Fatalf("got %d too old and %d in the future, want 1 and 2",
			res.Rejected.Count(ReasonTimestampTooOld), res.Rejected.Count(ReasonTimestampInFuture))
	}
	msg := res.Rejected.Message()
	for _, part := range []string{"timestamp_too_old=1 (before 2026-10-25T12:00:00Z)", "timestamp_in_future=2 (after 2026-10-26T00:00:01Z)"} {
		if !strings.Contains(msg, part) {
			t.Errorf("message %q lacks %q", msg, part)
		}
	}
}

func TestTimeLimitsWindow(t *testing.T) {
	now := time.Date(2026, 10, 26, 15, 30, 0, 0, time.UTC)
	nanos := func(t time.Time) uint64 { return uint64(t.UnixNano()) }
	tests := map[string]struct {
		limits TimeLimits
		want   Window
	}{
		"no retention": {
			TimeLimits{MaxAge: 7 * 24 * time.Hour, MaxClockSkew: time.Hour},
			Window{From: nanos(now.Add(-7 * 24 * time.Hour)), To: nanos(now.Add(time.Hour))},
		},
		"retention longer than the maximum age": {
			TimeLimits{MaxAge: 7 * 24 * time.Hour, MaxClockSkew: time.Hour, RetentionDays: 30},
			Window{From: nanos(now.Add(-7 * 24 * time.Hour)), To: nanos(now.Add(time.Hour))},
		},
		// With a 2-day TTL, rows of yesterday are kept until tomorrow; older ones are deleted.
		"retention shorter than the maximum age": {
			TimeLimits{MaxAge: 7 * 24 * time.Hour, MaxClockSkew: time.Hour, RetentionDays: 2},
			Window{From: nanos(time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)), To: nanos(now.Add(time.Hour))},
		},
		"retention of one day": {
			TimeLimits{MaxAge: 7 * 24 * time.Hour, RetentionDays: 1},
			Window{From: nanos(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)), To: nanos(now)},
		},
	}
	for name, tt := range tests {
		if got := tt.limits.Window(now); got != tt.want {
			t.Errorf("%s: got %+v, want %+v", name, got, tt.want)
		}
	}
}

func TestTimeLimitsValidate(t *testing.T) {
	if err := (TimeLimits{MaxAge: 89 * 24 * time.Hour, MaxClockSkew: 24 * time.Hour}).Validate(); err != nil {
		t.Errorf("a 90-day window must be valid: %v", err)
	}
	for name, limits := range map[string]TimeLimits{
		"zero age":                 {MaxClockSkew: time.Hour},
		"negative skew":            {MaxAge: time.Hour, MaxClockSkew: -time.Second},
		"more than 100 partitions": {MaxAge: 100 * 24 * time.Hour},
		"age plus skew too long":   {MaxAge: 90 * 24 * time.Hour, MaxClockSkew: time.Second},
	} {
		if err := limits.Validate(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestMapEmptyRequest(t *testing.T) {
	res := Map(nil, anyTime, nil)
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
	for _, s := range Map(req, anyTime, nil).NewSeries {
		cache.Add([]series.Key{s.Key()})
	}
	b.ReportAllocs()
	for b.Loop() {
		res := Map(req, anyTime, cache)
		if len(res.NewSeries) != 0 {
			b.Fatal("cache should be warm")
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/10000, "ns/datapoint")
}
