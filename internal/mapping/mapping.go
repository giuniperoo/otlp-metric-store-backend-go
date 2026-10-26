// Package mapping converts OTLP metrics into series lookup rows and data point rows, and
// validates data points on the way.
package mapping

import (
	"math"
	"sync"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"dash0.com/otlp-metric-store-backend/internal/series"
	"dash0.com/otlp-metric-store-backend/internal/storage"
)

// KnownSeries reports which series lookup rows have already been written. It is implemented
// by [series.Cache].
type KnownSeries interface {
	// Missing returns the keys that are not known to be written yet.
	Missing(keys []series.Key) []series.Key
}

// Result is the outcome of mapping one export request.
type Result struct {
	// Points are the accepted data points.
	Points []storage.NumberPointRow
	// NewSeries holds one lookup row for every series and day of the accepted data points
	// that was not known yet.
	NewSeries []storage.SeriesRow

	// Metrics is the number of metrics in the request.
	Metrics int
	// DataPoints is the number of data points in the request, accepted or rejected.
	DataPoints int
	// SeriesKeys is the number of distinct series and days of the accepted data points; of
	// those, len(NewSeries) were not known yet.
	SeriesKeys int
	// Rejected counts the rejected data points by reason.
	Rejected Rejections
}

var hasherPool = sync.Pool{New: func() any { return series.NewHasher() }}

// pending refers to the OTLP messages a series lookup row is built from, so that rows (and
// their attribute maps) are only built for series that are not known yet.
type pending struct {
	key       series.Key
	rm        *metricspb.ResourceMetrics
	sm        *metricspb.ScopeMetrics
	metric    *metricspb.Metric
	metricID  series.Metric
	dataPoint *metricspb.NumberDataPoint
}

// Map converts the resource metrics of an export request. Data points of gauges and sums with
// a time in the accepted window are accepted; other metric types and invalid data points are
// counted in Result.Rejected. known may be nil, in which case every series is treated as new.
func Map(resourceMetrics []*metricspb.ResourceMetrics, accepted Window, known KnownSeries) Result {
	h := hasherPool.Get().(*series.Hasher)
	defer hasherPool.Put(h)

	// Size the outputs up front: requests carry thousands of data points, and growing the
	// slices and the map one append at a time dominates the allocations otherwise.
	capacity := countNumberDataPoints(resourceMetrics)
	var (
		res      = Result{Points: make([]storage.NumberPointRow, 0, capacity)}
		pendings = make([]pending, 0, capacity)
		index    = make(map[series.Key]int, capacity)
	)
	res.Rejected.window = accepted
	for _, rm := range resourceMetrics {
		resourceHash := h.Resource(rm.GetResource().GetAttributes(), rm.GetSchemaUrl())
		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeHash := h.Scope(scope.GetName(), scope.GetVersion(), scope.GetAttributes(), sm.GetSchemaUrl())
			for _, m := range sm.GetMetrics() {
				res.Metrics++
				dataPoints, metricID, ok := numberDataPoints(m, &res)
				if !ok {
					continue
				}
				res.DataPoints += len(dataPoints)
				if m.GetName() == "" {
					res.Rejected.add(ReasonMissingMetricName, len(dataPoints), "")
					continue
				}
				metricID.Name = m.GetName()
				metricID.Unit = m.GetUnit()
				metricHash := h.Metric(metricID)

				for _, dp := range dataPoints {
					timeNano, startNano := dp.GetTimeUnixNano(), dp.GetStartTimeUnixNano()
					if timeNano == 0 || timeNano > math.MaxInt64 || startNano > math.MaxInt64 {
						res.Rejected.add(ReasonInvalidTimestamp, 1, "")
						continue
					}
					if timeNano < accepted.From {
						res.Rejected.add(ReasonTimestampTooOld, 1, "")
						continue
					}
					if timeNano > accepted.To {
						res.Rejected.add(ReasonTimestampInFuture, 1, "")
						continue
					}
					value, ok := numberValue(dp)
					if !ok {
						res.Rejected.add(ReasonMissingValue, 1, "")
						continue
					}
					id := h.Series(resourceHash, scopeHash, metricHash, dp.GetAttributes())
					key := series.Key{ID: id, Day: series.DayOf(timeNano)}
					if _, ok := index[key]; !ok {
						index[key] = len(pendings)
						pendings = append(pendings, pending{key: key, rm: rm, sm: sm, metric: m, metricID: metricID, dataPoint: dp})
					}
					res.Points = append(res.Points, storage.NumberPointRow{
						SeriesID:      id,
						StartTimeUnix: time.Unix(0, int64(startNano)).UTC(),
						TimeUnix:      time.Unix(0, int64(timeNano)).UTC(),
						Value:         value,
						Flags:         dp.GetFlags(),
					})
				}
			}
		}
	}

	res.SeriesKeys = len(pendings)
	if len(pendings) == 0 {
		return res
	}
	keys := make([]series.Key, len(pendings))
	for i := range pendings {
		keys[i] = pendings[i].key
	}
	if known != nil {
		keys = known.Missing(keys)
	}
	res.NewSeries = buildSeriesRows(keys, index, pendings)
	return res
}

func countNumberDataPoints(resourceMetrics []*metricspb.ResourceMetrics) int {
	n := 0
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += len(m.GetGauge().GetDataPoints()) + len(m.GetSum().GetDataPoints())
			}
		}
	}
	return n
}

// numberDataPoints returns the data points and the type part of the identity of a gauge or
// sum. For other metric types it rejects their data points and returns ok == false.
func numberDataPoints(m *metricspb.Metric, res *Result) (_ []*metricspb.NumberDataPoint, _ series.Metric, ok bool) {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return data.Gauge.GetDataPoints(), series.Metric{Type: series.MetricTypeGauge}, true
	case *metricspb.Metric_Sum:
		return data.Sum.GetDataPoints(), series.Metric{
			Type:                   series.MetricTypeSum,
			AggregationTemporality: int32(data.Sum.GetAggregationTemporality()),
			IsMonotonic:            data.Sum.GetIsMonotonic(),
		}, true
	case *metricspb.Metric_Histogram:
		n := len(data.Histogram.GetDataPoints())
		res.DataPoints += n
		res.Rejected.add(ReasonUnsupportedMetricType, n, "histogram")
	case *metricspb.Metric_ExponentialHistogram:
		n := len(data.ExponentialHistogram.GetDataPoints())
		res.DataPoints += n
		res.Rejected.add(ReasonUnsupportedMetricType, n, "exponential_histogram")
	case *metricspb.Metric_Summary:
		n := len(data.Summary.GetDataPoints())
		res.DataPoints += n
		res.Rejected.add(ReasonUnsupportedMetricType, n, "summary")
	}
	// A metric without data has no data points to store or reject.
	return nil, series.Metric{}, false
}

func numberValue(dp *metricspb.NumberDataPoint) (float64, bool) {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble, true
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt), true
	default:
		return 0, false
	}
}

// buildSeriesRows builds the lookup rows for the given keys. Resource and scope attribute maps
// are built once per ResourceMetrics and ScopeMetrics and shared between rows.
func buildSeriesRows(keys []series.Key, index map[series.Key]int, pendings []pending) []storage.SeriesRow {
	if len(keys) == 0 {
		return nil
	}
	resourceAttrs := make(map[*metricspb.ResourceMetrics]map[string]string)
	scopeAttrs := make(map[*metricspb.ScopeMetrics]map[string]string)
	rows := make([]storage.SeriesRow, 0, len(keys))
	for _, key := range keys {
		p := pendings[index[key]]
		resAttrs, ok := resourceAttrs[p.rm]
		if !ok {
			resAttrs = series.AttributeMap(p.rm.GetResource().GetAttributes())
			resourceAttrs[p.rm] = resAttrs
		}
		scope := p.sm.GetScope()
		scAttrs, ok := scopeAttrs[p.sm]
		if !ok {
			scAttrs = series.AttributeMap(scope.GetAttributes())
			scopeAttrs[p.sm] = scAttrs
		}
		rows = append(rows, storage.SeriesRow{
			Day:                    key.Day,
			SeriesID:               key.ID,
			MetricName:             p.metricID.Name,
			MetricDescription:      p.metric.GetDescription(),
			MetricUnit:             p.metricID.Unit,
			MetricType:             p.metricID.Type,
			AggregationTemporality: p.metricID.AggregationTemporality,
			IsMonotonic:            p.metricID.IsMonotonic,
			ServiceName:            resAttrs[serviceNameKey],
			ResourceAttributes:     resAttrs,
			ResourceSchemaURL:      p.rm.GetSchemaUrl(),
			ScopeName:              scope.GetName(),
			ScopeVersion:           scope.GetVersion(),
			ScopeAttributes:        scAttrs,
			ScopeDroppedAttrCount:  scope.GetDroppedAttributesCount(),
			ScopeSchemaURL:         p.sm.GetSchemaUrl(),
			Attributes:             series.AttributeMap(p.dataPoint.GetAttributes()),
		})
	}
	return rows
}

// serviceNameKey is the semantic convention attribute for the service name. ServiceName is
// stored in its own column because it is the most common filter and grouping.
const serviceNameKey = "service.name"
