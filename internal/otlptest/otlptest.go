// Package otlptest builds OTLP metric messages for tests and the load generator.
package otlptest

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Str returns a string attribute.
func Str(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

// Int returns an integer attribute.
func Int(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}}}
}

// Attrs is a shorthand for a slice of attributes.
func Attrs(kvs ...*commonpb.KeyValue) []*commonpb.KeyValue {
	return kvs
}

// Resource returns ResourceMetrics for a resource with a single instrumentation scope.
func Resource(resourceAttrs []*commonpb.KeyValue, metrics ...*metricspb.Metric) *metricspb.ResourceMetrics {
	return &metricspb.ResourceMetrics{
		Resource:  &resourcepb.Resource{Attributes: resourceAttrs},
		SchemaUrl: "https://opentelemetry.io/schemas/1.27.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope:   &commonpb.InstrumentationScope{Name: "otlptest", Version: "1.0.0"},
			Metrics: metrics,
		}},
	}
}

// Point returns a double data point.
func Point(timeUnixNano uint64, value float64, attrs ...*commonpb.KeyValue) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes:   attrs,
		TimeUnixNano: timeUnixNano,
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
	}
}

// Gauge returns a gauge metric.
func Gauge(name, unit string, points ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{
		Name: name,
		Unit: unit,
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: points}},
	}
}

// CumulativeSum returns a monotonic cumulative sum metric.
func CumulativeSum(name, unit string, points ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{
		Name: name,
		Unit: unit,
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints:             points,
		}},
	}
}

// Histogram returns a histogram metric with n data points.
func Histogram(name string, timeUnixNano uint64, n int) *metricspb.Metric {
	points := make([]*metricspb.HistogramDataPoint, n)
	for i := range points {
		points[i] = &metricspb.HistogramDataPoint{TimeUnixNano: timeUnixNano, Count: 1}
	}
	return &metricspb.Metric{
		Name: name,
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{DataPoints: points}},
	}
}

// LoadRequest builds a request with the given number of resources, gauge metrics per resource
// and data points (series) per metric, all at time timeUnixNano.
func LoadRequest(resources, metricsPerResource, pointsPerMetric int, timeUnixNano uint64) []*metricspb.ResourceMetrics {
	out := make([]*metricspb.ResourceMetrics, resources)
	for r := range resources {
		metrics := make([]*metricspb.Metric, metricsPerResource)
		for m := range metricsPerResource {
			points := make([]*metricspb.NumberDataPoint, pointsPerMetric)
			for p := range pointsPerMetric {
				points[p] = Point(timeUnixNano, float64(p), Int("worker", int64(p)), Str("pool", "default"))
			}
			metrics[m] = Gauge(fmt.Sprintf("load.metric.%d", m), "1", points...)
		}
		out[r] = Resource(Attrs(
			Str("service.name", fmt.Sprintf("load-service-%d", r)),
			Str("host.name", fmt.Sprintf("host-%d", r)),
			Str("deployment.environment.name", "test"),
		), metrics...)
	}
	return out
}
