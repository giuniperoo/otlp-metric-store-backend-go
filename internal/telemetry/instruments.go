// Package telemetry holds the OpenTelemetry setup of the service and the conventions for its
// own signals, so that all packages name their metrics and attributes the same way.
package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ScopeName is the instrumentation scope name of this service; packages append their path.
const ScopeName = "dash0.com/otlp-metric-store-backend"

// MetricPrefix namespaces all metrics of this service. It keeps the namespace of the starter
// code's com.dash0.homeexercise.metrics.received metric.
const MetricPrefix = "com.dash0.homeexercise."

// Outcome values, used as the "outcome" attribute.
const (
	OutcomeSuccess        = "success"
	OutcomePartialSuccess = "partial_success"
	OutcomeError          = "error"
	OutcomeOverloaded     = "overloaded"
)

// Outcome returns the "outcome" attribute.
func Outcome(v string) attribute.KeyValue {
	return attribute.String("outcome", v)
}

// Builder creates instruments with MetricPrefix and collects the first error, so constructors
// can declare all their instruments and check for errors once.
type Builder struct {
	meter metric.Meter
	err   error
}

// NewBuilder returns a Builder for the meter.
func NewBuilder(meter metric.Meter) *Builder {
	return &Builder{meter: meter}
}

// Err returns the first error that occurred while creating instruments.
func (b *Builder) Err() error {
	return b.err
}

// Counter creates a monotonic int64 counter.
func (b *Builder) Counter(name, unit, description string) metric.Int64Counter {
	c, err := b.meter.Int64Counter(MetricPrefix+name, metric.WithUnit(unit), metric.WithDescription(description))
	b.err = errors.Join(b.err, err)
	return c
}

// Histogram creates a float64 histogram with explicit bucket boundaries.
func (b *Builder) Histogram(name, unit, description string, bounds ...float64) metric.Float64Histogram {
	h, err := b.meter.Float64Histogram(MetricPrefix+name, metric.WithUnit(unit), metric.WithDescription(description),
		metric.WithExplicitBucketBoundaries(bounds...))
	b.err = errors.Join(b.err, err)
	return h
}

// IntHistogram creates an int64 histogram with explicit bucket boundaries.
func (b *Builder) IntHistogram(name, unit, description string, bounds ...float64) metric.Int64Histogram {
	h, err := b.meter.Int64Histogram(MetricPrefix+name, metric.WithUnit(unit), metric.WithDescription(description),
		metric.WithExplicitBucketBoundaries(bounds...))
	b.err = errors.Join(b.err, err)
	return h
}

// Gauge creates an asynchronous int64 gauge that reports the value of observe.
func (b *Builder) Gauge(name, unit, description string, observe func(context.Context) int64) {
	_, err := b.meter.Int64ObservableGauge(MetricPrefix+name, metric.WithUnit(unit), metric.WithDescription(description),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			o.Observe(observe(ctx))
			return nil
		}))
	b.err = errors.Join(b.err, err)
}
