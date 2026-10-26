// Package series defines metric series identity.
//
// A metric series is one time series: the unique combination of
//
//   - the resource (its attributes and schema URL),
//   - the instrumentation scope (name, version, attributes, schema URL),
//   - the metric (name, unit, type and, for sums, temporality and monotonicity),
//   - the data point attributes.
//
// Every data point belongs to exactly one series. The series is described once in the
// series lookup table and every data point refers to it by its [ID].
//
// The metric description and the dropped-attribute counts are deliberately not part of the
// identity: they are informational, and changing a description must not split a series in two.
package series

import (
	"time"
)

// ID identifies a metric series. It is a 64-bit xxHash of the canonical encoding of the
// series identity (see [Hasher]), computed from exactly the values that are stored in the
// series lookup table. Because the ID is derived from the data, every ingest replica computes
// the same ID for the same series without coordinating with the database or with each other.
type ID uint64

// MetricType is the OTLP metric data type of a series.
type MetricType int8

// Supported metric types. The values match the Enum8 in the series table schema.
const (
	MetricTypeGauge MetricType = 1
	MetricTypeSum   MetricType = 2
)

// String returns the name used for the metric type in the database and in telemetry.
func (t MetricType) String() string {
	switch t {
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeSum:
		return "sum"
	default:
		return "unknown"
	}
}

// Day is a UTC calendar day, counted in days since the Unix epoch. The series lookup table
// records each series once per day on which it received data points.
type Day int32

const nanosPerDay = uint64(24 * time.Hour)

// DayOf returns the UTC day containing the given Unix timestamp in nanoseconds.
func DayOf(unixNano uint64) Day {
	return Day(unixNano / nanosPerDay)
}

// Time returns midnight UTC at the start of the day.
func (d Day) Time() time.Time {
	return time.Unix(int64(d)*86400, 0).UTC()
}

// Key identifies one row of the series lookup table: a series on a given day.
type Key struct {
	ID  ID
	Day Day
}
