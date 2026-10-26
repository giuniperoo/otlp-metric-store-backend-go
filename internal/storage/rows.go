// Package storage writes metric series and data points to ClickHouse.
//
// Two tables are used (see schema.go):
//
//   - otel_metrics_series, the series lookup table: one row per metric series per UTC day on
//     which the series received data points, holding the full identity of the series.
//   - otel_metrics_number_points: one row per OTLP NumberDataPoint (the data point type of
//     gauges and sums), holding only the series ID, timestamps, value and flags.
package storage

import (
	"context"
	"time"

	"dash0.com/otlp-metric-store-backend/internal/series"
)

// SeriesRow is one row of the series lookup table.
type SeriesRow struct {
	// Day is the UTC day on which the series received the data points that caused this row.
	Day      series.Day
	SeriesID series.ID

	MetricName             string
	MetricDescription      string
	MetricUnit             string
	MetricType             series.MetricType
	AggregationTemporality int32
	IsMonotonic            bool

	ServiceName        string
	ResourceAttributes map[string]string
	ResourceSchemaURL  string

	ScopeName             string
	ScopeVersion          string
	ScopeAttributes       map[string]string
	ScopeDroppedAttrCount uint32
	ScopeSchemaURL        string

	// Attributes are the data point attributes.
	Attributes map[string]string
}

// Key returns the key of the row in the series cache.
func (r *SeriesRow) Key() series.Key {
	return series.Key{ID: r.SeriesID, Day: r.Day}
}

// NumberPointRow is one row of the number data point table.
type NumberPointRow struct {
	SeriesID      series.ID
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// Store persists series rows and data points.
type Store interface {
	// InsertSeries writes series lookup rows. Writing the same row twice is harmless.
	InsertSeries(ctx context.Context, rows []SeriesRow) error
	// InsertNumberPoints writes number data points.
	InsertNumberPoints(ctx context.Context, rows []NumberPointRow) error
}
