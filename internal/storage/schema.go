package storage

import (
	"fmt"
	"strings"
)

// Table and view names.
const (
	SeriesTable             = "otel_metrics_series"
	NumberPointsTable       = "otel_metrics_number_points"
	SeriesInRangeView       = "otel_metrics_series_in_range"
	NumberPointsInRangeView = "otel_metrics_number_points_in_range"
)

// Schema design (see README.md, "Storage design", for the reasoning):
//
//   - Both tables are partitioned by UTC day, so a query for a time range only reads the
//     partitions of the days in that range, and retention drops whole partitions.
//   - The series table is a ReplacingMergeTree ordered by the series, so the repeated rows that
//     every ingest replica writes for an active series collapse into one row per series per day.
//     Date leads its sorting key (it is constant within a partition, so this is free) so that
//     time-range lookups use the primary key even without a metric name filter.
//   - The data point table is ordered by (hour, series, time). Starting the sorting key with the
//     hour lets the primary key prune by time even inside a day partition and even when a query
//     filters on time only; within an hour, the points of one series are contiguous, which keeps
//     both series lookups and compression (Delta for timestamps, Gorilla for values) efficient.
//   - All time columns are explicitly UTC, so partitioning never depends on the server time zone.
//   - ClickHouse uses the (hour, ...) sorting key only for conditions on toStartOfHour(TimeUnix),
//     not for conditions on TimeUnix alone. The parameterized views below are the read path:
//     they take the time range as parameters and add both the partition and the sorting key
//     conditions, so a caller cannot forget them. The integration tests run every documented
//     query with force_index_by_date and force_primary_key, which make ClickHouse refuse any
//     query that would read all partitions or all granules.

const createSeriesTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_series (
    Date Date CODEC(ZSTD(1)),
    SeriesId UInt64 CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit LowCardinality(String) CODEC(ZSTD(1)),
    MetricType Enum8('gauge' = 1, 'sum' = 2) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic Bool CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName LowCardinality(String) CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree
PARTITION BY Date
ORDER BY (Date, MetricName, ServiceName, SeriesId)
{{TTL}}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
`

const createNumberPointsTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_number_points (
    SeriesId UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    Value Float64 CODEC(Gorilla, ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (toStartOfHour(TimeUnix), SeriesId, TimeUnix)
{{TTL}}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
`

// createSeriesInRangeViewSQL returns the series that received data points in [from, to), one row
// per series with the identity recorded on the latest day in the range.
const createSeriesInRangeViewSQL = `
CREATE OR REPLACE VIEW otel_metrics_series_in_range AS
SELECT *
FROM otel_metrics_series
WHERE Date >= toDate({from:DateTime64(9, 'UTC')}) AND Date <= toDate({to:DateTime64(9, 'UTC')})
ORDER BY SeriesId, Date DESC
LIMIT 1 BY SeriesId
`

// createNumberPointsInRangeViewSQL returns the number data points with from <= TimeUnix < to.
const createNumberPointsInRangeViewSQL = `
CREATE OR REPLACE VIEW otel_metrics_number_points_in_range AS
SELECT *
FROM otel_metrics_number_points
WHERE TimeUnix >= {from:DateTime64(9, 'UTC')} AND TimeUnix < {to:DateTime64(9, 'UTC')}
  AND toStartOfHour(TimeUnix) >= toStartOfHour({from:DateTime64(9, 'UTC')})
  AND toStartOfHour(TimeUnix) <= toStartOfHour({to:DateTime64(9, 'UTC')})
`

// views are (re)created on every start. They hold no data, so replacing them is always safe.
var views = []struct{ name, ddl string }{
	{SeriesInRangeView, createSeriesInRangeViewSQL},
	{NumberPointsInRangeView, createNumberPointsInRangeViewSQL},
}

// tableSpec describes a table: how to create it and which columns the application expects.
type tableSpec struct {
	name    string
	ddl     string
	ttlExpr string
	columns []string
}

var tables = []tableSpec{
	{
		name:    SeriesTable,
		ddl:     createSeriesTableSQL,
		ttlExpr: "Date",
		columns: []string{
			"Date", "SeriesId", "MetricName", "MetricDescription", "MetricUnit", "MetricType",
			"AggregationTemporality", "IsMonotonic", "ServiceName", "ResourceAttributes",
			"ResourceSchemaUrl", "ScopeName", "ScopeVersion", "ScopeAttributes",
			"ScopeDroppedAttrCount", "ScopeSchemaUrl", "Attributes",
		},
	},
	{
		name:    NumberPointsTable,
		ddl:     createNumberPointsTableSQL,
		ttlExpr: "toDate(TimeUnix)",
		columns: []string{"SeriesId", "StartTimeUnix", "TimeUnix", "Value", "Flags"},
	},
}

// createTableSQL renders the DDL for a table with the given retention. A retention of 0 days
// keeps data forever.
func (t tableSpec) createTableSQL(retentionDays int) string {
	ttl := ""
	if retentionDays > 0 {
		ttl = fmt.Sprintf("TTL %s + INTERVAL %d DAY", t.ttlExpr, retentionDays)
	}
	return strings.Replace(t.ddl, "{{TTL}}", ttl, 1)
}
