-- Data points of one metric in a time range, with the identity of their series.
-- Parameters: from, to (DateTime64(9, 'UTC'), half-open range), metric (String).
--
-- The series are resolved first in the small lookup table; the IN filter then lets ClickHouse
-- read only those series within each hour of the data point table.
WITH matching AS (
    SELECT SeriesId, ServiceName, ResourceAttributes, Attributes
    FROM otel_metrics_series_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')})
    WHERE MetricName = {metric:String}
)
SELECT
    s.ServiceName AS ServiceName,
    s.ResourceAttributes AS ResourceAttributes,
    s.Attributes AS Attributes,
    p.TimeUnix AS TimeUnix,
    p.Value AS Value
FROM otel_metrics_number_points_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')}) AS p
INNER JOIN matching AS s ON p.SeriesId = s.SeriesId
WHERE p.SeriesId IN (SELECT SeriesId FROM matching)
ORDER BY ServiceName, TimeUnix
