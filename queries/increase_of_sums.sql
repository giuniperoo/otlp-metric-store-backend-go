-- Increase of every cumulative monotonic sum (counter) per series in a time range: the last
-- value minus the first value. Counter resets within the range are not handled here.
-- Parameters: from, to (DateTime64(9, 'UTC'), half-open range).
SELECT
    s.ServiceName AS ServiceName,
    s.MetricName AS MetricName,
    s.Attributes AS Attributes,
    argMax(p.Value, p.TimeUnix) - argMin(p.Value, p.TimeUnix) AS Increase
FROM otel_metrics_number_points_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')}) AS p
INNER JOIN (
    SELECT SeriesId, ServiceName, MetricName, Attributes
    FROM otel_metrics_series_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')})
    WHERE MetricType = 'sum' AND IsMonotonic AND AggregationTemporality = 2 -- cumulative
) AS s ON p.SeriesId = s.SeriesId
GROUP BY ServiceName, MetricName, Attributes
ORDER BY ServiceName, MetricName
