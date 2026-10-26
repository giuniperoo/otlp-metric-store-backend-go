-- Number of data points and average value per metric and service in a time range: a query
-- with no filter other than the time range.
-- Parameters: from, to (DateTime64(9, 'UTC'), half-open range).
SELECT
    s.MetricName AS MetricName,
    s.ServiceName AS ServiceName,
    count() AS DataPoints,
    avg(p.Value) AS AvgValue
FROM otel_metrics_number_points_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')}) AS p
INNER JOIN otel_metrics_series_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')}) AS s
    ON p.SeriesId = s.SeriesId
GROUP BY MetricName, ServiceName
ORDER BY MetricName, ServiceName
