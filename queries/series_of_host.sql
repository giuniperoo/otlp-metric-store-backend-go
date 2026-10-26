-- The series a host reported in a time range, from the lookup table alone.
-- Parameters: from, to (DateTime64(9, 'UTC'), half-open range), host (String).
SELECT ServiceName, MetricName, MetricType, MetricUnit, Attributes
FROM otel_metrics_series_in_range(from = {from:DateTime64(9, 'UTC')}, to = {to:DateTime64(9, 'UTC')})
WHERE ResourceAttributes['host.name'] = {host:String}
ORDER BY ServiceName, MetricName
