# OTLP Metric Store (Go)

A backend that receives OpenTelemetry metrics over OTLP/gRPC and stores gauges and sums in
ClickHouse. The identity of every time series (resource, scope, metric and data point
attributes) is written once to a **series lookup table**. Each data point is stored as a
**series ID, timestamps, value and flags**.

Built on the Dash0 take-home scaffold ([otlp-metric-store-backend-go](https://github.com/dash0hq/take-home-assignments/tree/main/otlp-metric-store-backend-go)).

- [Quick start](#quick-start)
- [What changed from the scaffold](#what-changed-from-the-scaffold)
- [Terminology](#terminology)
- [Storage design](#storage-design)
- [Ingest path](#ingest-path)
- [Operating it](#operating-it)
- [Validation](#validation)
- [Trade-offs and next steps](#trade-offs-and-next-steps)

## Quick start

Requirements: Go 1.26, and Docker for ClickHouse and the integration tests.

```shell
# ClickHouse and the service
docker compose up --build -d

# send synthetic gauges for 10 seconds and print throughput and latency
go run ./tools/loadgen -duration 10s

# query: data points per metric and service in the last hour
docker compose exec clickhouse clickhouse-client --password dev --query "
  SELECT s.MetricName, s.ServiceName, count() AS points
  FROM otel_metrics_number_points_in_range(from = now64(9) - INTERVAL 1 HOUR, to = now64(9)) AS p
  JOIN otel_metrics_series_in_range(from = now64(9) - INTERVAL 1 HOUR, to = now64(9)) AS s USING SeriesId
  GROUP BY ALL ORDER BY ALL LIMIT 10"
```

Without Docker for the service, against any ClickHouse:

```shell
CLICKHOUSE_ADDR=localhost:9000 CLICKHOUSE_PASSWORD=dev go run .
```

Tests:

```shell
go test -race ./...                      # unit tests
go test -tags integration ./...          # integration tests; starts ClickHouse 26.2 with testcontainers
CLICKHOUSE_ADDR=localhost:9000 CLICKHOUSE_PASSWORD=dev go test -tags integration ./...   # or use a running one
```

`go run ./...` from the scaffold's README no longer works, because the repository now contains
two commands (the service and `tools/loadgen`). Use `go run .` instead.

## What changed from the scaffold

- **The lookup table design** described below replaces the wide `otel_metrics_gauge` and
  `otel_metrics_sum` tables. Gauges and sums share the OTLP `NumberDataPoint`, so they share one
  data point table, and the metric type is recorded in the series table.
- **Batching across requests**, backpressure, and acknowledgements only after data is stored.
- **OTLP partial success** for invalid data points and unsupported metric types (histograms,
  exponential histograms, summaries), reported with reasons instead of being dropped silently.
- **The service's own telemetry**: metrics, traces and logs for every stage, exportable over
  OTLP (for example to Dash0).
- **Production wiring**: configuration from environment variables, gRPC health service, gzip
  support, graceful shutdown, schema migration with a compatibility check, a Dockerfile,
  docker-compose and CI.

Issues in the scaffold, fixed along the way:

| Issue | Effect | Fix |
|---|---|---|
| `server.go` registers the service with a `nil` store | the running application accepted metrics and stored nothing; only the tests wired ClickHouse | the store is always wired |
| `metrics.received` was incremented once per request | the metric counted requests, not metrics | counts metrics; requests are counted by `exports` |
| double attribute values formatted with `%g` | `1234567.25` was stored as `1.23457e+06` | shortest exact representation |
| failed `batch.Append` returned without `Abort` | every failed append leaked a pooled ClickHouse connection | batches are aborted on error |
| module named `otlp-log-processor-backend` | inconsistent naming | `otlp-metric-store-backend` |

## Terminology

These terms are used consistently in code, table names, logs and metric names.

| Term | Meaning |
|---|---|
| **data point** | One OTLP `NumberDataPoint`: a value at a time. |
| **series** | One time series: the unique combination of resource, instrumentation scope, metric (name, unit, type, temporality, monotonicity) and data point attributes. |
| **series ID** (`SeriesId`) | 64-bit hash of a series' identity; the reference from data points to the lookup table. |
| **series row** | One row of the lookup table: a series on a UTC day on which it received data points. |
| **batch** | The rows of many export requests that are inserted together. |
| **flush** | Storing one batch: first its new series rows, then its data points. |
| **rejected** | A data point that is not stored, reported back in the OTLP partial success with a **reason**. |

## Storage design

### Tables

```
otel_metrics_series                         otel_metrics_number_points
(series lookup, ReplacingMergeTree)         (data points, MergeTree)
┌─────────────────────────────────┐         ┌──────────────────────────────┐
│ Date            Date            │         │ SeriesId       UInt64        │──┐
│ SeriesId        UInt64          │◀────────│ StartTimeUnix  DateTime64(9) │  │
│ MetricName, MetricUnit,         │         │ TimeUnix       DateTime64(9) │  │ ~2.4 bytes per
│ MetricType, MetricDescription,  │         │ Value          Float64       │  │ data point on disk
│ AggregationTemporality,         │         │ Flags          UInt32        │──┘
│ IsMonotonic                     │         └──────────────────────────────┘
│ ServiceName, ResourceAttributes,│         PARTITION BY toDate(TimeUnix)
│ ResourceSchemaUrl               │         ORDER BY (toStartOfHour(TimeUnix),
│ ScopeName, ScopeVersion,        │                   SeriesId, TimeUnix)
│ ScopeAttributes, ...            │
│ Attributes (data point)         │
└─────────────────────────────────┘
PARTITION BY Date
ORDER BY (Date, MetricName, ServiceName, SeriesId)
```

Full DDL: [`internal/storage/schema.go`](internal/storage/schema.go).

### The reference between the tables

The series ID is a **hash of the series identity, computed in the service**
([`internal/series`](internal/series)). There is no auto-increment ID and no lookup round trip:

- Ingestion never reads from ClickHouse. Every replica computes the same ID for the same series
  without coordinating, so the service scales horizontally.
- The hash is computed from exactly the values stored in the series row: attributes are sorted
  by key, and values are converted to their stored strings, so attribute order or duplicate
  keys can't produce different IDs for what is stored as the same row. Every string is
  length-prefixed so different identities never encode to the same bytes, and a version byte
  allows the encoding to change later without colliding with old IDs.
- Resource and scope are hashed once per `ResourceMetrics` and `ScopeMetrics` and combined with
  each data point's attributes: about 180 ns and no allocations per data point.
- **The metric description is not part of the identity**, so changing a description does not
  split a series. The latest description is stored with the series.
- 64 bits: with 10 million series, the probability of any collision is about 3 × 10⁻⁶. A
  collision would merge two series. A 128-bit ID would remove that risk for 8 more bytes per
  data point before compression.

### Why one series row per day

A series row is written for **every UTC day on which the series received data points**, not
once forever:

- **Lookups are time-bounded too.** To find the series of a time range, ClickHouse reads only
  the series table's partitions for those days, never the whole table.
- **Resource attributes change over time** (a new `service.version`, a new pod name). Each
  change is a new series, so old series stop getting rows after their last day, and the lookup
  table holds only series that were active in the queried range.
- **Retention works the same for both tables.** The same TTL drops whole day partitions from
  both tables (`ttl_only_drop_parts`), so no series row ever outlives its data points by more
  than a day.

The table is a `ReplacingMergeTree` keyed by `(Date, …, SeriesId)`. Writing the same series
row twice is harmless: duplicates are merged away. This is what makes the in-memory series
cache an optimisation rather than a correctness requirement.

### Never scanning a full table

The task says data is always queried for a time range, with no other mandatory filter.

- Both tables are **partitioned by day**, so a time range reads only its days.
- The data point table's sorting key **starts with the hour**, so even inside a day partition
  the primary key narrows the read to the hours of the range. A time-only query over one hour
  reads 1 of 8 granules in a day of test data. Within an hour, the points of one series are
  contiguous, so lookups by series are binary searches, and the timestamp codec (Delta) and
  value codec (Gorilla) compress well.
- ClickHouse uses an `(toStartOfHour(TimeUnix), …)` key only for conditions written on
  `toStartOfHour(TimeUnix)`, not for conditions on `TimeUnix` alone (verified on 26.2). So the
  **read path is two parameterized views** that take the range and add every condition the
  indexes need, which means callers can't forget them:

  ```sql
  SELECT … FROM otel_metrics_number_points_in_range(from = …, to = …)   -- from <= TimeUnix < to
  SELECT … FROM otel_metrics_series_in_range(from = …, to = …)          -- one row per series
  ```

- **This is enforced in the tests.** Every query in [`queries/`](queries) runs in the
  integration tests with `force_index_by_date = 1` and `force_primary_key = 1`. With these
  settings ClickHouse refuses any query that can't prune by partition and by primary key. The
  test also checks that a query without a time range is refused.

### Example queries

| Query | Filter |
|---|---|
| [`points_of_metric.sql`](queries/points_of_metric.sql) | time range + metric name: series resolved in the lookup table first, then only those series read per hour |
| [`data_points_per_metric.sql`](queries/data_points_per_metric.sql) | time range only |
| [`series_of_host.sql`](queries/series_of_host.sql) | time range + resource attribute, lookup table only (bloom filter indexes on attribute keys and values) |
| [`increase_of_sums.sql`](queries/increase_of_sums.sql) | time range, cumulative monotonic sums |

## Ingest path

```
 OTLP exporters                                     ClickHouse
      │ Export (gRPC, gzip)                             ▲   ▲
      ▼                                                 │   │ 2. data points
 MetricsService ── map + validate ──▶ Writer queue ──▶ batch ──▶ flush workers
  (per request)    hash series IDs    (bounded)       (100k points    │ 1. new series rows
                   skip cached series                  or 200 ms)     ▼
      ▲            count rejections                                 series cache
      └──────── response after the batch is stored ◀─────────────── (marked after insert)
```

- **Batching.** ClickHouse wants few, large inserts: every insert creates a part that has to
  be merged, and one insert per export request would produce thousands of parts a second.
  The writer merges the rows of all concurrent requests and flushes when a batch reaches 100,000
  data points or 200 ms after its first request. 200 ms matches ClickHouse's own async-insert
  window, so batching adds at most that much latency, and inserts happen about 5 times a
  second per worker.
- **Acknowledged when stored.** `Export` returns only after its batch is stored, so a success
  response means the data is in ClickHouse. Delivery is **at least once**: when a request fails
  or times out, the exporter retries, and data points that were stored anyway are stored twice.
- **Series before data points.** A flush inserts the batch's new series rows first, then its
  data points, so a data point never refers to a series that isn't stored. Series are added
  to the cache only after their insert succeeds. Otherwise a failed insert would be skipped
  forever.
- **Series cache.** A bounded, two-generation in-memory set of the (series, day) rows already
  written. With a warm cache, series rows are only built and written for new series. Lookups
  for a whole request take one lock. Losing the cache (restart, second replica) only causes
  duplicate series rows, which the ReplacingMergeTree merges.
- **Backpressure.** Memory is bounded by data, not only by request count. At most 2 million
  data points may be accepted and not yet stored, about 150 MB of rows (a single request can
  carry hundreds of thousands), and at most 1,000 requests may wait for a batch. When either
  limit is reached, `Export` fails at once with `UNAVAILABLE` and a `RetryInfo`. OTLP
  exporters treat that as retryable and back off, instead of the service buffering until it
  runs out of memory. A request is always admitted when nothing is pending, so an oversized
  request can't be rejected forever.

### Responses

| Situation | Response |
|---|---|
| all data points stored | success |
| some data points invalid or unsupported | success with `partial_success`: the rejected count and the reasons, e.g. `rejected 4 data points: unsupported_metric_type=4 (histogram; only gauge and sum are supported)`. The valid data points are stored. |
| queue full, ClickHouse failing, shutting down | `UNAVAILABLE` with `RetryInfo` (retried by OTLP exporters). Internal errors are logged, not sent to the client. |
| client deadline passed before the batch was stored | `DEADLINE_EXCEEDED` (the data may still be stored; see at-least-once above) |

Rejection reasons: `unsupported_metric_type`, `missing_metric_name`, `invalid_timestamp` (zero,
or beyond what `DateTime64(9)` can hold), `missing_value`.

## Operating it

### Configuration

Every flag can also be set with an environment variable in upper snake case. Flags win over
environment variables.

| Flag / environment variable | Default | |
|---|---|---|
| `-listenAddr` / `LISTEN_ADDR` | `localhost:4317` | OTLP/gRPC listen address (`0.0.0.0:4317` in the container image) |
| `-maxReceiveMessageSize` / `MAX_RECEIVE_MESSAGE_SIZE` | 16 MiB | |
| `-clickhouseAddr` / `CLICKHOUSE_ADDR` | `localhost:9000` | native protocol |
| `-clickhouseDatabase`, `-clickhouseUsername` | `default` | |
| `CLICKHOUSE_PASSWORD` | | environment variable only, so it doesn't show up in process listings |
| `-clickhouseMaxOpenConns` | 10 | |
| `-migrate` | `true` | create missing tables and views on startup and check existing tables |
| `-retentionDays` | 30 | TTL for new tables; 0 keeps data forever |
| `-batchMaxDataPoints` | 100000 | |
| `-batchMaxDelay` | 200ms | upper bound on the latency batching adds |
| `-writeQueueSize` | 1000 | requests waiting for a batch before new ones get `UNAVAILABLE` |
| `-maxPendingDataPoints` | 2000000 | data points accepted but not yet stored before new requests get `UNAVAILABLE`; bounds memory |
| `-flushWorkers` | 2 | batches stored concurrently |
| `-flushTimeout` | 30s | |
| `-seriesCacheSize` | 1000000 | (series, day) rows remembered; 0 disables the cache |
| `-shutdownTimeout` | 30s | |
| `-telemetryExporter` | `otlp` if `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else `stdout` | `otlp`, `stdout` or `none` |
| `-logLevel` | `info` | |

The service's own telemetry honours the standard `OTEL_*` variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRIC_EXPORT_INTERVAL`,
`OTEL_TRACES_SAMPLER`). To send it to Dash0, set the endpoint and an `Authorization` header.

### Signals

**Metrics** (prefix `com.dash0.homeexercise.`, kept from the scaffold):

| Metric | Attributes | Answers |
|---|---|---|
| `exports` | `outcome`: success, partial_success, overloaded, error | Are clients succeeding? |
| `metrics.received`, `data_points.received` | | Incoming volume |
| `data_points.rejected` | `reason` | What are clients sending that we drop? |
| `data_points.written`, `series.written` | | What reached ClickHouse |
| `series.cache.lookups` | `result`: hit, miss | Is the cache working, and how many new series appear? (A miss storm means a cardinality explosion.) |
| `series.cache.size` | | Cache memory |
| `ingest.queue.length`, `ingest.pending.data_points` | | Saturation: approaching `writeQueueSize` or `maxPendingDataPoints` means rejections are imminent |
| `ingest.overloaded` | `limit`: queue_size, pending_data_points | Requests rejected by backpressure, and which limit was reached |
| `ingest.batches` | `reason`: size, delay, shutdown | Batches mostly closing on `size` means `batchMaxDataPoints` is the limit |
| `ingest.batch.size`, `ingest.batch.wait`, `ingest.flush.duration` | `outcome` | Insert sizes and latency, ClickHouse health |

Plus the gRPC server metrics from `otelgrpc` (`rpc.server.call.duration` by status code, request
sizes).

**Traces:** a span per `Export` call (from `otelgrpc`), with the number of metrics, data points,
rejected data points and new series. Each flush is its own trace, `ingest.flush`, **linked to the
spans of all the export requests it stored**, so you can go from a slow export to the insert it
waited for.

**Logs:** JSON on stderr, and exported over OTLP with trace correlation when the OTLP exporter is
on. A failed flush is logged once per batch with its sizes and duration. Rejected data points
are logged with their reasons. Startup logs the effective configuration (without the
password).

**Health:** the standard gRPC health service. It reports `NOT_SERVING` as soon as shutdown
starts, so load balancers stop sending traffic.

**Shutdown** (SIGTERM): stop accepting requests, let in-flight requests finish (they wait for
their batch), flush everything pending, close ClickHouse, then flush the service's own
telemetry, all within `-shutdownTimeout`.

**Schema:** on startup, missing tables are created and the columns of existing tables are
checked. A table left by an incompatible version fails startup with a message naming the
missing columns, instead of every insert failing later.

## Validation

| | What it shows |
|---|---|
| `internal/series` tests | series IDs are deterministic, ignore attribute order, change with every identity field (all 15 cases), don't change with the description, match the stored maps for duplicate keys; the cache stays bounded and keeps active series |
| `internal/mapping` tests | gauge and sum mapping, one series row per series and day, cached series skipped, resource changes create new series, every rejection reason counted |
| `internal/ingest` tests | series stored before data points, concurrent requests merged into one insert, full batches flushed without waiting, store errors reported to every waiting request and failed series not cached, backpressure by queue size and by pending data points, context deadlines, flush on close (run with `-race`) |
| `internal/server` tests | partial success, status codes and `RetryInfo` per failure, internal errors not leaked to clients |
| integration tests (`-tags integration`) | the full service against ClickHouse 26.2: migration (idempotent, detects incompatible tables, TTL), series written once despite repeated exports, idempotent across restarts, exact round trip of nanosecond timestamps, values and flags, partial success end to end, and **every documented query passing `force_index_by_date` + `force_primary_key`** |

### Performance

Measured on a 2-vCPU sandbox with ClickHouse, the service and the load generator all on the
same machine, so they share the CPUs. Read these as lower bounds.

- **Mapping:** about 420 ns per data point, 36 allocations per 10,000-point request
  (`go test -bench . ./internal/mapping`). One core maps about 2.4 million data points a
  second.
- **End to end:** **380,000–450,000 data points per second** across runs (64 concurrent exporters, 5,000
  data points per request, `tools/loadgen`) with no errors. Latency p50 is 0.67 s; it is
  dominated by queueing on the saturated CPUs plus the batch window.
- **Storage:** 26 million data points took **2.6 bytes per data point** including the series
  table. The same data in the scaffold's wide schema took 7.6 bytes per data point: **2.9×
  less disk, and 7× less data to decompress** (600 MiB against 4.2 GiB). The load generator's
  attribute sets are small; real ones are larger, which widens the gap.

### A bug found under load

Load testing showed about one batch in 60 failing with `use of closed network connection`.
Cause: clickhouse-go v2.43 watches the context of each insert in a goroutine. When the context
is cancelled right after `Send` returns, that goroutine can still see the cancellation after
the connection went back to the pool, and closes it while the next insert is using it. The
flush context is therefore left to expire on its timeout instead of being cancelled
immediately. An A/B test under the same load: 2 failed batches per 30 s run before, 0 after.
This is worth reporting upstream.

## Trade-offs and next steps

- **Histograms, exponential histograms and summaries.** They're rejected with a clear reason
  today. They need their own data point tables (bucket counts as arrays), but use the same
  series table and IDs: add the metric types to the `MetricType` enum and a table per data
  point type.
- **Exactly-once.** Retries after timeouts can duplicate data points. With
  ReplicatedMergeTree, a deterministic `insert_deduplication_token` per batch, or idempotency
  keys from clients, the duplicates could be dropped. Alternatively, a ReplacingMergeTree on
  `(SeriesId, TimeUnix)` dedups at merge time.
- **Durability while batching.** Requests are acknowledged only after they are stored, so
  nothing acknowledged is lost on a crash, but a crash fails the in-flight requests, which the
  exporters retry. A write-ahead log or a queue such as Kafka in front would decouple ingest
  from ClickHouse availability. The cost is operational complexity.
- **Normalising resources.** Series rows repeat the resource attributes of all their series.
  That's fine at low cardinality, and the table is small, compressed and written about once a
  day per series. For very large resources, a third `resources` table keyed by a resource hash
  would store each resource once.
- **Clustering.** The schema works unchanged on `Replicated*` engines behind `Distributed`
  tables, sharded by `SeriesId`, so a series' points live on one shard.
- **Schema evolution.** `CREATE TABLE IF NOT EXISTS` with a column check is enough for one
  version. Real changes (TTL, new columns) want versioned migrations (e.g. goose or
  golang-migrate).
- **Cardinality protection.** A per-tenant or per-metric limit on new series per day, using
  the `series.cache.lookups{result=miss}` signal that already exists.
- **Rollups.** Materialized views into per-minute or per-hour aggregates for long-range queries.

## Project layout

```
main.go, config.go        wiring, configuration, graceful shutdown
internal/series           series identity: ID hashing, attribute encoding, series cache
internal/mapping          OTLP → series rows + data point rows, validation and rejection reasons
internal/storage          ClickHouse schema, migrations, inserts
internal/ingest           cross-request batching, backpressure, flush ordering
internal/server           OTLP MetricsService: partial success and status codes
internal/telemetry        the service's own telemetry setup and naming conventions
internal/otlptest         OTLP builders for tests and the load generator
queries/                  documented, tested read queries
tools/loadgen             load generator
```
