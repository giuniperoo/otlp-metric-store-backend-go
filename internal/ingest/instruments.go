package ingest

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

type instruments struct {
	seriesWritten metric.Int64Counter
	pointsWritten metric.Int64Counter
	overloaded    metric.Int64Counter
	batches       metric.Int64Counter
	flushDuration metric.Float64Histogram
	batchWait     metric.Float64Histogram
	batchSize     metric.Int64Histogram
}

func newInstruments(w *Writer) (instruments, error) {
	meter := otel.Meter(telemetry.ScopeName + "/internal/ingest")
	var tel instruments
	var err error
	b := telemetry.NewBuilder(meter)
	tel.seriesWritten = b.Counter("series.written", "{series}",
		"Series lookup rows written to ClickHouse")
	tel.pointsWritten = b.Counter("data_points.written", "{data_point}",
		"Data points written to ClickHouse")
	tel.overloaded = b.Counter("ingest.overloaded", "{request}",
		"Export requests rejected by backpressure, by the limit reached (queue_size, pending_data_points)")
	tel.batches = b.Counter("ingest.batches", "{batch}",
		"Batches handed to flush workers, by the reason the batch was closed (size, delay, shutdown)")
	tel.flushDuration = b.Histogram("ingest.flush.duration", "s",
		"Duration of storing one batch (series rows and data points)",
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
	tel.batchWait = b.Histogram("ingest.batch.wait", "s",
		"Time from the first request of a batch until the batch starts being stored",
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2.5, 5)
	tel.batchSize = b.IntHistogram("ingest.batch.size", "{data_point}",
		"Data points per stored batch",
		100, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000)
	b.Gauge("ingest.queue.length", "{request}",
		"Export requests waiting to be added to a batch",
		func(context.Context) int64 { return w.queueLength() })
	b.Gauge("ingest.pending.data_points", "{data_point}",
		"Data points accepted and not yet stored",
		func(context.Context) int64 { return w.pendingPoints() })
	b.Gauge("series.cache.size", "{series}",
		"Series lookup rows remembered as written",
		func(context.Context) int64 { return int64(w.cache.Len()) })
	if err = b.Err(); err != nil {
		return instruments{}, err
	}
	return tel, nil
}

func (t instruments) batchFlushReason(reason string) {
	t.batches.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}
