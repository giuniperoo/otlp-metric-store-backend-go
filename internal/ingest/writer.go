// Package ingest batches the rows of many concurrent export requests into large ClickHouse
// inserts.
//
// ClickHouse is built for few, large inserts: every insert creates a part on disk that has to
// be merged later, so one insert per export request (thousands per second) would overload it.
// The Writer collects the rows of all requests into a batch and inserts the batch when it
// reaches MaxBatchPoints data points or when MaxBatchDelay has passed since its first request.
//
// Delivery is at least once: Write returns only after the batch holding its rows has been
// stored, so a successful export response means the data is in ClickHouse. If a request fails
// or times out, the client retries it, which can store its data points twice.
//
// Backpressure: the number of requests waiting for a batch is bounded by QueueSize. When the
// queue is full, Write fails immediately with ErrOverloaded instead of piling up memory; the
// server turns that into a retryable gRPC status, so OTLP clients back off and retry.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"dash0.com/otlp-metric-store-backend/internal/series"
	"dash0.com/otlp-metric-store-backend/internal/storage"
	"dash0.com/otlp-metric-store-backend/internal/telemetry"
)

var (
	// ErrOverloaded is returned by Write when the queue of pending requests is full.
	ErrOverloaded = errors.New("ingest: write queue is full")
	// ErrClosed is returned by Write after Close was called.
	ErrClosed = errors.New("ingest: writer is closed")
)

// Config configures a Writer.
type Config struct {
	// MaxBatchPoints is the number of data points at which a batch is inserted.
	MaxBatchPoints int
	// MaxBatchDelay is the longest a request waits for its batch to fill up before the batch
	// is inserted anyway. It bounds the latency added by batching.
	MaxBatchDelay time.Duration
	// QueueSize is the number of requests that may wait to be added to a batch.
	QueueSize int
	// FlushWorkers is the number of batches inserted concurrently.
	FlushWorkers int
	// FlushTimeout bounds the inserts of one batch.
	FlushTimeout time.Duration
}

// DefaultConfig returns production defaults: at most ~5 inserts per second per worker into
// each table, each of up to 100k data points.
func DefaultConfig() Config {
	return Config{
		MaxBatchPoints: 100_000,
		MaxBatchDelay:  200 * time.Millisecond,
		QueueSize:      1_000,
		FlushWorkers:   2,
		FlushTimeout:   30 * time.Second,
	}
}

func (c Config) validate() error {
	if c.MaxBatchPoints <= 0 || c.MaxBatchDelay <= 0 || c.QueueSize <= 0 || c.FlushWorkers <= 0 || c.FlushTimeout <= 0 {
		return fmt.Errorf("ingest: all writer settings must be positive, got %+v", c)
	}
	return nil
}

// Writer batches series rows and data points and writes them to a Store.
type Writer struct {
	cfg   Config
	store storage.Store
	cache *series.Cache
	log   *slog.Logger
	tel   instruments

	mu       sync.RWMutex // guards closed and sending on requests
	closed   bool
	requests chan *request
	batches  chan *batch
	done     chan struct{} // closed when all batches have been flushed
}

type request struct {
	series []storage.SeriesRow
	points []storage.NumberPointRow
	span   trace.SpanContext
	result chan error // buffered, receives exactly one value
}

// NewWriter starts a Writer. Call Close to flush pending data and stop it.
func NewWriter(cfg Config, store storage.Store, cache *series.Cache, log *slog.Logger) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	w := &Writer{
		cfg:      cfg,
		store:    store,
		cache:    cache,
		log:      log,
		requests: make(chan *request, cfg.QueueSize),
		batches:  make(chan *batch),
		done:     make(chan struct{}),
	}
	tel, err := newInstruments(w)
	if err != nil {
		return nil, err
	}
	w.tel = tel

	var workers sync.WaitGroup
	for range cfg.FlushWorkers {
		workers.Go(w.flushLoop)
	}
	go w.batchLoop()
	go func() {
		workers.Wait()
		close(w.done)
	}()
	return w, nil
}

// Write adds the rows of one export request to a batch and waits until the batch is stored.
// New series rows are always stored before the data points that refer to them.
//
// If ctx ends first, Write returns ctx.Err(); the rows may still be stored.
func (w *Writer) Write(ctx context.Context, newSeries []storage.SeriesRow, points []storage.NumberPointRow) error {
	if len(newSeries) == 0 && len(points) == 0 {
		return nil
	}
	req := &request{
		series: newSeries,
		points: points,
		span:   trace.SpanContextFromContext(ctx),
		result: make(chan error, 1),
	}

	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return ErrClosed
	}
	select {
	case w.requests <- req:
		w.mu.RUnlock()
	default:
		w.mu.RUnlock()
		w.tel.overloaded.Add(ctx, 1)
		return ErrOverloaded
	}

	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting writes, stores all pending rows and waits until that is done or ctx
// ends.
func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.requests)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ingest: pending batches not flushed before shutdown deadline: %w", ctx.Err())
	}
}

// batch is a set of requests whose rows are inserted together.
type batch struct {
	requests []*request
	series   []storage.SeriesRow
	points   []storage.NumberPointRow
	seen     map[series.Key]struct{}
	started  time.Time
}

func (b *batch) add(r *request) {
	b.requests = append(b.requests, r)
	b.points = append(b.points, r.points...)
	for _, s := range r.series {
		// Concurrent requests often carry the same new series; insert it once.
		if _, ok := b.seen[s.Key()]; ok {
			continue
		}
		b.seen[s.Key()] = struct{}{}
		b.series = append(b.series, s)
	}
}

// batchLoop collects requests into batches and hands full or expired batches to the flush
// workers. While all workers are busy it blocks, the request queue fills up, and Write starts
// returning ErrOverloaded: that is the backpressure.
func (w *Writer) batchLoop() {
	defer close(w.batches)
	var current *batch
	timer := time.NewTimer(w.cfg.MaxBatchDelay)
	timer.Stop()
	flush := func(reason string) {
		w.tel.batchFlushReason(reason)
		w.batches <- current
		current = nil
		timer.Stop()
	}
	for {
		select {
		case req, ok := <-w.requests:
			if !ok {
				if current != nil {
					flush("shutdown")
				}
				return
			}
			if current == nil {
				current = &batch{seen: make(map[series.Key]struct{}), started: time.Now()}
				timer.Reset(w.cfg.MaxBatchDelay)
			}
			current.add(req)
			if len(current.points) >= w.cfg.MaxBatchPoints {
				flush("size")
			}
		case <-timer.C:
			if current != nil {
				flush("delay")
			}
		}
	}
}

func (w *Writer) flushLoop() {
	for b := range w.batches {
		w.flush(b)
	}
}

// flush stores a batch: series rows first, so that a data point never refers to a series that
// is not stored, then data points. Every request of the batch gets the same result.
func (w *Writer) flush(b *batch) {
	links := make([]trace.Link, 0, len(b.requests))
	for _, r := range b.requests {
		if r.span.IsValid() {
			links = append(links, trace.Link{SpanContext: r.span})
		}
	}
	// The flush serves many export requests, so it gets its own trace, linked to all of them.
	ctx, span := tracer.Start(context.Background(), "ingest.flush",
		trace.WithNewRoot(),
		trace.WithLinks(links...),
		trace.WithAttributes(
			attribute.Int("ingest.batch.requests", len(b.requests)),
			attribute.Int("ingest.batch.series", len(b.series)),
			attribute.Int("ingest.batch.data_points", len(b.points)),
		))
	defer span.End()
	// The context is cancelled when the timeout expires, not as soon as the inserts return:
	// clickhouse-go (v2.43) watches the context of a finished insert in a goroutine that can
	// still observe the cancellation after the connection went back to the pool, and then
	// closes the TCP connection under the next insert ("use of closed network connection").
	// Found under load; letting the context expire later avoids the race.
	ctx, cancel := context.WithTimeout(ctx, w.cfg.FlushTimeout)
	time.AfterFunc(w.cfg.FlushTimeout, cancel)

	start := time.Now()
	w.tel.batchWait.Record(ctx, start.Sub(b.started).Seconds())
	err := w.insert(ctx, b)
	elapsed := time.Since(start)

	outcome := telemetry.OutcomeSuccess
	if err != nil {
		outcome = telemetry.OutcomeError
		span.RecordError(err)
		span.SetStatus(codes.Error, "insert failed")
		w.log.ErrorContext(ctx, "Failed to store batch",
			slog.Any("error", err),
			slog.Int("requests", len(b.requests)),
			slog.Int("series", len(b.series)),
			slog.Int("data_points", len(b.points)),
			slog.String("duration", elapsed.String()))
	} else {
		w.tel.seriesWritten.Add(ctx, int64(len(b.series)))
		w.tel.pointsWritten.Add(ctx, int64(len(b.points)))
		w.log.DebugContext(ctx, "Stored batch",
			slog.Int("requests", len(b.requests)),
			slog.Int("series", len(b.series)),
			slog.Int("data_points", len(b.points)),
			slog.String("duration", elapsed.String()))
	}
	w.tel.flushDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(telemetry.Outcome(outcome)))
	w.tel.batchSize.Record(ctx, int64(len(b.points)), metric.WithAttributes(telemetry.Outcome(outcome)))

	for _, r := range b.requests {
		r.result <- err
	}
}

func (w *Writer) insert(ctx context.Context, b *batch) error {
	if len(b.series) > 0 {
		if err := w.store.InsertSeries(ctx, b.series); err != nil {
			return err
		}
		// Only now are the series known: remembering them earlier would skip their rows forever
		// after a failed insert.
		keys := make([]series.Key, len(b.series))
		for i := range b.series {
			keys[i] = b.series[i].Key()
		}
		w.cache.Add(keys)
	}
	return w.store.InsertNumberPoints(ctx, b.points)
}

var tracer = otel.Tracer(telemetry.ScopeName + "/internal/ingest")

// queueLength is read by the queue length gauge.
func (w *Writer) queueLength() int64 {
	return int64(len(w.requests))
}
