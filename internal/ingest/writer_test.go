package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"dash0.com/otlp-metric-store-backend/internal/series"
	"dash0.com/otlp-metric-store-backend/internal/storage"
)

// fakeStore records inserts. If block is set, inserts wait until it is closed.
type fakeStore struct {
	mu        sync.Mutex
	calls     []string // "series:<n>" / "points:<n>", in order
	series    []storage.SeriesRow
	points    []storage.NumberPointRow
	seriesErr error
	pointsErr error
	block     chan struct{}
}

func (f *fakeStore) InsertSeries(_ context.Context, rows []storage.SeriesRow) error {
	f.wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "series")
	if f.seriesErr != nil {
		return f.seriesErr
	}
	f.series = append(f.series, rows...)
	return nil
}

func (f *fakeStore) InsertNumberPoints(_ context.Context, rows []storage.NumberPointRow) error {
	f.wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "points")
	if f.pointsErr != nil {
		return f.pointsErr
	}
	f.points = append(f.points, rows...)
	return nil
}

func (f *fakeStore) wait() {
	if f.block != nil {
		<-f.block
	}
}

func (f *fakeStore) snapshot() (calls []string, nSeries, nPoints int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), len(f.series), len(f.points)
}

func testConfig() Config {
	return Config{MaxBatchPoints: 1000, MaxBatchDelay: 20 * time.Millisecond, QueueSize: 100, FlushWorkers: 1, FlushTimeout: time.Second}
}

func newTestWriter(t *testing.T, cfg Config, store storage.Store, cache *series.Cache) *Writer {
	t.Helper()
	w, err := NewWriter(cfg, store, cache, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func seriesRow(id series.ID) storage.SeriesRow {
	return storage.SeriesRow{SeriesID: id, Day: 1, MetricName: "m"}
}

func points(id series.ID, n int) []storage.NumberPointRow {
	out := make([]storage.NumberPointRow, n)
	for i := range out {
		out[i] = storage.NumberPointRow{SeriesID: id, TimeUnix: time.Unix(int64(i), 0), Value: float64(i)}
	}
	return out
}

func TestWriterStoresSeriesBeforePoints(t *testing.T) {
	store := &fakeStore{}
	cache := series.NewCache(100)
	w := newTestWriter(t, testConfig(), store, cache)

	if err := w.Write(context.Background(), []storage.SeriesRow{seriesRow(1)}, points(1, 3)); err != nil {
		t.Fatal(err)
	}
	calls, nSeries, nPoints := store.snapshot()
	if len(calls) != 2 || calls[0] != "series" || calls[1] != "points" || nSeries != 1 || nPoints != 3 {
		t.Fatalf("got calls %v with %d series and %d points; want series then points, 1 and 3", calls, nSeries, nPoints)
	}
	if missing := cache.Missing([]series.Key{{ID: 1, Day: 1}}); len(missing) != 0 {
		t.Fatal("a stored series must be cached")
	}
}

func TestWriterBatchesConcurrentRequests(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.MaxBatchDelay = 200 * time.Millisecond // long enough for all writers to join one batch
	w := newTestWriter(t, cfg, store, nil)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			// Every request carries the same new series: it must be inserted once per batch.
			if err := w.Write(context.Background(), []storage.SeriesRow{seriesRow(7)}, points(series.ID(i), 5)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	calls, nSeries, nPoints := store.snapshot()
	if len(calls) != 2 || nSeries != 1 || nPoints != 50 {
		t.Fatalf("got calls %v with %d series and %d points; want one insert per table, 1 series, 50 points", calls, nSeries, nPoints)
	}
}

func TestWriterFlushesFullBatchWithoutWaitingForDelay(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.MaxBatchPoints = 10
	cfg.MaxBatchDelay = time.Hour
	w := newTestWriter(t, cfg, store, nil)

	done := make(chan error, 1)
	go func() { done <- w.Write(context.Background(), nil, points(1, 10)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a full batch was not stored before MaxBatchDelay")
	}
}

func TestWriterReportsStoreErrorsAndDoesNotCacheFailedSeries(t *testing.T) {
	store := &fakeStore{seriesErr: errors.New("clickhouse down")}
	cache := series.NewCache(100)
	w := newTestWriter(t, testConfig(), store, cache)

	err := w.Write(context.Background(), []storage.SeriesRow{seriesRow(1)}, points(1, 1))
	if err == nil || err.Error() != "clickhouse down" {
		t.Fatalf("got %v, want the store error", err)
	}
	if calls, _, _ := store.snapshot(); len(calls) != 1 {
		t.Fatalf("data points must not be inserted when their series could not be: calls %v", calls)
	}
	if missing := cache.Missing([]series.Key{{ID: 1, Day: 1}}); len(missing) != 1 {
		t.Fatal("a series whose insert failed must not be cached, or it would never be written")
	}
}

func TestWriterRejectsWhenQueueIsFull(t *testing.T) {
	store := &fakeStore{block: make(chan struct{})}
	cfg := testConfig()
	cfg.MaxBatchPoints = 1 // every request is its own batch
	cfg.QueueSize = 2
	w := newTestWriter(t, cfg, store, nil)
	defer close(store.block)

	// One request blocks in the flush worker, one in the batch loop, two fill the queue.
	results := make(chan error, 10)
	for range 10 {
		go func() { results <- w.Write(context.Background(), nil, points(1, 1)) }()
	}
	deadline := time.After(5 * time.Second)
	overloaded := 0
	for overloaded < 6 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrOverloaded) {
				t.Fatalf("got %v, want ErrOverloaded", err)
			}
			overloaded++
		case <-deadline:
			t.Fatalf("only %d of 10 requests were rejected with ErrOverloaded, want 6", overloaded)
		}
	}
}

func TestWriterWriteHonoursContext(t *testing.T) {
	store := &fakeStore{block: make(chan struct{})}
	w := newTestWriter(t, testConfig(), store, nil)
	defer close(store.block)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.Write(ctx, nil, points(1, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestWriterCloseStoresPendingRows(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.MaxBatchDelay = time.Hour // only Close can flush
	w := newTestWriter(t, cfg, store, nil)

	done := make(chan error, 1)
	go func() { done <- w.Write(context.Background(), nil, points(1, 3)) }()
	time.Sleep(100 * time.Millisecond) // let Write enqueue its request before closing
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("pending write failed on close: %v", err)
	}
	if _, _, nPoints := store.snapshot(); nPoints != 3 {
		t.Fatalf("stored %d data points on close, want 3", nPoints)
	}
	if err := w.Write(context.Background(), nil, points(1, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after Close = %v, want ErrClosed", err)
	}
}

func TestNewWriterValidatesConfig(t *testing.T) {
	cfg := testConfig()
	cfg.FlushWorkers = 0
	if _, err := NewWriter(cfg, &fakeStore{}, nil, slog.Default()); err == nil {
		t.Fatal("want an error for zero flush workers")
	}
}
