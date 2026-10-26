package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseConfig configures the ClickHouse connection.
type ClickHouseConfig struct {
	Addr     string
	Database string
	Username string
	Password string
	// MaxOpenConns limits concurrent connections. Each in-flight insert holds one connection.
	MaxOpenConns int
	// DialTimeout bounds connection establishment.
	DialTimeout time.Duration
}

// ClickHouseStore implements Store on ClickHouse using the native protocol.
type ClickHouseStore struct {
	conn driver.Conn
}

var _ Store = (*ClickHouseStore)(nil)

// NewClickHouseStore opens a connection pool and verifies that ClickHouse is reachable.
func NewClickHouseStore(ctx context.Context, cfg ClickHouseConfig) (*ClickHouseStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout:  cfg.DialTimeout,
		MaxOpenConns: cfg.MaxOpenConns,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection to %s: %w", cfg.Addr, err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse at %s: %w", cfg.Addr, err)
	}
	return &ClickHouseStore{conn: conn}, nil
}

// Conn exposes the underlying connection, for queries in tests and tools.
func (s *ClickHouseStore) Conn() driver.Conn {
	return s.conn
}

// Migrate creates missing tables, verifies that existing tables have the columns this
// version of the application writes, and (re)creates the read views. It fails fast with a clear error instead of letting every
// insert fail later, for example when a table was created by an older, incompatible version.
//
// CREATE TABLE IF NOT EXISTS does not change existing tables: changing the retention of an
// existing table requires an ALTER TABLE ... MODIFY TTL.
func (s *ClickHouseStore) Migrate(ctx context.Context, retentionDays int) error {
	for _, t := range tables {
		if err := s.conn.Exec(ctx, t.createTableSQL(retentionDays)); err != nil {
			return fmt.Errorf("creating table %s: %w", t.name, err)
		}
		if err := s.verifyColumns(ctx, t); err != nil {
			return err
		}
	}
	for _, v := range views {
		if err := s.conn.Exec(ctx, v.ddl); err != nil {
			return fmt.Errorf("creating view %s: %w", v.name, err)
		}
	}
	return nil
}

func (s *ClickHouseStore) verifyColumns(ctx context.Context, t tableSpec) error {
	rows, err := s.conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = $1", t.name)
	if err != nil {
		return fmt.Errorf("reading columns of %s: %w", t.name, err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("reading columns of %s: %w", t.name, err)
		}
		actual = append(actual, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading columns of %s: %w", t.name, err)
	}
	var missing []string
	for _, c := range t.columns {
		if !slices.Contains(actual, c) {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("table %s exists but lacks columns %v: it was created by an incompatible version of this application", t.name, missing)
	}
	return nil
}

// InsertSeries writes series lookup rows in a single insert.
func (s *ClickHouseStore) InsertSeries(ctx context.Context, rows []SeriesRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+SeriesTable+` (
		Date, SeriesId, MetricName, MetricDescription, MetricUnit, MetricType,
		AggregationTemporality, IsMonotonic, ServiceName, ResourceAttributes, ResourceSchemaUrl,
		ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes)`)
	if err != nil {
		return fmt.Errorf("preparing %s insert: %w", SeriesTable, err)
	}
	defer abortUnlessSent(batch, &err)
	for i := range rows {
		r := &rows[i]
		if err := batch.Append(
			r.Day.Time(), uint64(r.SeriesID), r.MetricName, r.MetricDescription, r.MetricUnit,
			int8(r.MetricType), r.AggregationTemporality, r.IsMonotonic, r.ServiceName,
			r.ResourceAttributes, r.ResourceSchemaURL, r.ScopeName, r.ScopeVersion,
			r.ScopeAttributes, r.ScopeDroppedAttrCount, r.ScopeSchemaURL, r.Attributes,
		); err != nil {
			return fmt.Errorf("appending %s row: %w", SeriesTable, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending %s insert of %d rows: %w", SeriesTable, len(rows), err)
	}
	return nil
}

// InsertNumberPoints writes number data points in a single insert. Data points are appended
// column by column, which avoids boxing every value of every row on this hot path.
func (s *ClickHouseStore) InsertNumberPoints(ctx context.Context, rows []NumberPointRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO "+NumberPointsTable+" (SeriesId, StartTimeUnix, TimeUnix, Value, Flags)")
	if err != nil {
		return fmt.Errorf("preparing %s insert: %w", NumberPointsTable, err)
	}
	defer abortUnlessSent(batch, &err)

	ids := make([]uint64, len(rows))
	startTimes := make([]time.Time, len(rows))
	times := make([]time.Time, len(rows))
	values := make([]float64, len(rows))
	flags := make([]uint32, len(rows))
	for i := range rows {
		ids[i] = uint64(rows[i].SeriesID)
		startTimes[i] = rows[i].StartTimeUnix
		times[i] = rows[i].TimeUnix
		values[i] = rows[i].Value
		flags[i] = rows[i].Flags
	}
	for i, column := range []any{ids, startTimes, times, values, flags} {
		if err := batch.Column(i).Append(column); err != nil {
			return fmt.Errorf("appending %s column %d: %w", NumberPointsTable, i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending %s insert of %d rows: %w", NumberPointsTable, len(rows), err)
	}
	return nil
}

// abortUnlessSent releases the connection held by a batch that is abandoned because of an
// error. Without it, every failed append would leak a pooled connection.
func abortUnlessSent(batch driver.Batch, err *error) {
	if *err != nil && !batch.IsSent() {
		if abortErr := batch.Abort(); abortErr != nil && !errors.Is(abortErr, clickhouse.ErrBatchAlreadySent) {
			*err = errors.Join(*err, fmt.Errorf("aborting batch: %w", abortErr))
		}
	}
}

// Close closes the connection pool.
func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
