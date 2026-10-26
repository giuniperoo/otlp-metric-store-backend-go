package mapping

import (
	"fmt"
	"time"

	"dash0.com/otlp-metric-store-backend/internal/series"
)

// MaxWindow is the longest accepted time window. Both tables are partitioned by UTC day, and
// ClickHouse refuses an insert that touches more than 100 partitions
// (max_partitions_per_insert_block). A batch merges many requests, so a single client sending
// data points spread over more days would fail the batch for every request in it, and its
// retries would fail the next batches too. 90 days leaves room for the day boundaries at both
// ends and for requests of one batch that were mapped at slightly different times.
const MaxWindow = 90 * 24 * time.Hour

// TimeLimits bound the data point times that are accepted, relative to the time a request is
// received. Data points outside these limits are rejected rather than stored.
type TimeLimits struct {
	// MaxAge is how far in the past a data point may be.
	MaxAge time.Duration
	// MaxClockSkew is how far in the future a data point may be, for clients whose clocks run
	// ahead.
	MaxClockSkew time.Duration
	// RetentionDays, when positive, also rejects data points that the tables' TTL would delete
	// right away: only data points on the last RetentionDays UTC days, including today, are
	// accepted. Without this, such data points would be acknowledged and then lost.
	RetentionDays int
}

// Validate checks that the limits are usable and span at most MaxWindow.
func (l TimeLimits) Validate() error {
	if l.MaxAge <= 0 || l.MaxClockSkew < 0 {
		return fmt.Errorf("the maximum data point age must be positive and the maximum clock skew must not be negative, got %v and %v", l.MaxAge, l.MaxClockSkew)
	}
	if l.MaxAge+l.MaxClockSkew > MaxWindow {
		return fmt.Errorf("the maximum data point age plus the maximum clock skew must not exceed %v (ClickHouse refuses inserts into more than 100 day partitions), got %v", MaxWindow, l.MaxAge+l.MaxClockSkew)
	}
	return nil
}

// Window returns the times accepted for a request received at now.
func (l TimeLimits) Window(now time.Time) Window {
	from := now.Add(-l.MaxAge)
	if l.RetentionDays > 0 {
		kept := (series.DayOf(uint64(now.UnixNano())) - series.Day(l.RetentionDays-1)).Time()
		if kept.After(from) {
			from = kept
		}
	}
	return Window{From: uint64(max(from.UnixNano(), 0)), To: uint64(now.Add(l.MaxClockSkew).UnixNano())}
}

// Window is a range of accepted data point times in Unix nanoseconds, From <= time <= To.
type Window struct {
	From, To uint64
}
