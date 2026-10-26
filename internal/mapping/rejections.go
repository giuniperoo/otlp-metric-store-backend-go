package mapping

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Reason is why a data point was rejected. Reasons are used verbatim in the partial success
// message returned to the client and as the "reason" attribute of the rejected data points
// metric, so that both use the same terms.
type Reason string

const (
	// ReasonUnsupportedMetricType: the metric is a histogram, exponential histogram or summary.
	// Only gauges and sums are stored.
	ReasonUnsupportedMetricType Reason = "unsupported_metric_type"
	// ReasonMissingMetricName: the metric has no name.
	ReasonMissingMetricName Reason = "missing_metric_name"
	// ReasonInvalidTimestamp: the data point has no time, or a time or start time that cannot
	// be represented (beyond the year 2262).
	ReasonInvalidTimestamp Reason = "invalid_timestamp"
	// ReasonTimestampTooOld: the data point is older than the accepted window (see
	// [TimeLimits]): older than the maximum data point age, or old enough that the retention
	// TTL would delete it right away.
	ReasonTimestampTooOld Reason = "timestamp_too_old"
	// ReasonTimestampInFuture: the data point is further in the future than the maximum clock
	// skew.
	ReasonTimestampInFuture Reason = "timestamp_in_future"
	// ReasonMissingValue: the data point has neither an integer nor a double value.
	ReasonMissingValue Reason = "missing_value"
)

// Rejections counts rejected data points by reason. The zero value is ready to use.
type Rejections struct {
	counts      map[Reason]int64
	metricTypes map[string]struct{} // unsupported metric types seen, for the message
	window      Window              // the accepted window, for the message
}

func (r *Rejections) add(reason Reason, dataPoints int, metricType string) {
	if dataPoints <= 0 {
		return
	}
	if r.counts == nil {
		r.counts = make(map[Reason]int64)
	}
	r.counts[reason] += int64(dataPoints)
	if metricType != "" {
		if r.metricTypes == nil {
			r.metricTypes = make(map[string]struct{})
		}
		r.metricTypes[metricType] = struct{}{}
	}
}

// Total returns the number of rejected data points.
func (r *Rejections) Total() int64 {
	var total int64
	for _, n := range r.counts {
		total += n
	}
	return total
}

// Count returns the number of data points rejected for a reason.
func (r *Rejections) Count(reason Reason) int64 {
	return r.counts[reason]
}

// Reasons returns the reasons with at least one rejected data point, sorted.
func (r *Rejections) Reasons() []Reason {
	reasons := make([]Reason, 0, len(r.counts))
	for reason := range r.counts {
		reasons = append(reasons, reason)
	}
	slices.Sort(reasons)
	return reasons
}

// Message describes the rejections for the client, for example
// "rejected 3 data points: invalid_timestamp=1, unsupported_metric_type=2 (histogram)".
// It is empty when nothing was rejected.
func (r *Rejections) Message() string {
	total := r.Total()
	if total == 0 {
		return ""
	}
	parts := make([]string, 0, len(r.counts))
	for _, reason := range r.Reasons() {
		part := fmt.Sprintf("%s=%d", reason, r.counts[reason])
		switch reason {
		case ReasonUnsupportedMetricType:
			if len(r.metricTypes) > 0 {
				types := make([]string, 0, len(r.metricTypes))
				for t := range r.metricTypes {
					types = append(types, t)
				}
				slices.Sort(types)
				part += " (" + strings.Join(types, ", ") + "; only gauge and sum are supported)"
			}
		case ReasonTimestampTooOld:
			part += " (before " + formatUnixNano(r.window.From) + ")"
		case ReasonTimestampInFuture:
			part += " (after " + formatUnixNano(r.window.To) + ")"
		}
		parts = append(parts, part)
	}
	noun := "data points"
	if total == 1 {
		noun = "data point"
	}
	return fmt.Sprintf("rejected %d %s: %s", total, noun, strings.Join(parts, ", "))
}

// formatUnixNano formats a window bound for the message, to the second.
func formatUnixNano(unixNano uint64) string {
	return time.Unix(0, int64(unixNano)).UTC().Format(time.RFC3339)
}
