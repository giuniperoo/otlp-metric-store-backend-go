package series

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// AttributeMap converts OTLP attributes to the map that is stored in the series lookup table.
//
// Values are converted with [AppendAttributeValue]. If a key occurs more than once (which the
// OTLP specification forbids, but which receivers must survive), the last occurrence wins.
// The [Hasher] applies exactly the same rules, so a series ID can always be recomputed from
// the stored maps.
func AttributeMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	var buf []byte
	for _, kv := range attrs {
		buf = AppendAttributeValue(buf[:0], kv.GetValue())
		m[kv.GetKey()] = string(buf)
	}
	return m
}

// AppendAttributeValue appends the string form of an OTLP attribute value to buf.
//
//   - strings are used as-is,
//   - booleans, integers and doubles use their shortest exact representation
//     (doubles round-trip; the starter code's %g lost precision beyond 6 digits),
//   - bytes are base64 encoded,
//   - arrays and key-value lists are encoded as JSON, with key-value lists sorted by key,
//   - an absent value is the empty string.
func AppendAttributeValue(buf []byte, v *commonpb.AnyValue) []byte {
	if v == nil {
		return buf
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return append(buf, val.StringValue...)
	case *commonpb.AnyValue_BoolValue:
		return strconv.AppendBool(buf, val.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.AppendInt(buf, val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.AppendFloat(buf, val.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.AppendEncode(buf, val.BytesValue)
	case *commonpb.AnyValue_ArrayValue, *commonpb.AnyValue_KvlistValue:
		encoded, err := json.Marshal(jsonValue(v))
		if err != nil {
			// jsonValue only produces JSON-safe values, so this is unreachable in practice.
			return fmt.Appendf(buf, "%v", v)
		}
		return append(buf, encoded...)
	default:
		return buf
	}
}

// jsonValue converts an AnyValue into a value that encoding/json encodes deterministically.
// Maps are encoded with sorted keys by encoding/json.
func jsonValue(v *commonpb.AnyValue) any {
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		if math.IsNaN(val.DoubleValue) || math.IsInf(val.DoubleValue, 0) {
			return strconv.FormatFloat(val.DoubleValue, 'g', -1, 64) // JSON has no NaN/Inf
		}
		return val.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return val.BytesValue // encoded as base64 by encoding/json
	case *commonpb.AnyValue_ArrayValue:
		values := val.ArrayValue.GetValues()
		out := make([]any, len(values))
		for i, e := range values {
			out[i] = jsonValue(e)
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		kvs := val.KvlistValue.GetValues()
		out := make(map[string]any, len(kvs))
		for _, kv := range kvs {
			out[kv.GetKey()] = jsonValue(kv.GetValue())
		}
		return out
	default:
		return nil
	}
}

// sortedAttributeIndexes returns the indexes of attrs in key order, keeping only the last
// occurrence of duplicate keys (matching [AttributeMap]). The result is written into idx to
// avoid allocating on the hot path.
func sortedAttributeIndexes(idx []int, attrs []*commonpb.KeyValue) []int {
	idx = idx[:0]
	sorted := true
	for i := range attrs {
		if i > 0 && attrs[i-1].GetKey() >= attrs[i].GetKey() {
			sorted = false
		}
		idx = append(idx, i)
	}
	if sorted {
		return idx // fast path: SDKs usually emit attributes sorted and without duplicates
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		return strings.Compare(attrs[a].GetKey(), attrs[b].GetKey())
	})
	// Drop all but the last occurrence of each key. The sort is stable, so for equal keys
	// the original order is kept and the last element of each run is the last occurrence.
	out := idx[:0]
	for i, n := range idx {
		if i+1 < len(idx) && attrs[idx[i+1]].GetKey() == attrs[n].GetKey() {
			continue
		}
		out = append(out, n)
	}
	return out
}
