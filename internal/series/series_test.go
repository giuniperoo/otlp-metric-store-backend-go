package series

import (
	"math"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// identity is a complete series identity, so tests can vary one part at a time.
type identity struct {
	resourceAttrs []*commonpb.KeyValue
	resourceURL   string
	scopeName     string
	scopeVersion  string
	scopeAttrs    []*commonpb.KeyValue
	scopeURL      string
	metric        Metric
	pointAttrs    []*commonpb.KeyValue
}

func baseIdentity() identity {
	return identity{
		resourceAttrs: []*commonpb.KeyValue{str("service.name", "checkout"), str("host.name", "a")},
		resourceURL:   "https://opentelemetry.io/schemas/1.27.0",
		scopeName:     "io.opentelemetry.http",
		scopeVersion:  "1.0.0",
		metric:        Metric{Name: "http.server.requests", Unit: "{request}", Type: MetricTypeSum, AggregationTemporality: 2, IsMonotonic: true},
		pointAttrs:    []*commonpb.KeyValue{str("method", "GET"), str("status", "200")},
	}
}

func (i identity) id() ID {
	h := NewHasher()
	return h.Series(
		h.Resource(i.resourceAttrs, i.resourceURL),
		h.Scope(i.scopeName, i.scopeVersion, i.scopeAttrs, i.scopeURL),
		h.Metric(i.metric),
		i.pointAttrs,
	)
}

func TestSeriesIDIsDeterministic(t *testing.T) {
	if a, b := baseIdentity().id(), baseIdentity().id(); a != b {
		t.Fatalf("same identity produced different IDs: %d != %d", a, b)
	}
}

func TestSeriesIDIgnoresAttributeOrder(t *testing.T) {
	reordered := baseIdentity()
	reordered.resourceAttrs = []*commonpb.KeyValue{str("host.name", "a"), str("service.name", "checkout")}
	reordered.pointAttrs = []*commonpb.KeyValue{str("status", "200"), str("method", "GET")}
	if baseIdentity().id() != reordered.id() {
		t.Fatal("attribute order changed the series ID")
	}
}

func TestSeriesIDChangesWithEveryIdentityField(t *testing.T) {
	base := baseIdentity().id()
	tests := map[string]func(*identity){
		"resource attribute value": func(i *identity) { i.resourceAttrs[1] = str("host.name", "b") },
		"resource attribute added": func(i *identity) { i.resourceAttrs = append(i.resourceAttrs, str("k8s.pod.name", "p")) },
		"resource schema URL":      func(i *identity) { i.resourceURL = "" },
		"scope name":               func(i *identity) { i.scopeName = "other" },
		"scope version":            func(i *identity) { i.scopeVersion = "1.0.1" },
		"scope attribute":          func(i *identity) { i.scopeAttrs = []*commonpb.KeyValue{str("k", "v")} },
		"scope schema URL":         func(i *identity) { i.scopeURL = "https://example.com" },
		"metric name":              func(i *identity) { i.metric.Name = "http.server.request.count" },
		"metric unit":              func(i *identity) { i.metric.Unit = "1" },
		"metric type":              func(i *identity) { i.metric.Type = MetricTypeGauge },
		"aggregation temporality":  func(i *identity) { i.metric.AggregationTemporality = 1 },
		"monotonicity":             func(i *identity) { i.metric.IsMonotonic = false },
		"data point attribute":     func(i *identity) { i.pointAttrs[1] = str("status", "500") },
		"data point attribute removed": func(i *identity) {
			i.pointAttrs = i.pointAttrs[:1]
		},
		// Length prefixes keep these apart: the concatenated bytes are identical.
		"value boundary": func(i *identity) {
			i.pointAttrs = []*commonpb.KeyValue{str("method", "GETstatus"), str("", "200")}
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			changed := baseIdentity()
			change(&changed)
			if changed.id() == base {
				t.Fatalf("changing the %s did not change the series ID", name)
			}
		})
	}
}

func TestSeriesIDDistinguishesLevels(t *testing.T) {
	// The same attribute on the resource or on the data point is a different series.
	onResource := baseIdentity()
	onResource.resourceAttrs = append(onResource.resourceAttrs, str("region", "eu"))
	onPoint := baseIdentity()
	onPoint.pointAttrs = append(onPoint.pointAttrs, str("region", "eu"))
	if onResource.id() == onPoint.id() {
		t.Fatal("an attribute on the resource and on the data point produced the same series ID")
	}
}

func TestDuplicateAttributeKeysLastWins(t *testing.T) {
	dup := []*commonpb.KeyValue{str("status", "500"), str("method", "GET"), str("status", "200")}
	if got := AttributeMap(dup); got["status"] != "200" || len(got) != 2 {
		t.Fatalf("AttributeMap(%v) = %v, want last value to win", dup, got)
	}
	withDup := baseIdentity()
	withDup.pointAttrs = dup
	if withDup.id() != baseIdentity().id() {
		t.Fatal("duplicate keys are hashed differently from the map that is stored")
	}
}

func TestAppendAttributeValue(t *testing.T) {
	tests := map[string]struct {
		in   *commonpb.AnyValue
		want string
	}{
		"nil":    {nil, ""},
		"string": {&commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}}, "x"},
		"bool":   {&commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		"int":    {&commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: -42}}, "-42"},
		"double keeps precision": {
			&commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1234567.25}}, "1.23456725e+06",
		},
		"double NaN": {&commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: math.NaN()}}, "NaN"},
		"bytes":      {&commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte("hi")}}, "aGk="},
		"array": {&commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
			Values: []*commonpb.AnyValue{
				{Value: &commonpb.AnyValue_StringValue{StringValue: "a"}},
				{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
				{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: math.Inf(1)}},
			},
		}}}, `["a",1,"+Inf"]`},
		"kvlist is sorted by key": {&commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
			Values: []*commonpb.KeyValue{str("b", "2"), str("a", "1")},
		}}}, `{"a":"1","b":"2"}`},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := string(AppendAttributeValue(nil, tt.in)); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDayOf(t *testing.T) {
	ts := time.Date(2026, 10, 25, 23, 59, 59, 999999999, time.UTC)
	day := DayOf(uint64(ts.UnixNano()))
	if got := day.Time(); !got.Equal(time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("DayOf(%v).Time() = %v", ts, got)
	}
	if next := DayOf(uint64(ts.UnixNano()) + 1); next != day+1 {
		t.Fatalf("the next nanosecond is day %d, want %d", next, day+1)
	}
}

func BenchmarkSeriesID(b *testing.B) {
	i := baseIdentity()
	h := NewHasher()
	resource := h.Resource(i.resourceAttrs, i.resourceURL)
	scope := h.Scope(i.scopeName, i.scopeVersion, i.scopeAttrs, i.scopeURL)
	metric := h.Metric(i.metric)
	b.ReportAllocs()
	for b.Loop() {
		h.Series(resource, scope, metric, i.pointAttrs)
	}
}
