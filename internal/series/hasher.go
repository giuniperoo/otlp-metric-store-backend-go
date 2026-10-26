package series

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// encodingVersion is hashed first into every ID. Bump it whenever the canonical encoding
// below changes, so that new IDs never silently collide with IDs of the old encoding.
const encodingVersion byte = 1

// Field tags keep the different levels of the identity apart in the canonical encoding.
const (
	tagResource byte = 'R'
	tagScope    byte = 'S'
	tagMetric   byte = 'M'
	tagSeries   byte = 'P'
)

// Hasher computes series IDs. The identity is hashed level by level, so that the resource and
// scope are hashed once per ResourceMetrics/ScopeMetrics rather than once per data point:
//
//	resource := h.Resource(...)
//	scope := h.Scope(...)
//	metric := h.Metric(...)
//	id := h.Series(resource, scope, metric, dataPointAttributes)
//
// Every string is length-prefixed, so different identities can never encode to the same bytes
// (for example the attribute sets {a: "b,c"} and {a: "b", c: ""}).
//
// A Hasher reuses internal buffers and is not safe for concurrent use; use one per goroutine.
type Hasher struct {
	digest *xxhash.Digest
	buf    []byte // scratch for fixed-size fields
	value  []byte // scratch for attribute values, separate from buf so writes cannot clobber it
	idx    []int
}

// NewHasher returns a Hasher.
func NewHasher() *Hasher {
	return &Hasher{digest: xxhash.New()}
}

// Resource hashes the identity of an OTLP resource.
func (h *Hasher) Resource(attrs []*commonpb.KeyValue, schemaURL string) uint64 {
	h.begin(tagResource)
	h.writeString(schemaURL)
	h.writeAttributes(attrs)
	return h.digest.Sum64()
}

// Scope hashes the identity of an OTLP instrumentation scope.
func (h *Hasher) Scope(name, version string, attrs []*commonpb.KeyValue, schemaURL string) uint64 {
	h.begin(tagScope)
	h.writeString(name)
	h.writeString(version)
	h.writeString(schemaURL)
	h.writeAttributes(attrs)
	return h.digest.Sum64()
}

// Metric is the identity of an OTLP metric, excluding its data points.
type Metric struct {
	Name string
	Unit string
	Type MetricType
	// AggregationTemporality and IsMonotonic are only set for sums.
	AggregationTemporality int32
	IsMonotonic            bool
}

// Metric hashes the identity of a metric.
func (h *Hasher) Metric(m Metric) uint64 {
	h.begin(tagMetric)
	h.writeString(m.Name)
	h.writeString(m.Unit)
	h.writeUint64(uint64(m.Type))
	h.writeUint64(uint64(uint32(m.AggregationTemporality)))
	if m.IsMonotonic {
		h.writeUint64(1)
	} else {
		h.writeUint64(0)
	}
	return h.digest.Sum64()
}

// Series combines the resource, scope and metric hashes with the data point attributes into
// the series ID.
func (h *Hasher) Series(resource, scope, metric uint64, attrs []*commonpb.KeyValue) ID {
	h.begin(tagSeries)
	h.writeUint64(resource)
	h.writeUint64(scope)
	h.writeUint64(metric)
	h.writeAttributes(attrs)
	return ID(h.digest.Sum64())
}

func (h *Hasher) begin(tag byte) {
	h.digest.Reset()
	h.buf = append(h.buf[:0], encodingVersion, tag)
	_, _ = h.digest.Write(h.buf)
}

func (h *Hasher) writeUint64(v uint64) {
	h.buf = binary.LittleEndian.AppendUint64(h.buf[:0], v)
	_, _ = h.digest.Write(h.buf)
}

func (h *Hasher) writeString(s string) {
	h.writeUint64(uint64(len(s)))
	_, _ = h.digest.WriteString(s)
}

// writeAttributes writes attributes sorted by key, with values in their stored string form,
// so that the hash depends only on the attribute set and not on the order in which a client
// happened to send it.
func (h *Hasher) writeAttributes(attrs []*commonpb.KeyValue) {
	h.idx = sortedAttributeIndexes(h.idx, attrs)
	h.writeUint64(uint64(len(h.idx)))
	for _, i := range h.idx {
		h.writeString(attrs[i].GetKey())
		h.value = AppendAttributeValue(h.value[:0], attrs[i].GetValue())
		h.writeUint64(uint64(len(h.value)))
		_, _ = h.digest.Write(h.value)
	}
}
