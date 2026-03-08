package cowrie

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
)

// Buffer pool for encoding - reduces allocations in hot paths
var bufferPool = sync.Pool{
	New: func() any {
		return &buffer{data: make([]byte, 0, 4096)}
	},
}

// Encode encodes a value to Cowrie v2 binary format.
func Encode(v *Value) ([]byte, error) {
	buf := bufferPool.Get().(*buffer)
	buf.data = buf.data[:0] // reset length, keep capacity

	if err := encode(buf, v); err != nil {
		// Don't pool buffers > 1MB to prevent memory bloat (matches Gen1)
		if cap(buf.data) <= 1<<20 {
			bufferPool.Put(buf)
		}
		return nil, err
	}

	// Copy result (can't return pooled buffer)
	out := make([]byte, len(buf.data))
	copy(out, buf.data)

	// Don't pool buffers > 1MB to prevent memory bloat (matches Gen1)
	if cap(buf.data) <= 1<<20 {
		bufferPool.Put(buf)
	}
	return out, nil
}

// EncodeAppend encodes v and appends the result to dst.
// Useful for avoiding allocations when the caller manages buffers.
func EncodeAppend(dst []byte, v *Value) ([]byte, error) {
	buf := buffer{data: dst}
	if err := encode(&buf, v); err != nil {
		return nil, err
	}
	return buf.data, nil
}

// EncodeTo encodes a value to an io.Writer.
func EncodeTo(w io.Writer, v *Value) error {
	data, err := Encode(v)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// buffer is a simple byte buffer for encoding.
type buffer struct {
	data []byte
}

func (b *buffer) bytes() []byte {
	return b.data
}

func (b *buffer) writeByte(c byte) {
	b.data = append(b.data, c)
}

func (b *buffer) write(p []byte) {
	b.data = append(b.data, p...)
}

func (b *buffer) writeUvarint(v uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	b.write(buf[:n])
}

func (b *buffer) writeString(s string) {
	b.writeUvarint(uint64(len(s)))
	b.write([]byte(s))
}

func (b *buffer) writeUint16LE(v uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	b.write(buf[:])
}

func (b *buffer) writeUint32LE(v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	b.write(buf[:])
}

func (b *buffer) writeInt32LE(v int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(v))
	b.write(buf[:])
}

// zigzagEncode encodes a signed integer using zigzag encoding.
func zigzagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// dictionary for field name encoding
type dict struct {
	keys   []string
	lookup map[string]int
}

func newDict() *dict {
	return &dict{
		lookup: make(map[string]int),
	}
}

func (d *dict) add(key string) int {
	if idx, ok := d.lookup[key]; ok {
		return idx
	}
	idx := len(d.keys)
	d.keys = append(d.keys, key)
	d.lookup[key] = idx
	return idx
}

func (d *dict) get(key string) int {
	return d.lookup[key]
}

// collectKeys recursively collects all object keys into the dictionary.
func collectKeys(v *Value, d *dict) {
	if v == nil {
		return
	}
	switch v.typ {
	case TypeArray:
		for _, item := range v.arrayVal {
			collectKeys(item, d)
		}
	case TypeObject:
		for _, m := range v.objectVal {
			d.add(m.Key)
			collectKeys(m.Value, d)
		}
	case TypeNode:
		// Collect node property keys (including nested maps)
		collectKeysFromAnyMap(v.nodeVal.Props, d)
	case TypeEdge:
		// Collect edge property keys (including nested maps)
		collectKeysFromAnyMap(v.edgeVal.Props, d)
	case TypeNodeBatch:
		// Collect property keys from all nodes
		for _, node := range v.nodeBatchVal.Nodes {
			collectKeysFromAnyMap(node.Props, d)
		}
	case TypeEdgeBatch:
		// Collect property keys from all edges
		for _, edge := range v.edgeBatchVal.Edges {
			collectKeysFromAnyMap(edge.Props, d)
		}
	case TypeGraphShard:
		// Collect property keys from all nodes, edges, and metadata
		for _, node := range v.graphShardVal.Nodes {
			collectKeysFromAnyMap(node.Props, d)
		}
		for _, edge := range v.graphShardVal.Edges {
			collectKeysFromAnyMap(edge.Props, d)
		}
		collectKeysFromAnyMap(v.graphShardVal.Metadata, d)
	}
}

// collectKeysFromAnyMap recursively collects keys from a map[string]any.
func collectKeysFromAnyMap(m map[string]any, d *dict) {
	for k, v := range m {
		d.add(k)
		// Recursively collect keys from nested maps and arrays
		collectKeysFromAnyValue(v, d)
	}
}

// collectKeysFromAnyValue recursively collects keys from nested structures.
func collectKeysFromAnyValue(v any, d *dict) {
	if v == nil {
		return
	}
	switch val := v.(type) {
	case map[string]any:
		collectKeysFromAnyMap(val, d)
	case []any:
		for _, item := range val {
			collectKeysFromAnyValue(item, d)
		}
	case *Value:
		collectKeys(val, d)
	}
}

// encode writes the complete Cowrie v2 format to the buffer.
func encode(buf *buffer, v *Value) error {
	// Build dictionary
	d := newDict()
	collectKeys(v, d)

	// Write header
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Version)
	buf.writeByte(0) // flags = 0 (no compression)

	// Write dictionary
	buf.writeUvarint(uint64(len(d.keys)))
	for _, key := range d.keys {
		buf.writeString(key)
	}

	// Write root value
	return encodeValue(buf, v, d)
}

// encodeValue writes a single value to the buffer.
func encodeValue(buf *buffer, v *Value, d *dict) error {
	if v == nil {
		buf.writeByte(TagNull)
		return nil
	}

	switch v.typ {
	case TypeNull:
		buf.writeByte(TagNull)

	case TypeBool:
		if v.boolVal {
			buf.writeByte(TagTrue)
		} else {
			buf.writeByte(TagFalse)
		}

	case TypeInt64:
		buf.writeByte(TagInt64)
		buf.writeUvarint(zigzagEncode(v.int64Val))

	case TypeUint64:
		buf.writeByte(TagUint64)
		buf.writeUvarint(v.uint64Val)

	case TypeFloat64:
		buf.writeByte(TagFloat64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], uint64FromFloat64(v.float64Val))
		buf.write(bits[:])

	case TypeDecimal128:
		buf.writeByte(TagDecimal128)
		buf.writeByte(byte(v.decimal128.Scale))
		buf.write(v.decimal128.Coef[:])

	case TypeString:
		buf.writeByte(TagString)
		buf.writeString(v.stringVal)

	case TypeBytes:
		buf.writeByte(TagBytes)
		buf.writeUvarint(uint64(len(v.bytesVal)))
		buf.write(v.bytesVal)

	case TypeDatetime64:
		buf.writeByte(TagDatetime64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], uint64(v.datetime64))
		buf.write(bits[:])

	case TypeUUID128:
		buf.writeByte(TagUUID128)
		buf.write(v.uuid128[:])

	case TypeBigInt:
		buf.writeByte(TagBigInt)
		buf.writeUvarint(uint64(len(v.bigintVal)))
		buf.write(v.bigintVal)

	case TypeArray:
		buf.writeByte(TagArray)
		buf.writeUvarint(uint64(len(v.arrayVal)))
		for _, item := range v.arrayVal {
			if err := encodeValue(buf, item, d); err != nil {
				return err
			}
		}

	case TypeObject:
		buf.writeByte(TagObject)
		buf.writeUvarint(uint64(len(v.objectVal)))
		for _, m := range v.objectVal {
			idx := d.get(m.Key)
			buf.writeUvarint(uint64(idx))
			if err := encodeValue(buf, m.Value, d); err != nil {
				return err
			}
		}

	// v2.1 Extension Types
	case TypeTensor:
		buf.writeByte(TagTensor)
		buf.writeByte(byte(v.tensorVal.DType))
		buf.writeByte(byte(len(v.tensorVal.Dims))) // rank
		for _, dim := range v.tensorVal.Dims {
			buf.writeUvarint(dim)
		}
		buf.writeUvarint(uint64(len(v.tensorVal.Data)))
		buf.write(v.tensorVal.Data)

	case TypeTensorRef:
		buf.writeByte(TagTensorRef)
		buf.writeByte(v.tensorRefVal.StoreID)
		buf.writeUvarint(uint64(len(v.tensorRefVal.Key)))
		buf.write(v.tensorRefVal.Key)

	case TypeImage:
		buf.writeByte(TagImage)
		buf.writeByte(byte(v.imageVal.Format))
		buf.writeUint16LE(v.imageVal.Width)
		buf.writeUint16LE(v.imageVal.Height)
		buf.writeUvarint(uint64(len(v.imageVal.Data)))
		buf.write(v.imageVal.Data)

	case TypeAudio:
		buf.writeByte(TagAudio)
		buf.writeByte(byte(v.audioVal.Encoding))
		buf.writeUint32LE(v.audioVal.SampleRate)
		buf.writeByte(v.audioVal.Channels)
		buf.writeUvarint(uint64(len(v.audioVal.Data)))
		buf.write(v.audioVal.Data)

	case TypeAdjlist:
		buf.writeByte(TagAdjlist)
		buf.writeByte(byte(v.adjlistVal.IDWidth))
		buf.writeUvarint(v.adjlistVal.NodeCount)
		buf.writeUvarint(v.adjlistVal.EdgeCount)
		for _, offset := range v.adjlistVal.RowOffsets {
			buf.writeUvarint(offset)
		}
		buf.write(v.adjlistVal.ColIndices)

	case TypeRichText:
		buf.writeByte(TagRichText)
		buf.writeString(v.richTextVal.Text)
		// Calculate flags
		var flags byte
		if len(v.richTextVal.Tokens) > 0 {
			flags |= 0x01
		}
		if len(v.richTextVal.Spans) > 0 {
			flags |= 0x02
		}
		buf.writeByte(flags)
		// Write tokens if present
		if flags&0x01 != 0 {
			buf.writeUvarint(uint64(len(v.richTextVal.Tokens)))
			for _, tok := range v.richTextVal.Tokens {
				buf.writeInt32LE(tok)
			}
		}
		// Write spans if present
		if flags&0x02 != 0 {
			buf.writeUvarint(uint64(len(v.richTextVal.Spans)))
			for _, span := range v.richTextVal.Spans {
				buf.writeUvarint(span.Start)
				buf.writeUvarint(span.End)
				buf.writeUvarint(span.KindID)
			}
		}

	case TypeDelta:
		buf.writeByte(TagDelta)
		buf.writeUvarint(v.deltaVal.BaseID)
		buf.writeUvarint(uint64(len(v.deltaVal.Ops)))
		for _, op := range v.deltaVal.Ops {
			buf.writeByte(byte(op.OpCode))
			buf.writeUvarint(op.FieldID)
			if op.OpCode == DeltaOpSetField || op.OpCode == DeltaOpAppendArray {
				if err := encodeValue(buf, op.Value, d); err != nil {
					return err
				}
			}
		}

	case TypeNode:
		buf.writeByte(TagNode)
		// ID
		buf.writeString(v.nodeVal.ID)
		// Labels
		buf.writeUvarint(uint64(len(v.nodeVal.Labels)))
		for _, label := range v.nodeVal.Labels {
			buf.writeString(label)
		}
		// Properties (dictionary-coded keys)
		buf.writeUvarint(uint64(len(v.nodeVal.Props)))
		for k, propVal := range v.nodeVal.Props {
			buf.writeUvarint(uint64(d.get(k)))
			if err := encodeAny(buf, propVal, d); err != nil {
				return err
			}
		}

	case TypeEdge:
		buf.writeByte(TagEdge)
		// Source and destination IDs
		buf.writeString(v.edgeVal.From)
		buf.writeString(v.edgeVal.To)
		// Edge type
		buf.writeString(v.edgeVal.Type)
		// Properties (dictionary-coded keys)
		buf.writeUvarint(uint64(len(v.edgeVal.Props)))
		for k, propVal := range v.edgeVal.Props {
			buf.writeUvarint(uint64(d.get(k)))
			if err := encodeAny(buf, propVal, d); err != nil {
				return err
			}
		}

	case TypeNodeBatch:
		buf.writeByte(TagNodeBatch)
		buf.writeUvarint(uint64(len(v.nodeBatchVal.Nodes)))
		for _, node := range v.nodeBatchVal.Nodes {
			if err := encodeNodeData(buf, &node, d); err != nil {
				return err
			}
		}

	case TypeEdgeBatch:
		buf.writeByte(TagEdgeBatch)
		buf.writeUvarint(uint64(len(v.edgeBatchVal.Edges)))
		for _, edge := range v.edgeBatchVal.Edges {
			if err := encodeEdgeData(buf, &edge, d); err != nil {
				return err
			}
		}

	case TypeGraphShard:
		buf.writeByte(TagGraphShard)
		// Nodes
		buf.writeUvarint(uint64(len(v.graphShardVal.Nodes)))
		for _, node := range v.graphShardVal.Nodes {
			if err := encodeNodeData(buf, &node, d); err != nil {
				return err
			}
		}
		// Edges
		buf.writeUvarint(uint64(len(v.graphShardVal.Edges)))
		for _, edge := range v.graphShardVal.Edges {
			if err := encodeEdgeData(buf, &edge, d); err != nil {
				return err
			}
		}
		// Metadata (dictionary-coded keys)
		buf.writeUvarint(uint64(len(v.graphShardVal.Metadata)))
		for k, metaVal := range v.graphShardVal.Metadata {
			buf.writeUvarint(uint64(d.get(k)))
			if err := encodeAny(buf, metaVal, d); err != nil {
				return err
			}
		}

	case TypeUnknownExt:
		// Re-encode unknown extensions to enable round-trip preservation
		buf.writeByte(TagExt)
		buf.writeUvarint(v.unknownExtVal.ExtType)
		buf.writeUvarint(uint64(len(v.unknownExtVal.Payload)))
		buf.write(v.unknownExtVal.Payload)

	default:
		return fmt.Errorf("cowrie: unsupported type %v (%d)", v.typ, v.typ)
	}

	return nil
}

// uint64FromFloat64 converts a float64 to its bit representation.
func uint64FromFloat64(f float64) uint64 {
	return math.Float64bits(f)
}

// ============================================================
// Graph type encoding helpers
// ============================================================

// encodeNodeData encodes a NodeData struct (without the tag byte).
func encodeNodeData(buf *buffer, node *NodeData, d *dict) error {
	// ID
	buf.writeString(node.ID)
	// Labels
	buf.writeUvarint(uint64(len(node.Labels)))
	for _, label := range node.Labels {
		buf.writeString(label)
	}
	// Properties (dictionary-coded keys)
	buf.writeUvarint(uint64(len(node.Props)))
	for k, propVal := range node.Props {
		buf.writeUvarint(uint64(d.get(k)))
		if err := encodeAny(buf, propVal, d); err != nil {
			return err
		}
	}
	return nil
}

// encodeEdgeData encodes an EdgeData struct (without the tag byte).
func encodeEdgeData(buf *buffer, edge *EdgeData, d *dict) error {
	// Source and destination IDs
	buf.writeString(edge.From)
	buf.writeString(edge.To)
	// Edge type
	buf.writeString(edge.Type)
	// Properties (dictionary-coded keys)
	buf.writeUvarint(uint64(len(edge.Props)))
	for k, propVal := range edge.Props {
		buf.writeUvarint(uint64(d.get(k)))
		if err := encodeAny(buf, propVal, d); err != nil {
			return err
		}
	}
	return nil
}

// encodeAny encodes a Go any value to Cowrie.
// Supports basic types: nil, bool, int/int64, uint/uint64, float64, string, []byte,
// []any, map[string]any, and *Value.
func encodeAny(buf *buffer, v any, d *dict) error {
	if v == nil {
		buf.writeByte(TagNull)
		return nil
	}
	switch val := v.(type) {
	case bool:
		if val {
			buf.writeByte(TagTrue)
		} else {
			buf.writeByte(TagFalse)
		}
	case int:
		buf.writeByte(TagInt64)
		buf.writeUvarint(zigzagEncode(int64(val)))
	case int64:
		buf.writeByte(TagInt64)
		buf.writeUvarint(zigzagEncode(val))
	case uint:
		buf.writeByte(TagUint64)
		buf.writeUvarint(uint64(val))
	case uint64:
		buf.writeByte(TagUint64)
		buf.writeUvarint(val)
	case float64:
		buf.writeByte(TagFloat64)
		var fbuf [8]byte
		binary.LittleEndian.PutUint64(fbuf[:], uint64FromFloat64(val))
		buf.write(fbuf[:])
	case string:
		buf.writeByte(TagString)
		buf.writeString(val)
	case []byte:
		buf.writeByte(TagBytes)
		buf.writeUvarint(uint64(len(val)))
		buf.write(val)
	case []any:
		buf.writeByte(TagArray)
		buf.writeUvarint(uint64(len(val)))
		for _, item := range val {
			if err := encodeAny(buf, item, d); err != nil {
				return err
			}
		}
	case map[string]any:
		buf.writeByte(TagObject)
		buf.writeUvarint(uint64(len(val)))
		for k, v := range val {
			buf.writeUvarint(uint64(d.get(k)))
			if err := encodeAny(buf, v, d); err != nil {
				return err
			}
		}
	case *Value:
		if val == nil {
			buf.writeByte(TagNull)
		} else {
			return encodeValue(buf, val, d)
		}
	default:
		return fmt.Errorf("cowrie: unsupported property type %T", v)
	}
	return nil
}

// EncodeOptions controls encoding behavior.
type EncodeOptions struct {
	// Deterministic sorts object keys lexicographically for reproducible output.
	// This is essential for hashing, caching, and content addressing.
	Deterministic bool

	// OmitNull excludes null values from encoded objects.
	// Useful for sparse data and reducing payload size.
	OmitNull bool
}

// EncodeWithOptions encodes a value with explicit options.
func EncodeWithOptions(v *Value, opts EncodeOptions) ([]byte, error) {
	if opts.OmitNull {
		v = omitNulls(v)
	}
	if opts.Deterministic {
		v = canonicalize(v)
	}
	return Encode(v)
}

// omitNulls returns a copy of v with all null values removed from objects.
// For non-object types, returns the same value. Recursively processes nested values.
func omitNulls(v *Value) *Value {
	if v == nil {
		return nil
	}

	switch v.Type() {
	case TypeObject:
		members := v.Members()
		if len(members) == 0 {
			return v
		}

		// Filter out null values and recursively process nested values
		filtered := make([]Member, 0, len(members))
		for _, m := range members {
			if m.Value != nil && m.Value.Type() != TypeNull {
				filtered = append(filtered, Member{
					Key:   m.Key,
					Value: omitNulls(m.Value),
				})
			}
		}

		return Object(filtered...)

	case TypeArray:
		arr := v.Array()
		if len(arr) == 0 {
			return v
		}

		// Recursively process array elements (don't filter nulls from arrays)
		processed := make([]*Value, len(arr))
		for i, elem := range arr {
			processed[i] = omitNulls(elem)
		}

		return Array(processed...)

	default:
		return v
	}
}

// canonicalize returns a copy of v with all object keys sorted lexicographically.
// For non-object types, returns the same value. For objects and arrays, recursively
// canonicalizes nested values.
func canonicalize(v *Value) *Value {
	if v == nil {
		return nil
	}

	switch v.Type() {
	case TypeObject:
		members := v.Members()
		if len(members) == 0 {
			return v
		}

		// Create sorted copy of members
		sorted := make([]Member, len(members))
		copy(sorted, members)
		sortMembers(sorted)

		// Recursively canonicalize nested values
		for i := range sorted {
			sorted[i].Value = canonicalize(sorted[i].Value)
		}

		return Object(sorted...)

	case TypeArray:
		arr := v.Array()
		if len(arr) == 0 {
			return v
		}

		// Recursively canonicalize array elements
		canonical := make([]*Value, len(arr))
		for i, elem := range arr {
			canonical[i] = canonicalize(elem)
		}

		return Array(canonical...)

	default:
		// Scalars and other types don't need canonicalization
		return v
	}
}

// sortMembers sorts members by key in lexicographic order.
func sortMembers(members []Member) {
	// Simple insertion sort - efficient for small arrays (typical JSON)
	for i := 1; i < len(members); i++ {
		key := members[i]
		j := i - 1
		for j >= 0 && members[j].Key > key.Key {
			members[j+1] = members[j]
			j--
		}
		members[j+1] = key
	}
}
