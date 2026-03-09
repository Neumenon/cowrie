package cowrie

import "time"

// Null returns a null value.
func Null() *Value {
	return &Value{typ: TypeNull}
}

// Bool returns a boolean value.
func Bool(b bool) *Value {
	return &Value{typ: TypeBool, boolVal: b}
}

// Int64 returns an int64 value.
func Int64(i int64) *Value {
	return &Value{typ: TypeInt64, int64Val: i}
}

// Uint64 returns a uint64 value.
func Uint64(u uint64) *Value {
	return &Value{typ: TypeUint64, uint64Val: u}
}

// Float64 returns a float64 value.
func Float64(f float64) *Value {
	return &Value{typ: TypeFloat64, float64Val: f}
}

// NewDecimal128 returns a decimal128 value.
func NewDecimal128(scale int8, coef [16]byte) *Value {
	return &Value{typ: TypeDecimal128, decimal128: Decimal128{Scale: scale, Coef: coef}}
}

// String returns a string value.
func String(s string) *Value {
	return &Value{typ: TypeString, stringVal: s}
}

// Bytes returns a bytes value.
func Bytes(b []byte) *Value {
	// Make a copy to avoid aliasing
	cpy := make([]byte, len(b))
	copy(cpy, b)
	return &Value{typ: TypeBytes, bytesVal: cpy}
}

// Datetime64 returns a datetime64 value from nanoseconds since Unix epoch.
func Datetime64(nanos int64) *Value {
	return &Value{typ: TypeDatetime64, datetime64: nanos}
}

// Datetime returns a datetime64 value from a time.Time.
func Datetime(t time.Time) *Value {
	return &Value{typ: TypeDatetime64, datetime64: t.UnixNano()}
}

// UUID128 returns a uuid128 value.
func UUID128(uuid [16]byte) *Value {
	return &Value{typ: TypeUUID128, uuid128: uuid}
}

// BigInt returns a bigint value from two's complement big-endian bytes.
func BigInt(b []byte) *Value {
	// Make a copy to avoid aliasing
	cpy := make([]byte, len(b))
	copy(cpy, b)
	return &Value{typ: TypeBigInt, bigintVal: cpy}
}

// Array returns an empty array value.
func Array(items ...*Value) *Value {
	return &Value{typ: TypeArray, arrayVal: items}
}

// Object returns an empty object value.
func Object(members ...Member) *Value {
	return &Value{typ: TypeObject, objectVal: members}
}

// ObjectFromMap returns an object from a map of values.
func ObjectFromMap(m map[string]*Value) *Value {
	members := make([]Member, 0, len(m))
	for k, v := range m {
		members = append(members, Member{Key: k, Value: v})
	}
	return &Value{typ: TypeObject, objectVal: members}
}

// ============================================================
// v2.1 Extension Type Constructors
// ============================================================

// Tensor returns a tensor value.
func Tensor(dtype DType, dims []uint64, data []byte) *Value {
	// Make copies to avoid aliasing
	dimsCopy := make([]uint64, len(dims))
	copy(dimsCopy, dims)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &Value{
		typ: TypeTensor,
		tensorVal: TensorData{
			DType: dtype,
			Dims:  dimsCopy,
			Data:  dataCopy,
		},
	}
}

// TensorRef returns a tensor reference value.
func TensorRef(storeID uint8, key []byte) *Value {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return &Value{
		typ: TypeTensorRef,
		tensorRefVal: TensorRefData{
			StoreID: storeID,
			Key:     keyCopy,
		},
	}
}

// Image returns an image value.
func Image(format ImageFormat, width, height uint16, data []byte) *Value {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &Value{
		typ: TypeImage,
		imageVal: ImageData{
			Format: format,
			Width:  width,
			Height: height,
			Data:   dataCopy,
		},
	}
}

// Audio returns an audio value.
func Audio(encoding AudioEncoding, sampleRate uint32, channels uint8, data []byte) *Value {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &Value{
		typ: TypeAudio,
		audioVal: AudioData{
			Encoding:   encoding,
			SampleRate: sampleRate,
			Channels:   channels,
			Data:       dataCopy,
		},
	}
}

// Adjlist returns an adjacency list value (CSR format).
func Adjlist(idWidth IDWidth, nodeCount, edgeCount uint64, rowOffsets []uint64, colIndices []byte) *Value {
	rowOffsetsCopy := make([]uint64, len(rowOffsets))
	copy(rowOffsetsCopy, rowOffsets)
	colIndicesCopy := make([]byte, len(colIndices))
	copy(colIndicesCopy, colIndices)
	return &Value{
		typ: TypeAdjlist,
		adjlistVal: AdjlistData{
			IDWidth:    idWidth,
			NodeCount:  nodeCount,
			EdgeCount:  edgeCount,
			RowOffsets: rowOffsetsCopy,
			ColIndices: colIndicesCopy,
		},
	}
}

// RichText returns a rich text value with optional tokens and spans.
func RichText(text string, tokens []int32, spans []RichTextSpan) *Value {
	var tokensCopy []int32
	if tokens != nil {
		tokensCopy = make([]int32, len(tokens))
		copy(tokensCopy, tokens)
	}
	var spansCopy []RichTextSpan
	if spans != nil {
		spansCopy = make([]RichTextSpan, len(spans))
		copy(spansCopy, spans)
	}
	return &Value{
		typ: TypeRichText,
		richTextVal: RichTextData{
			Text:   text,
			Tokens: tokensCopy,
			Spans:  spansCopy,
		},
	}
}

// Delta returns a delta value (semantic diff/patch).
func Delta(baseID uint64, ops []DeltaOp) *Value {
	opsCopy := make([]DeltaOp, len(ops))
	copy(opsCopy, ops)
	return &Value{
		typ: TypeDelta,
		deltaVal: DeltaData{
			BaseID: baseID,
			Ops:    opsCopy,
		},
	}
}

// Bitmask returns a bitmask value from a count and packed bits.
// Bits should be ceil(count/8) bytes, LSB-first within each byte.
func Bitmask(count uint64, bits []byte) *Value {
	expected := int((count + 7) / 8)
	bitsCopy := make([]byte, expected)
	copy(bitsCopy, bits)
	// Clear trailing bits beyond count
	if count%8 != 0 && expected > 0 {
		mask := byte((1 << (count % 8)) - 1)
		bitsCopy[expected-1] &= mask
	}
	return &Value{
		typ: TypeBitmask,
		bitmaskVal: BitmaskData{
			Count: count,
			Bits:  bitsCopy,
		},
	}
}

// BitmaskFromBools creates a bitmask from a slice of booleans.
func BitmaskFromBools(bools []bool) *Value {
	count := uint64(len(bools))
	byteLen := (count + 7) / 8
	bits := make([]byte, byteLen)
	for i, b := range bools {
		if b {
			bits[i/8] |= 1 << (uint(i) % 8)
		}
	}
	return &Value{
		typ: TypeBitmask,
		bitmaskVal: BitmaskData{
			Count: count,
			Bits:  bits,
		},
	}
}

// UnknownExtension returns an unknown extension value.
// This is typically created by the decoder when it encounters a TagExt
// with an unrecognized ExtType and OnUnknownExt is set to Keep.
func UnknownExtension(extType uint64, payload []byte) *Value {
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)
	return &Value{
		typ: TypeUnknownExt,
		unknownExtVal: UnknownExtData{
			ExtType: extType,
			Payload: payloadCopy,
		},
	}
}

// ============================================================
// v2.1 Graph Type Constructors
// ============================================================

// Node returns a graph node value.
func Node(id string, labels []string, props map[string]any) *Value {
	labelsCopy := make([]string, len(labels))
	copy(labelsCopy, labels)
	propsCopy := make(map[string]any, len(props))
	for k, v := range props {
		propsCopy[k] = v
	}
	return &Value{
		typ: TypeNode,
		nodeVal: NodeData{
			ID:     id,
			Labels: labelsCopy,
			Props:  propsCopy,
		},
	}
}

// Edge returns a graph edge value.
func Edge(from, to, edgeType string, props map[string]any) *Value {
	propsCopy := make(map[string]any, len(props))
	for k, v := range props {
		propsCopy[k] = v
	}
	return &Value{
		typ: TypeEdge,
		edgeVal: EdgeData{
			From:  from,
			To:    to,
			Type:  edgeType,
			Props: propsCopy,
		},
	}
}

// NodeBatch returns a batch of nodes value.
func NodeBatch(nodes []NodeData) *Value {
	nodesCopy := make([]NodeData, len(nodes))
	copy(nodesCopy, nodes)
	return &Value{
		typ:          TypeNodeBatch,
		nodeBatchVal: NodeBatchData{Nodes: nodesCopy},
	}
}

// EdgeBatch returns a batch of edges value.
func EdgeBatch(edges []EdgeData) *Value {
	edgesCopy := make([]EdgeData, len(edges))
	copy(edgesCopy, edges)
	return &Value{
		typ:          TypeEdgeBatch,
		edgeBatchVal: EdgeBatchData{Edges: edgesCopy},
	}
}

// GraphShard returns a graph shard value (self-contained subgraph).
func GraphShard(nodes []NodeData, edges []EdgeData, metadata map[string]any) *Value {
	nodesCopy := make([]NodeData, len(nodes))
	copy(nodesCopy, nodes)
	edgesCopy := make([]EdgeData, len(edges))
	copy(edgesCopy, edges)
	metaCopy := make(map[string]any, len(metadata))
	for k, v := range metadata {
		metaCopy[k] = v
	}
	return &Value{
		typ: TypeGraphShard,
		graphShardVal: GraphShardData{
			Nodes:    nodesCopy,
			Edges:    edgesCopy,
			Metadata: metaCopy,
		},
	}
}
