// Package gen1 implements GEN-1 Cowrie: a compact binary JSON format
// with proto-tensor support for numeric arrays.
//
// This is the "legacy" codec that provides size savings without full
// TensorV1/UCodec complexity. Perfect for:
//   - JSON APIs that want binary efficiency
//   - ML data with float arrays
//   - Any structured data with numeric payloads
package gen1

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"sync"
)

// Type tags - aligned with cross-language SPEC
const (
	tagNull         = 0x00
	tagFalse        = 0x01
	tagTrue         = 0x02
	tagInt64        = 0x03
	tagFloat64      = 0x04
	tagString       = 0x05
	tagArrayGeneric = 0x06 // heterogeneous array (v3: aligned with Gen2)
	tagObject       = 0x07 // object/map (v3: aligned with Gen2)
	tagBytes        = 0x08 // binary bytes (v3: aligned with Gen2)
	tagArrayInt64   = 0x09 // homogeneous int64 array (proto-tensor)
	tagArrayFloat64 = 0x0A // homogeneous float64 array (proto-tensor)
	tagArrayString  = 0x0B // homogeneous string array
	tagArrayFloat32 = 0x0C // homogeneous float32 array (Go extension, 4 bytes/float)
	tagFloat32      = 0x0D // scalar float32 (compact float, 4 bytes vs 8)

	// Graph types (v3: aligned with Gen2 at 0x30+0x35-0x39)
	tagAdjList    = 0x30 // Adjacency list: nodeID + neighbors (int64 array)
	tagNode       = 0x35 // Graph node: id + labels + props
	tagEdge       = 0x36 // Graph edge: id + type + from + to + props
	tagNodeBatch  = 0x37 // Batch of nodes (for streaming)
	tagEdgeBatch  = 0x38 // Batch of edges (COO format)
	tagGraphShard = 0x39 // Graph container: nodes + edges + adjacency + features
)

// NumericArrayMin is the threshold for promoting arrays to proto-tensors.
// Arrays smaller than this use generic encoding.
// Recommended threshold: 4 elements (per SPEC.md)
const NumericArrayMin = 4

// =============================================================================
// Security Limits - prevent memory exhaustion from malicious input
// =============================================================================

// Default security limits (tightened based on msgpack-go best practices)
const (
	DefaultMaxDepth     = 1000      // Maximum nesting depth
	DefaultMaxArrayLen  = 1000000   // 1M elements (was 100M)
	DefaultMaxObjectLen = 1000000   // 1M fields (was 10M)
	DefaultMaxStringLen = 10000000  // 10MB (was 500MB)
	DefaultMaxBytesLen  = 50000000  // 50MB (was 1GB)
)

// DecodeOptions configures security limits for decoding.
type DecodeOptions struct {
	MaxDepth     int   // Maximum nesting depth (0 = use default)
	MaxArrayLen  int64 // Maximum array element count
	MaxObjectLen int64 // Maximum object field count
	MaxStringLen int64 // Maximum string byte length
	MaxBytesLen  int64 // Maximum bytes length
}

// DefaultDecodeOptions returns options with default security limits.
func DefaultDecodeOptions() DecodeOptions {
	return DecodeOptions{
		MaxDepth:     DefaultMaxDepth,
		MaxArrayLen:  DefaultMaxArrayLen,
		MaxObjectLen: DefaultMaxObjectLen,
		MaxStringLen: DefaultMaxStringLen,
		MaxBytesLen:  DefaultMaxBytesLen,
	}
}

// global decode options used by Decode()
var globalDecodeOptions = DefaultDecodeOptions()

// Security errors
var (
	ErrMaxDepthExceeded = errors.New("cowrie: maximum nesting depth exceeded")
	ErrMaxArrayLen      = errors.New("cowrie: array too large")
	ErrMaxObjectLen     = errors.New("cowrie: object has too many fields")
	ErrMaxStringLen     = errors.New("cowrie: string too long")
	ErrMaxBytesLen      = errors.New("cowrie: bytes too long")
	ErrIntegerOverflow  = errors.New("cowrie: integer overflow in size calculation")
)

// Decode errors — sentinel values for reliable error checking (replaces string matching)
var (
	ErrUnexpectedEOF     = errors.New("unexpected EOF")
	ErrShortFloat64      = errors.New("short float64")
	ErrShortFloat32      = errors.New("short float32")
	ErrShortFloat64Array = errors.New("short float64 array")
	ErrShortFloat32Array = errors.New("short float32 array")
	ErrShortInt64Array   = errors.New("short int64 array")
	ErrShortString       = errors.New("short string")
	ErrShortBytes        = errors.New("short bytes")
	ErrShortKey          = errors.New("short key")
	ErrShortStringArray  = errors.New("short string in string array")
	ErrInvalidUvarint    = errors.New("invalid uvarint")
	ErrInvalidVarint     = errors.New("invalid varint")
	ErrUnknownTag        = errors.New("unknown tag")
	ErrShortData         = errors.New("short data")
	ErrExpectedObjectTag = errors.New("expected object tag")
	ErrIncompleteRecord  = errors.New("unexpected EOF: incomplete record")
)

// Buffer pool for encoding - reduces allocations in hot paths
var bufferPool = sync.Pool{
	New: func() any {
		// Start with 4KB buffer, will grow as needed
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// getBuffer gets a buffer from the pool
func getBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// putBuffer returns a buffer to the pool.
// Buffers larger than 1MB are not pooled to prevent memory bloat.
func putBuffer(buf *[]byte) {
	if cap(*buf) > 1<<20 {
		return // don't pool oversized buffers
	}
	// Reset length but keep capacity
	*buf = (*buf)[:0]
	bufferPool.Put(buf)
}

// =============================================================================
// Graph Types - Lightweight GNN support for Gen 1
// =============================================================================

// Node represents a graph node with optional labels and properties.
type Node struct {
	ID     string         // Node identifier
	Labels []string       // Optional labels (e.g., ["Person", "Employee"])
	Props  map[string]any // Optional properties
}

// Edge represents a graph edge with optional type and properties.
type Edge struct {
	ID    string         // Optional edge identifier
	Type  string         // Edge type/label (e.g., "KNOWS", "FOLLOWS")
	From  string         // Source node ID
	To    string         // Target node ID
	Props map[string]any // Optional properties
}

// AdjList represents an adjacency list for a single node.
// Efficient for neighborhood queries.
type AdjList struct {
	NodeID    int64   // Node ID (integer for efficiency)
	Neighbors []int64 // Connected node IDs
}

// NodeBatch is a batch of nodes for streaming ingestion.
type NodeBatch struct {
	Nodes []Node
}

// EdgeBatch is a batch of edges in COO (coordinate) format.
// Efficient for bulk graph loading.
type EdgeBatch struct {
	Sources []int64          // Source node IDs
	Targets []int64          // Target node IDs
	Types   []string         // Optional edge types (nil if homogeneous)
	Props   []map[string]any // Optional per-edge properties (nil if none)
}

// GraphShard is a complete graph container optimized for GNN workloads.
// It combines nodes, edges, adjacency lists, and optional node/edge features
// into a single self-contained unit that can be efficiently serialized.
//
// Use cases:
//   - Storing subgraphs for mini-batch training
//   - Caching graph partitions
//   - Streaming graph data between services
//   - Checkpointing GNN model inputs
type GraphShard struct {
	// Metadata
	Name     string         // Optional shard name/identifier
	Metadata map[string]any // Optional metadata (schema version, partition info, etc.)

	// Graph structure
	Nodes []Node // Node definitions with properties
	Edges []Edge // Edge definitions with properties

	// Efficient representations for GNN
	// COO format: parallel arrays for source/target node indices
	EdgeIndex [][]int64 // [2][num_edges] - row 0 = sources, row 1 = targets

	// Adjacency lists for fast neighborhood access
	AdjLists []AdjList // Per-node adjacency lists

	// Node features as dense tensor (float32 for efficiency)
	// Shape: [num_nodes, feature_dim]
	NodeFeatures [][]float64

	// Edge features as dense tensor
	// Shape: [num_edges, feature_dim]
	EdgeFeatures [][]float64

	// Optional: node labels for classification tasks
	NodeLabels []int64

	// Optional: edge labels for link prediction
	EdgeLabels []int64
}

// NewGraphShard creates a new empty GraphShard with the given name.
func NewGraphShard(name string) *GraphShard {
	return &GraphShard{
		Name:     name,
		Metadata: make(map[string]any),
	}
}

// AddNode adds a node to the shard and returns its index.
func (gs *GraphShard) AddNode(node Node) int {
	idx := len(gs.Nodes)
	gs.Nodes = append(gs.Nodes, node)
	return idx
}

// AddEdge adds an edge to the shard and returns its index.
func (gs *GraphShard) AddEdge(edge Edge) int {
	idx := len(gs.Edges)
	gs.Edges = append(gs.Edges, edge)
	return idx
}

// SetNodeFeatures sets the node feature matrix.
// features should be [num_nodes][feature_dim].
func (gs *GraphShard) SetNodeFeatures(features [][]float64) {
	gs.NodeFeatures = features
}

// SetEdgeIndex sets the edge index in COO format.
// sources and targets should have the same length.
func (gs *GraphShard) SetEdgeIndex(sources, targets []int64) {
	gs.EdgeIndex = [][]int64{sources, targets}
}

// BuildAdjLists constructs adjacency lists from the edge index.
// Call this after setting EdgeIndex if you need neighborhood queries.
func (gs *GraphShard) BuildAdjLists() {
	if len(gs.EdgeIndex) != 2 {
		return
	}
	sources := gs.EdgeIndex[0]
	targets := gs.EdgeIndex[1]

	// Find max node ID
	maxNode := int64(0)
	for _, s := range sources {
		if s > maxNode {
			maxNode = s
		}
	}
	for _, t := range targets {
		if t > maxNode {
			maxNode = t
		}
	}

	// Build adjacency map
	adjMap := make(map[int64][]int64)
	for i := range sources {
		adjMap[sources[i]] = append(adjMap[sources[i]], targets[i])
	}

	// Convert to AdjList slice
	gs.AdjLists = make([]AdjList, 0, len(adjMap))
	for nodeID, neighbors := range adjMap {
		gs.AdjLists = append(gs.AdjLists, AdjList{
			NodeID:    nodeID,
			Neighbors: neighbors,
		})
	}
}

// NumNodes returns the number of nodes in the shard.
func (gs *GraphShard) NumNodes() int {
	return len(gs.Nodes)
}

// NumEdges returns the number of edges in the shard.
func (gs *GraphShard) NumEdges() int {
	if len(gs.EdgeIndex) == 2 && len(gs.EdgeIndex[0]) > 0 {
		return len(gs.EdgeIndex[0])
	}
	return len(gs.Edges)
}

// =============================================================================
// Encode Options - Control precision and behavior
// =============================================================================

// EncodeOptions controls encoding behavior.
type EncodeOptions struct {
	// HighPrecision preserves float64 precision instead of converting to float32.
	// When false (default), float64 slices are encoded as float32 for ~50% size reduction.
	// When true, float64 slices are encoded as float64 (larger but lossless).
	//
	// Use HighPrecision for:
	//   - Financial data requiring exact decimal representation
	//   - Scientific constants with many significant digits
	//   - Cryptographic values where bit-exactness matters
	//
	// Default (false) is safe for:
	//   - ML embeddings and features
	//   - Sensor data and measurements
	//   - Graphics and game data
	HighPrecision bool

	// CompactFloats encodes individual float64 values as float32 when lossless.
	// A float64 value is compact-eligible if float64(float32(v)) == v and it's
	// not NaN. This saves 4 bytes per qualifying float (5 bytes vs 9 bytes).
	// Array floats already have their own compact encoding via HighPrecision.
	CompactFloats bool
}

// DefaultEncodeOptions returns the default encoding options.
// Default prioritizes cross-language compatibility (HighPrecision: true).
// Use HighPrecision: false for Go-only workloads where ~50% size reduction
// on float arrays is desired (uses non-standard tag 0x0C for Float32).
func DefaultEncodeOptions() EncodeOptions {
	return EncodeOptions{
		HighPrecision: true,
	}
}

// globalEncodeOptions holds the current default encoding options.
// Can be changed via SetDefaultEncodeOptions.
var globalEncodeOptions = DefaultEncodeOptions()

// SetDefaultEncodeOptions sets the global default encoding options.
// This affects all Encode calls that don't specify options.
func SetDefaultEncodeOptions(opts EncodeOptions) {
	globalEncodeOptions = opts
}

// Encode encodes arbitrary Go data into GEN-1 Cowrie binary format.
func Encode(v any) ([]byte, error) {
	return EncodeWithOptions(v, globalEncodeOptions)
}

// EncodeWithOptions encodes with explicit options.
func EncodeWithOptions(v any, opts EncodeOptions) ([]byte, error) {
	// Get buffer from pool
	bufPtr := getBuffer()
	result, err := appendValueWithOpts(*bufPtr, v, opts)
	if err != nil {
		putBuffer(bufPtr)
		return nil, err
	}
	// Copy result to new slice (can't return pooled buffer)
	out := make([]byte, len(result))
	copy(out, result)
	// Return buffer to pool
	*bufPtr = result
	putBuffer(bufPtr)
	return out, nil
}

// EncodeAppend encodes v and appends the result to buf.
// Useful for avoiding allocations when the caller manages buffers.
func EncodeAppend(buf []byte, v any) ([]byte, error) {
	return appendValueWithOpts(buf, v, globalEncodeOptions)
}

// EncodeAppendWithOptions encodes v with options and appends the result to buf.
func EncodeAppendWithOptions(buf []byte, v any, opts EncodeOptions) ([]byte, error) {
	return appendValueWithOpts(buf, v, opts)
}

// Decode decodes GEN-1 Cowrie into a Go value compatible with json.Marshal.
func Decode(data []byte) (any, error) {
	return DecodeWithOptions(data, globalDecodeOptions)
}

// DecodeWithOptions decodes with custom security limits.
// Thread-safe: options are passed through recursion, not stored globally.
func DecodeWithOptions(data []byte, opts DecodeOptions) (any, error) {
	// Apply defaults for any zero values
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxDepth
	}
	if opts.MaxArrayLen <= 0 {
		opts.MaxArrayLen = DefaultMaxArrayLen
	}
	if opts.MaxObjectLen <= 0 {
		opts.MaxObjectLen = DefaultMaxObjectLen
	}
	if opts.MaxStringLen <= 0 {
		opts.MaxStringLen = DefaultMaxStringLen
	}
	if opts.MaxBytesLen <= 0 {
		opts.MaxBytesLen = DefaultMaxBytesLen
	}

	v, _, err := readValue(data, 0, opts)
	return v, err
}

// EncodeJSON encodes raw JSON bytes into GEN-1 Cowrie.
func EncodeJSON(raw []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return Encode(v)
}

// DecodeJSON decodes GEN-1 Cowrie back to JSON bytes.
func DecodeJSON(data []byte) ([]byte, error) {
	v, err := Decode(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// EncodeTo writes encoded data to a writer (for streaming).
func EncodeTo(w io.Writer, v any) error {
	data, err := Encode(v)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// =============================================================================
// StreamDecoder - Safe streaming decode without data loss
// =============================================================================

// StreamDecoder provides safe streaming decoding for any reader.
// It properly handles unconsumed bytes and works correctly with
// net.Conn, gzip.Reader, bytes.Reader, and other readers.
//
// Example usage:
//
//	dec := gen1.NewStreamDecoder(conn)
//	for {
//	    v, err := dec.Decode()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    process(v)
//	}
type StreamDecoder struct {
	r    io.Reader
	buf  []byte        // Accumulator for incomplete records
	tmp  []byte        // Reusable read buffer (avoid per-Decode alloc)
	opts DecodeOptions // Thread-safe: each decoder owns its options
}

// NewStreamDecoder creates a StreamDecoder for the given reader.
// The StreamDecoder maintains an internal buffer to handle record boundaries
// correctly, even for non-seekable readers like net.Conn or gzip.Reader.
func NewStreamDecoder(r io.Reader) *StreamDecoder {
	return &StreamDecoder{
		r:    r,
		buf:  make([]byte, 0, 4096),
		tmp:  make([]byte, 4096),
		opts: DefaultDecodeOptions(),
	}
}

// NewStreamDecoderWithOptions creates a StreamDecoder with custom security limits.
func NewStreamDecoderWithOptions(r io.Reader, opts DecodeOptions) *StreamDecoder {
	d := NewStreamDecoder(r)
	d.opts = opts
	return d
}

// Decode reads and decodes one value from the stream.
// Returns io.EOF when no more data is available.
// Safe for use with non-seekable readers (net.Conn, gzip.Reader, etc.).
func (d *StreamDecoder) Decode() (any, error) {
	for {
		// Try to decode from existing buffer first
		if len(d.buf) > 0 {
			v, consumed, decErr := readValue(d.buf, 0, d.opts)
			if decErr == nil {
				// Success! Keep unconsumed bytes for next decode
				if consumed < len(d.buf) {
					// Buffer rewind: if all data consumed, just reset length (no memmove)
					remaining := len(d.buf) - consumed
					copy(d.buf[:remaining], d.buf[consumed:])
					d.buf = d.buf[:remaining]
				} else {
					d.buf = d.buf[:0]
				}
				return v, nil
			}
			if !isIncompleteError(decErr) {
				return nil, decErr
			}
			// Need more data, fall through to read
		}

		// Read more data
		n, err := d.r.Read(d.tmp)
		if n > 0 {
			d.buf = append(d.buf, d.tmp[:n]...)
			continue // Try decode again with more data
		}

		if err == io.EOF {
			if len(d.buf) == 0 {
				return nil, io.EOF
			}
			// Have data but can't decode - try one more time
			v, _, decErr := readValue(d.buf, 0, d.opts)
			if decErr == nil {
				d.buf = d.buf[:0]
				return v, nil
			}
			return nil, ErrIncompleteRecord
		}
		if err != nil {
			return nil, err
		}
	}
}

// isIncompleteError checks if the decode error indicates we need more data.
// Uses errors.Is with sentinel errors for reliable matching.
func isIncompleteError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrUnexpectedEOF) ||
		errors.Is(err, ErrShortFloat64) ||
		errors.Is(err, ErrShortFloat32) ||
		errors.Is(err, ErrShortFloat64Array) ||
		errors.Is(err, ErrShortFloat32Array) ||
		errors.Is(err, ErrShortInt64Array) ||
		errors.Is(err, ErrShortString) ||
		errors.Is(err, ErrShortBytes) ||
		errors.Is(err, ErrShortKey) ||
		errors.Is(err, ErrShortStringArray) ||
		errors.Is(err, ErrShortData)
}

// ---------- Encoding ----------

func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendVarint(buf []byte, x int64) []byte {
	var tmp [10]byte
	n := binary.PutVarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendFloat64(buf []byte, f float64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(f))
	return append(buf, tmp[:]...)
}

func appendFloat32(buf []byte, f float32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(f))
	return append(buf, tmp[:]...)
}

// canCompactFloat returns true if a float64 can be losslessly represented as float32.
// NaN and Inf are excluded (they have special representations that differ between widths).
func canCompactFloat(f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	return float64(float32(f)) == f
}

// appendValue encodes with default options (backward compatible).
func appendValue(buf []byte, v any) ([]byte, error) {
	return appendValueWithOpts(buf, v, globalEncodeOptions)
}

// appendValueWithOpts encodes with explicit options.
func appendValueWithOpts(buf []byte, v any, opts EncodeOptions) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(buf, tagNull), nil

	case bool:
		if x {
			return append(buf, tagTrue), nil
		}
		return append(buf, tagFalse), nil

	case float64:
		if opts.CompactFloats && canCompactFloat(x) {
			buf = append(buf, tagFloat32)
			return appendFloat32(buf, float32(x)), nil
		}
		buf = append(buf, tagFloat64)
		return appendFloat64(buf, x), nil

	case float32:
		if opts.CompactFloats {
			buf = append(buf, tagFloat32)
			return appendFloat32(buf, x), nil
		}
		buf = append(buf, tagFloat64)
		return appendFloat64(buf, float64(x)), nil

	case json.Number:
		if i, err := x.Int64(); err == nil {
			buf = append(buf, tagInt64)
			return appendVarint(buf, i), nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, err
		}
		buf = append(buf, tagFloat64)
		return appendFloat64(buf, f), nil

	case int:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case int8:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case int16:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case int32:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case int64:
		buf = append(buf, tagInt64)
		return appendVarint(buf, x), nil

	case uint:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case uint8:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case uint16:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case uint32:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil
	case uint64:
		buf = append(buf, tagInt64)
		return appendVarint(buf, int64(x)), nil

	case string:
		b := []byte(x)
		buf = append(buf, tagString)
		buf = appendUvarint(buf, uint64(len(b)))
		return append(buf, b...), nil

	case []byte:
		buf = append(buf, tagBytes)
		buf = appendUvarint(buf, uint64(len(x)))
		return append(buf, x...), nil

	case []string:
		buf = append(buf, tagArrayString)
		buf = appendUvarint(buf, uint64(len(x)))
		for _, s := range x {
			b := []byte(s)
			buf = appendUvarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		}
		return buf, nil

	case []float64:
		// HighPrecision: preserve float64, otherwise convert to float32 for ~50% size reduction
		if opts.HighPrecision {
			return appendFloat64Slice(buf, x), nil
		}
		return appendFloat32Slice(buf, x), nil

	case []float32:
		// Directly encode float32 slice (4 bytes per float)
		buf = append(buf, tagArrayFloat32)
		buf = appendUvarint(buf, uint64(len(x)))
		for _, f := range x {
			buf = appendFloat32(buf, f)
		}
		return buf, nil

	case []int:
		ints := make([]int64, len(x))
		for i, v := range x {
			ints[i] = int64(v)
		}
		return appendInt64Slice(buf, ints), nil

	case []int64:
		return appendInt64Slice(buf, x), nil

	case [][]float64:
		// Matrix: encode as generic array of float tensors
		buf = append(buf, tagArrayGeneric)
		buf = appendUvarint(buf, uint64(len(x)))
		for _, row := range x {
			if opts.HighPrecision {
				buf = appendFloat64Slice(buf, row)
			} else {
				buf = appendFloat32Slice(buf, row)
			}
		}
		return buf, nil

	case []any:
		return appendArrayWithOpts(buf, x, opts)

	case map[string]any:
		return appendObjectWithOpts(buf, x, opts)

	// Graph types
	case Node:
		return appendNode(buf, x)
	case *Node:
		return appendNode(buf, *x)
	case Edge:
		return appendEdge(buf, x)
	case *Edge:
		return appendEdge(buf, *x)
	case AdjList:
		return appendAdjList(buf, x)
	case *AdjList:
		return appendAdjList(buf, *x)
	case NodeBatch:
		return appendNodeBatch(buf, x)
	case *NodeBatch:
		return appendNodeBatch(buf, *x)
	case EdgeBatch:
		return appendEdgeBatch(buf, x)
	case *EdgeBatch:
		return appendEdgeBatch(buf, *x)
	case GraphShard:
		return appendGraphShard(buf, x)
	case *GraphShard:
		return appendGraphShard(buf, *x)

	default:
		// Try JSON round-trip for unknown types
		raw, err := json.Marshal(x)
		if err != nil {
			return nil, err
		}
		var v2 any
		if err := json.Unmarshal(raw, &v2); err != nil {
			return nil, err
		}
		return appendValueWithOpts(buf, v2, opts)
	}
}

func appendFloat64Slice(buf []byte, floats []float64) []byte {
	buf = append(buf, tagArrayFloat64)
	buf = appendUvarint(buf, uint64(len(floats)))
	for _, f := range floats {
		buf = appendFloat64(buf, f)
	}
	return buf
}

// appendFloat32Slice encodes as float32 for ~60% compression vs JSON
func appendFloat32Slice(buf []byte, floats []float64) []byte {
	buf = append(buf, tagArrayFloat32)
	buf = appendUvarint(buf, uint64(len(floats)))
	for _, f := range floats {
		buf = appendFloat32(buf, float32(f))
	}
	return buf
}

func appendInt64Slice(buf []byte, ints []int64) []byte {
	buf = append(buf, tagArrayInt64)
	buf = appendUvarint(buf, uint64(len(ints)))
	// SPEC: Int64Array uses fixed 8-byte LE for each element (not varint)
	for _, i := range ints {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(i))
		buf = append(buf, tmp[:]...)
	}
	return buf
}

func appendObject(buf []byte, m map[string]any) ([]byte, error) {
	return appendObjectWithOpts(buf, m, globalEncodeOptions)
}

func appendObjectWithOpts(buf []byte, m map[string]any, opts EncodeOptions) ([]byte, error) {
	buf = append(buf, tagObject)

	// Deterministic order
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf = appendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		kb := []byte(k)
		buf = appendUvarint(buf, uint64(len(kb)))
		buf = append(buf, kb...)

		var err error
		buf, err = appendValueWithOpts(buf, m[k], opts)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func appendArray(buf []byte, arr []any) ([]byte, error) {
	return appendArrayWithOpts(buf, arr, globalEncodeOptions)
}

func appendArrayWithOpts(buf []byte, arr []any, opts EncodeOptions) ([]byte, error) {
	// Try proto-tensor promotion for numeric arrays
	if len(arr) >= NumericArrayMin {
		if floats, ok := asFloatSlice(arr); ok {
			// HighPrecision: preserve float64, otherwise use float32 for ~50% size reduction
			if opts.HighPrecision {
				return appendFloat64Slice(buf, floats), nil
			}
			return appendFloat32Slice(buf, floats), nil
		}
		if ints, ok := asIntSlice(arr); ok {
			return appendInt64Slice(buf, ints), nil
		}
	}

	// Generic array
	buf = append(buf, tagArrayGeneric)
	buf = appendUvarint(buf, uint64(len(arr)))
	for _, el := range arr {
		var err error
		buf, err = appendValueWithOpts(buf, el, opts)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func asFloatSlice(arr []any) ([]float64, bool) {
	out := make([]float64, len(arr))
	for i, v := range arr {
		switch x := v.(type) {
		case float64:
			out[i] = x
		case float32:
			out[i] = float64(x)
		case json.Number:
			f, err := x.Float64()
			if err != nil {
				return nil, false
			}
			out[i] = f
		case int:
			out[i] = float64(x)
		case int64:
			out[i] = float64(x)
		case int32:
			out[i] = float64(x)
		default:
			return nil, false
		}
	}
	return out, true
}

func asIntSlice(arr []any) ([]int64, bool) {
	out := make([]int64, len(arr))
	for i, v := range arr {
		switch x := v.(type) {
		case int:
			out[i] = int64(x)
		case int64:
			out[i] = x
		case int32:
			out[i] = int64(x)
		case int16:
			out[i] = int64(x)
		case int8:
			out[i] = int64(x)
		case json.Number:
			i64, err := x.Int64()
			if err != nil {
				return nil, false
			}
			out[i] = i64
		case float64:
			// Only if it's a whole number
			if x == float64(int64(x)) {
				out[i] = int64(x)
			} else {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return out, true
}

// ---------- Graph Encoding ----------

// appendNode encodes a Node
// Format: tagNode + id:string + labelCount:uvarint + labels... + props:object
func appendNode(buf []byte, n Node) ([]byte, error) {
	buf = append(buf, tagNode)

	// ID (string)
	id := []byte(n.ID)
	buf = appendUvarint(buf, uint64(len(id)))
	buf = append(buf, id...)

	// Labels
	buf = appendUvarint(buf, uint64(len(n.Labels)))
	for _, label := range n.Labels {
		lb := []byte(label)
		buf = appendUvarint(buf, uint64(len(lb)))
		buf = append(buf, lb...)
	}

	// Props (as object, or empty if nil)
	props := n.Props
	if props == nil {
		props = map[string]any{}
	}
	return appendObject(buf, props)
}

// appendEdge encodes an Edge
// Format: tagEdge + id:string + type:string + from:string + to:string + props:object
func appendEdge(buf []byte, e Edge) ([]byte, error) {
	buf = append(buf, tagEdge)

	// ID
	id := []byte(e.ID)
	buf = appendUvarint(buf, uint64(len(id)))
	buf = append(buf, id...)

	// Type
	et := []byte(e.Type)
	buf = appendUvarint(buf, uint64(len(et)))
	buf = append(buf, et...)

	// From
	from := []byte(e.From)
	buf = appendUvarint(buf, uint64(len(from)))
	buf = append(buf, from...)

	// To
	to := []byte(e.To)
	buf = appendUvarint(buf, uint64(len(to)))
	buf = append(buf, to...)

	// Props
	props := e.Props
	if props == nil {
		props = map[string]any{}
	}
	return appendObject(buf, props)
}

// appendAdjList encodes an AdjList
// Format: tagAdjList + nodeID:varint + count:uvarint + neighbors:varint...
func appendAdjList(buf []byte, a AdjList) ([]byte, error) {
	buf = append(buf, tagAdjList)
	buf = appendVarint(buf, a.NodeID)
	buf = appendUvarint(buf, uint64(len(a.Neighbors)))
	for _, n := range a.Neighbors {
		buf = appendVarint(buf, n)
	}
	return buf, nil
}

// appendNodeBatch encodes a NodeBatch
// Format: tagNodeBatch + count:uvarint + nodes...
func appendNodeBatch(buf []byte, nb NodeBatch) ([]byte, error) {
	buf = append(buf, tagNodeBatch)
	buf = appendUvarint(buf, uint64(len(nb.Nodes)))
	for _, n := range nb.Nodes {
		var err error
		// Encode node without tag (inline)
		buf, err = appendNodeInline(buf, n)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// appendNodeInline encodes a node without the tag (for batches)
func appendNodeInline(buf []byte, n Node) ([]byte, error) {
	// ID
	id := []byte(n.ID)
	buf = appendUvarint(buf, uint64(len(id)))
	buf = append(buf, id...)

	// Labels
	buf = appendUvarint(buf, uint64(len(n.Labels)))
	for _, label := range n.Labels {
		lb := []byte(label)
		buf = appendUvarint(buf, uint64(len(lb)))
		buf = append(buf, lb...)
	}

	// Props
	props := n.Props
	if props == nil {
		props = map[string]any{}
	}
	return appendObject(buf, props)
}

// appendEdgeBatch encodes an EdgeBatch in COO format
// Format: tagEdgeBatch + edgeCount:uvarint + hasTypes:byte + hasProps:byte
//   - sources:varint... + targets:varint...
//   - [types:string...] + [props:object...]
func appendEdgeBatch(buf []byte, eb EdgeBatch) ([]byte, error) {
	buf = append(buf, tagEdgeBatch)

	edgeCount := len(eb.Sources)
	buf = appendUvarint(buf, uint64(edgeCount))

	// Flags
	hasTypes := byte(0)
	if eb.Types != nil && len(eb.Types) == edgeCount {
		hasTypes = 1
	}
	hasProps := byte(0)
	if eb.Props != nil && len(eb.Props) == edgeCount {
		hasProps = 1
	}
	buf = append(buf, hasTypes, hasProps)

	// Sources (COO column 1)
	for _, src := range eb.Sources {
		buf = appendVarint(buf, src)
	}

	// Targets (COO column 2)
	for _, tgt := range eb.Targets {
		buf = appendVarint(buf, tgt)
	}

	// Types (optional)
	if hasTypes == 1 {
		for _, t := range eb.Types {
			tb := []byte(t)
			buf = appendUvarint(buf, uint64(len(tb)))
			buf = append(buf, tb...)
		}
	}

	// Props (optional)
	if hasProps == 1 {
		for _, p := range eb.Props {
			props := p
			if props == nil {
				props = map[string]any{}
			}
			var err error
			buf, err = appendObject(buf, props)
			if err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
}

// appendGraphShard encodes a GraphShard container
// Format: tagGraphShard + flags:byte + sections...
// Flags indicate which optional sections are present
const (
	gsHasName         = 0x01
	gsHasMetadata     = 0x02
	gsHasNodes        = 0x04
	gsHasEdges        = 0x08
	gsHasEdgeIndex    = 0x10
	gsHasAdjLists     = 0x20
	gsHasNodeFeatures = 0x40
	gsHasEdgeFeatures = 0x80
	// Second flags byte for labels
	gsHasNodeLabels = 0x01
	gsHasEdgeLabels = 0x02
)

func appendGraphShard(buf []byte, gs GraphShard) ([]byte, error) {
	buf = append(buf, tagGraphShard)

	// Calculate flags
	flags1 := byte(0)
	flags2 := byte(0)

	if gs.Name != "" {
		flags1 |= gsHasName
	}
	if len(gs.Metadata) > 0 {
		flags1 |= gsHasMetadata
	}
	if len(gs.Nodes) > 0 {
		flags1 |= gsHasNodes
	}
	if len(gs.Edges) > 0 {
		flags1 |= gsHasEdges
	}
	if len(gs.EdgeIndex) == 2 && len(gs.EdgeIndex[0]) > 0 {
		flags1 |= gsHasEdgeIndex
	}
	if len(gs.AdjLists) > 0 {
		flags1 |= gsHasAdjLists
	}
	if len(gs.NodeFeatures) > 0 {
		flags1 |= gsHasNodeFeatures
	}
	if len(gs.EdgeFeatures) > 0 {
		flags1 |= gsHasEdgeFeatures
	}
	if len(gs.NodeLabels) > 0 {
		flags2 |= gsHasNodeLabels
	}
	if len(gs.EdgeLabels) > 0 {
		flags2 |= gsHasEdgeLabels
	}

	buf = append(buf, flags1, flags2)

	var err error

	// Name
	if flags1&gsHasName != 0 {
		nameBytes := []byte(gs.Name)
		buf = appendUvarint(buf, uint64(len(nameBytes)))
		buf = append(buf, nameBytes...)
	}

	// Metadata (as object)
	if flags1&gsHasMetadata != 0 {
		buf, err = appendObject(buf, gs.Metadata)
		if err != nil {
			return nil, err
		}
	}

	// Nodes (as NodeBatch inline)
	if flags1&gsHasNodes != 0 {
		buf = appendUvarint(buf, uint64(len(gs.Nodes)))
		for _, n := range gs.Nodes {
			buf, err = appendNodeInline(buf, n)
			if err != nil {
				return nil, err
			}
		}
	}

	// Edges (as inline edge list)
	if flags1&gsHasEdges != 0 {
		buf = appendUvarint(buf, uint64(len(gs.Edges)))
		for _, e := range gs.Edges {
			buf, err = appendEdgeInline(buf, e)
			if err != nil {
				return nil, err
			}
		}
	}

	// EdgeIndex (COO format: two int64 arrays)
	if flags1&gsHasEdgeIndex != 0 {
		buf = appendUvarint(buf, uint64(len(gs.EdgeIndex[0])))
		for _, src := range gs.EdgeIndex[0] {
			buf = appendVarint(buf, src)
		}
		for _, tgt := range gs.EdgeIndex[1] {
			buf = appendVarint(buf, tgt)
		}
	}

	// AdjLists
	if flags1&gsHasAdjLists != 0 {
		buf = appendUvarint(buf, uint64(len(gs.AdjLists)))
		for _, adj := range gs.AdjLists {
			buf = appendVarint(buf, adj.NodeID)
			buf = appendUvarint(buf, uint64(len(adj.Neighbors)))
			for _, n := range adj.Neighbors {
				buf = appendVarint(buf, n)
			}
		}
	}

	// NodeFeatures (matrix as float32)
	if flags1&gsHasNodeFeatures != 0 {
		buf = appendUvarint(buf, uint64(len(gs.NodeFeatures)))
		if len(gs.NodeFeatures) > 0 {
			buf = appendUvarint(buf, uint64(len(gs.NodeFeatures[0])))
		} else {
			buf = appendUvarint(buf, 0)
		}
		for _, row := range gs.NodeFeatures {
			for _, f := range row {
				buf = appendFloat32(buf, float32(f))
			}
		}
	}

	// EdgeFeatures (matrix as float32)
	if flags1&gsHasEdgeFeatures != 0 {
		buf = appendUvarint(buf, uint64(len(gs.EdgeFeatures)))
		if len(gs.EdgeFeatures) > 0 {
			buf = appendUvarint(buf, uint64(len(gs.EdgeFeatures[0])))
		} else {
			buf = appendUvarint(buf, 0)
		}
		for _, row := range gs.EdgeFeatures {
			for _, f := range row {
				buf = appendFloat32(buf, float32(f))
			}
		}
	}

	// NodeLabels
	if flags2&gsHasNodeLabels != 0 {
		buf = appendUvarint(buf, uint64(len(gs.NodeLabels)))
		for _, l := range gs.NodeLabels {
			buf = appendVarint(buf, l)
		}
	}

	// EdgeLabels
	if flags2&gsHasEdgeLabels != 0 {
		buf = appendUvarint(buf, uint64(len(gs.EdgeLabels)))
		for _, l := range gs.EdgeLabels {
			buf = appendVarint(buf, l)
		}
	}

	return buf, nil
}

// appendEdgeInline encodes an edge without the tag (for GraphShard)
func appendEdgeInline(buf []byte, e Edge) ([]byte, error) {
	// ID
	id := []byte(e.ID)
	buf = appendUvarint(buf, uint64(len(id)))
	buf = append(buf, id...)

	// Type
	et := []byte(e.Type)
	buf = appendUvarint(buf, uint64(len(et)))
	buf = append(buf, et...)

	// From
	from := []byte(e.From)
	buf = appendUvarint(buf, uint64(len(from)))
	buf = append(buf, from...)

	// To
	to := []byte(e.To)
	buf = appendUvarint(buf, uint64(len(to)))
	buf = append(buf, to...)

	// Props
	props := e.Props
	if props == nil {
		props = map[string]any{}
	}
	return appendObject(buf, props)
}

// ---------- Decoding ----------

func readUvarint(data []byte, off int) (uint64, int, error) {
	if off >= len(data) {
		return 0, 0, ErrUnexpectedEOF
	}
	v, n := binary.Uvarint(data[off:])
	if n <= 0 {
		return 0, 0, ErrInvalidUvarint
	}
	return v, n, nil
}

func readVarint(data []byte, off int) (int64, int, error) {
	if off >= len(data) {
		return 0, 0, ErrUnexpectedEOF
	}
	v, n := binary.Varint(data[off:])
	if n <= 0 {
		return 0, 0, ErrInvalidVarint
	}
	return v, n, nil
}

func readFloat64(data []byte, off int) (float64, int, error) {
	if off+8 > len(data) {
		return 0, 0, ErrShortFloat64
	}
	bits := binary.LittleEndian.Uint64(data[off : off+8])
	return math.Float64frombits(bits), 8, nil
}

func readFloat32(data []byte, off int) (float32, int, error) {
	if off+4 > len(data) {
		return 0, 0, ErrShortFloat32
	}
	bits := binary.LittleEndian.Uint32(data[off : off+4])
	return math.Float32frombits(bits), 4, nil
}

func readValue(data []byte, off int, opts DecodeOptions) (any, int, error) {
	if off >= len(data) {
		return nil, 0, ErrUnexpectedEOF
	}
	tag := data[off]
	off++

	switch tag {
	case tagNull:
		return nil, off, nil

	case tagFalse:
		return false, off, nil

	case tagTrue:
		return true, off, nil

	case tagInt64:
		v, n, err := readVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		return v, off + n, nil

	case tagFloat64:
		f, n, err := readFloat64(data, off)
		if err != nil {
			return nil, 0, err
		}
		return f, off + n, nil

	case tagFloat32:
		f, n, err := readFloat32(data, off)
		if err != nil {
			return nil, 0, err
		}
		return float64(f), off + n, nil

	case tagString:
		length, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		// Security: validate string length
		if length > uint64(opts.MaxStringLen) {
			return nil, 0, ErrMaxStringLen
		}
		off += n
		end := off + int(length)
		if end > len(data) {
			return nil, 0, ErrShortString
		}
		return string(data[off:end]), end, nil

	case tagBytes:
		length, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		// Security: validate bytes length
		if length > uint64(opts.MaxBytesLen) {
			return nil, 0, ErrMaxBytesLen
		}
		off += n
		end := off + int(length)
		if end > len(data) {
			return nil, 0, ErrShortBytes
		}
		result := make([]byte, length)
		copy(result, data[off:end])
		return result, end, nil

	case tagObject:
		return readObject(data, off, opts)

	case tagArrayGeneric:
		return readArrayGeneric(data, off, opts)

	case tagArrayFloat64:
		return readArrayFloat64(data, off, opts)

	case tagArrayInt64:
		return readArrayInt64(data, off, opts)

	case tagArrayFloat32:
		return readArrayFloat32(data, off, opts)

	case tagArrayString:
		return readArrayString(data, off, opts)

	// Graph types
	case tagNode:
		return readNode(data, off, opts)

	case tagEdge:
		return readEdge(data, off, opts)

	case tagAdjList:
		return readAdjList(data, off, opts)

	case tagNodeBatch:
		return readNodeBatch(data, off, opts)

	case tagEdgeBatch:
		return readEdgeBatch(data, off, opts)

	case tagGraphShard:
		return readGraphShard(data, off, opts)

	default:
		return nil, 0, ErrUnknownTag
	}
}

func readObject(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation
	if count > uint64(opts.MaxObjectLen) {
		return nil, 0, ErrMaxObjectLen
	}

	m := make(map[string]any, int(count))
	for i := 0; i < int(count); i++ {
		// Read key
		klen, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		end := off + int(klen)
		if end > len(data) {
			return nil, 0, ErrShortKey
		}
		key := string(data[off:end])
		off = end

		// Read value
		val, newOff, err := readValue(data, off, opts)
		if err != nil {
			return nil, 0, err
		}
		off = newOff
		m[key] = val
	}
	return m, off, nil
}

func readArrayGeneric(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation
	if count > uint64(opts.MaxArrayLen) {
		return nil, 0, ErrMaxArrayLen
	}

	out := make([]any, int(count))
	for i := 0; i < int(count); i++ {
		v, newOff, err := readValue(data, off, opts)
		if err != nil {
			return nil, 0, err
		}
		off = newOff
		out[i] = v
	}
	return out, off, nil
}

func readArrayFloat64(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation and calculation
	if count > uint64(opts.MaxArrayLen) {
		return nil, 0, ErrMaxArrayLen
	}
	// Check for integer overflow in size calculation
	if count > uint64(^uint(0)>>3) { // max safe count for *8
		return nil, 0, ErrIntegerOverflow
	}

	// Bulk read optimization: check we have enough bytes upfront
	needed := int(count) * 8
	if off+needed > len(data) {
		return nil, 0, ErrShortFloat64Array
	}

	// Return []float64 for type safety and usability
	out := make([]float64, int(count))
	for i := 0; i < int(count); i++ {
		// Inline float64 read (avoids function call overhead)
		bits := binary.LittleEndian.Uint64(data[off:])
		off += 8
		out[i] = math.Float64frombits(bits)
	}
	return out, off, nil
}

func readArrayInt64(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation
	if count > uint64(opts.MaxArrayLen) {
		return nil, 0, ErrMaxArrayLen
	}
	// Check for integer overflow in size calculation
	if count > uint64(^uint(0)>>3) { // max safe count for *8
		return nil, 0, ErrIntegerOverflow
	}

	// SPEC: Int64Array uses fixed 8-byte LE for each element
	// Bulk read optimization: check we have enough bytes upfront
	needed := int(count) * 8
	if off+needed > len(data) {
		return nil, 0, ErrShortInt64Array
	}

	// Return []int64 for type safety and usability
	out := make([]int64, int(count))
	for i := 0; i < int(count); i++ {
		// Inline int64 read (avoids function call overhead)
		bits := binary.LittleEndian.Uint64(data[off:])
		off += 8
		out[i] = int64(bits)
	}
	return out, off, nil
}

func readArrayFloat32(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation and calculation
	if count > uint64(opts.MaxArrayLen) {
		return nil, 0, ErrMaxArrayLen
	}
	// Check for integer overflow in size calculation
	if count > uint64(^uint(0)>>2) { // max safe count for *4
		return nil, 0, ErrIntegerOverflow
	}

	// Bulk read optimization: check we have enough bytes upfront
	needed := int(count) * 4
	if off+needed > len(data) {
		return nil, 0, ErrShortFloat32Array
	}

	// Return []float64 for API consistency (float32 values promoted to float64)
	out := make([]float64, int(count))
	for i := 0; i < int(count); i++ {
		// Inline float32 read (avoids function call overhead)
		bits := binary.LittleEndian.Uint32(data[off:])
		off += 4
		out[i] = float64(math.Float32frombits(bits))
	}
	return out, off, nil
}

func readArrayString(data []byte, off int, opts DecodeOptions) (any, int, error) {
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Security: validate count before allocation
	if count > uint64(opts.MaxArrayLen) {
		return nil, 0, ErrMaxArrayLen
	}

	out := make([]string, int(count))
	for i := 0; i < int(count); i++ {
		sLen, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		end := off + int(sLen)
		if end > len(data) {
			return nil, 0, ErrShortStringArray
		}
		out[i] = string(data[off:end])
		off = end
	}
	return out, off, nil
}

// ---------- Graph Decoding ----------

// readNode decodes a Node
func readNode(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// ID
	idLen, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n
	if off+int(idLen) > len(data) {
		return nil, 0, ErrShortData
	}
	id := string(data[off : off+int(idLen)])
	off += int(idLen)

	// Labels
	labelCount, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	labels := make([]string, labelCount)
	for i := uint64(0); i < labelCount; i++ {
		lLen, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		if off+int(lLen) > len(data) {
			return nil, 0, ErrShortData
		}
		labels[i] = string(data[off : off+int(lLen)])
		off += int(lLen)
	}

	// Props (encoded as object - skip the tagObject byte first)
	if off >= len(data) || data[off] != tagObject {
		return nil, 0, ErrExpectedObjectTag
	}
	off++ // skip tagObject
	propsVal, off, err := readObject(data, off, opts)
	if err != nil {
		return nil, 0, err
	}
	props := propsVal.(map[string]any)

	return Node{ID: id, Labels: labels, Props: props}, off, nil
}

// readNodeInline decodes a node without the tag (for batches)
func readNodeInline(data []byte, off int, opts DecodeOptions) (Node, int, error) {
	// ID
	idLen, n, err := readUvarint(data, off)
	if err != nil {
		return Node{}, 0, err
	}
	off += n
	if off+int(idLen) > len(data) {
		return Node{}, 0, ErrShortData
	}
	id := string(data[off : off+int(idLen)])
	off += int(idLen)

	// Labels
	labelCount, n, err := readUvarint(data, off)
	if err != nil {
		return Node{}, 0, err
	}
	off += n

	labels := make([]string, labelCount)
	for i := uint64(0); i < labelCount; i++ {
		lLen, n, err := readUvarint(data, off)
		if err != nil {
			return Node{}, 0, err
		}
		off += n
		if off+int(lLen) > len(data) {
			return Node{}, 0, ErrShortData
		}
		labels[i] = string(data[off : off+int(lLen)])
		off += int(lLen)
	}

	// Props (encoded as object - skip the tagObject byte first)
	if off >= len(data) || data[off] != tagObject {
		return Node{}, 0, ErrExpectedObjectTag
	}
	off++ // skip tagObject
	propsVal, off, err := readObject(data, off, opts)
	if err != nil {
		return Node{}, 0, err
	}
	props := propsVal.(map[string]any)

	return Node{ID: id, Labels: labels, Props: props}, off, nil
}

// readEdge decodes an Edge
func readEdge(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// ID
	idLen, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n
	if off+int(idLen) > len(data) {
		return nil, 0, ErrShortData
	}
	id := string(data[off : off+int(idLen)])
	off += int(idLen)

	// Type
	typeLen, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n
	if off+int(typeLen) > len(data) {
		return nil, 0, ErrShortData
	}
	edgeType := string(data[off : off+int(typeLen)])
	off += int(typeLen)

	// From
	fromLen, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n
	if off+int(fromLen) > len(data) {
		return nil, 0, ErrShortData
	}
	from := string(data[off : off+int(fromLen)])
	off += int(fromLen)

	// To
	toLen, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n
	if off+int(toLen) > len(data) {
		return nil, 0, ErrShortData
	}
	to := string(data[off : off+int(toLen)])
	off += int(toLen)

	// Props (encoded as object - skip the tagObject byte first)
	if off >= len(data) || data[off] != tagObject {
		return nil, 0, ErrExpectedObjectTag
	}
	off++ // skip tagObject
	propsVal, off, err := readObject(data, off, opts)
	if err != nil {
		return nil, 0, err
	}
	props := propsVal.(map[string]any)

	return Edge{ID: id, Type: edgeType, From: from, To: to, Props: props}, off, nil
}

// readAdjList decodes an AdjList
func readAdjList(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// NodeID
	nodeID, n, err := readVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Neighbor count
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Neighbors
	neighbors := make([]int64, count)
	for i := uint64(0); i < count; i++ {
		v, n, err := readVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		neighbors[i] = v
	}

	return AdjList{NodeID: nodeID, Neighbors: neighbors}, off, nil
}

// readNodeBatch decodes a NodeBatch
func readNodeBatch(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// Node count
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Nodes (inline, without tags)
	nodes := make([]Node, count)
	for i := uint64(0); i < count; i++ {
		node, newOff, err := readNodeInline(data, off, opts)
		if err != nil {
			return nil, 0, err
		}
		off = newOff
		nodes[i] = node
	}

	return NodeBatch{Nodes: nodes}, off, nil
}

// readEdgeBatch decodes an EdgeBatch
func readEdgeBatch(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// Edge count
	edgeCount, n, err := readUvarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	// Flags
	if off+2 > len(data) {
		return nil, 0, ErrShortData
	}
	hasTypes := data[off] == 1
	hasProps := data[off+1] == 1
	off += 2

	// Sources
	sources := make([]int64, edgeCount)
	for i := uint64(0); i < edgeCount; i++ {
		v, n, err := readVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		sources[i] = v
	}

	// Targets
	targets := make([]int64, edgeCount)
	for i := uint64(0); i < edgeCount; i++ {
		v, n, err := readVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		targets[i] = v
	}

	// Types (optional)
	var types []string
	if hasTypes {
		types = make([]string, edgeCount)
		for i := uint64(0); i < edgeCount; i++ {
			tLen, n, err := readUvarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n
			if off+int(tLen) > len(data) {
				return nil, 0, ErrShortData
			}
			types[i] = string(data[off : off+int(tLen)])
			off += int(tLen)
		}
	}

	// Props (optional - each object is prefixed with tagObject)
	var props []map[string]any
	if hasProps {
		props = make([]map[string]any, edgeCount)
		for i := uint64(0); i < edgeCount; i++ {
			if off >= len(data) || data[off] != tagObject {
				return nil, 0, ErrExpectedObjectTag
			}
			off++ // skip tagObject
			propsVal, newOff, err := readObject(data, off, opts)
			if err != nil {
				return nil, 0, err
			}
			off = newOff
			props[i] = propsVal.(map[string]any)
		}
	}

	return EdgeBatch{Sources: sources, Targets: targets, Types: types, Props: props}, off, nil
}

// readGraphShard decodes a GraphShard container
func readGraphShard(data []byte, off int, opts DecodeOptions) (any, int, error) {
	// Read flags
	if off+2 > len(data) {
		return nil, 0, ErrShortData
	}
	flags1 := data[off]
	flags2 := data[off+1]
	off += 2

	gs := GraphShard{}
	var err error

	// Name
	if flags1&gsHasName != 0 {
		nameLen, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		if off+int(nameLen) > len(data) {
			return nil, 0, ErrShortData
		}
		gs.Name = string(data[off : off+int(nameLen)])
		off += int(nameLen)
	}

	// Metadata (as object)
	if flags1&gsHasMetadata != 0 {
		if off >= len(data) || data[off] != tagObject {
			return nil, 0, ErrExpectedObjectTag
		}
		off++ // skip tagObject
		metaVal, newOff, err := readObject(data, off, opts)
		if err != nil {
			return nil, 0, err
		}
		off = newOff
		gs.Metadata = metaVal.(map[string]any)
	}

	// Nodes
	if flags1&gsHasNodes != 0 {
		nodeCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		gs.Nodes = make([]Node, nodeCount)
		for i := uint64(0); i < nodeCount; i++ {
			node, newOff, err := readNodeInline(data, off, opts)
			if err != nil {
				return nil, 0, err
			}
			off = newOff
			gs.Nodes[i] = node
		}
	}

	// Edges
	if flags1&gsHasEdges != 0 {
		edgeCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n
		gs.Edges = make([]Edge, edgeCount)
		for i := uint64(0); i < edgeCount; i++ {
			edge, newOff, err := readEdgeInline(data, off, opts)
			if err != nil {
				return nil, 0, err
			}
			off = newOff
			gs.Edges[i] = edge
		}
	}

	// EdgeIndex (COO format)
	if flags1&gsHasEdgeIndex != 0 {
		edgeCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		sources := make([]int64, edgeCount)
		targets := make([]int64, edgeCount)

		for i := uint64(0); i < edgeCount; i++ {
			v, n, err := readVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n
			sources[i] = v
		}
		for i := uint64(0); i < edgeCount; i++ {
			v, n, err := readVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n
			targets[i] = v
		}
		gs.EdgeIndex = [][]int64{sources, targets}
	}

	// AdjLists
	if flags1&gsHasAdjLists != 0 {
		adjCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		gs.AdjLists = make([]AdjList, adjCount)
		for i := uint64(0); i < adjCount; i++ {
			nodeID, n, err := readVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n

			neighborCount, n, err := readUvarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n

			neighbors := make([]int64, neighborCount)
			for j := uint64(0); j < neighborCount; j++ {
				v, n, err := readVarint(data, off)
				if err != nil {
					return nil, 0, err
				}
				off += n
				neighbors[j] = v
			}
			gs.AdjLists[i] = AdjList{NodeID: nodeID, Neighbors: neighbors}
		}
	}

	// NodeFeatures
	if flags1&gsHasNodeFeatures != 0 {
		numNodes, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		featureDim, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		gs.NodeFeatures = make([][]float64, numNodes)
		for i := uint64(0); i < numNodes; i++ {
			gs.NodeFeatures[i] = make([]float64, featureDim)
			for j := uint64(0); j < featureDim; j++ {
				f, n, err := readFloat32(data, off)
				if err != nil {
					return nil, 0, err
				}
				off += n
				gs.NodeFeatures[i][j] = float64(f)
			}
		}
	}

	// EdgeFeatures
	if flags1&gsHasEdgeFeatures != 0 {
		numEdges, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		featureDim, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		gs.EdgeFeatures = make([][]float64, numEdges)
		for i := uint64(0); i < numEdges; i++ {
			gs.EdgeFeatures[i] = make([]float64, featureDim)
			for j := uint64(0); j < featureDim; j++ {
				f, n, err := readFloat32(data, off)
				if err != nil {
					return nil, 0, err
				}
				off += n
				gs.EdgeFeatures[i][j] = float64(f)
			}
		}
	}

	// NodeLabels
	if flags2&gsHasNodeLabels != 0 {
		labelCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		gs.NodeLabels = make([]int64, labelCount)
		for i := uint64(0); i < labelCount; i++ {
			v, n, err := readVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n
			gs.NodeLabels[i] = v
		}
	}

	// EdgeLabels
	if flags2&gsHasEdgeLabels != 0 {
		labelCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		off += n

		gs.EdgeLabels = make([]int64, labelCount)
		for i := uint64(0); i < labelCount; i++ {
			v, n, err := readVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
			off += n
			gs.EdgeLabels[i] = v
		}
	}

	// Suppress unused variable warning
	_ = err

	return gs, off, nil
}

// readEdgeInline decodes an edge without the tag (for GraphShard)
func readEdgeInline(data []byte, off int, opts DecodeOptions) (Edge, int, error) {
	// ID
	idLen, n, err := readUvarint(data, off)
	if err != nil {
		return Edge{}, 0, err
	}
	off += n
	if off+int(idLen) > len(data) {
		return Edge{}, 0, ErrShortData
	}
	id := string(data[off : off+int(idLen)])
	off += int(idLen)

	// Type
	typeLen, n, err := readUvarint(data, off)
	if err != nil {
		return Edge{}, 0, err
	}
	off += n
	if off+int(typeLen) > len(data) {
		return Edge{}, 0, ErrShortData
	}
	edgeType := string(data[off : off+int(typeLen)])
	off += int(typeLen)

	// From
	fromLen, n, err := readUvarint(data, off)
	if err != nil {
		return Edge{}, 0, err
	}
	off += n
	if off+int(fromLen) > len(data) {
		return Edge{}, 0, ErrShortData
	}
	from := string(data[off : off+int(fromLen)])
	off += int(fromLen)

	// To
	toLen, n, err := readUvarint(data, off)
	if err != nil {
		return Edge{}, 0, err
	}
	off += n
	if off+int(toLen) > len(data) {
		return Edge{}, 0, ErrShortData
	}
	to := string(data[off : off+int(toLen)])
	off += int(toLen)

	// Props (encoded as object - skip the tagObject byte first)
	if off >= len(data) || data[off] != tagObject {
		return Edge{}, 0, ErrExpectedObjectTag
	}
	off++ // skip tagObject
	propsVal, off, err := readObject(data, off, opts)
	if err != nil {
		return Edge{}, 0, err
	}
	props := propsVal.(map[string]any)

	return Edge{ID: id, Type: edgeType, From: from, To: to, Props: props}, off, nil
}
