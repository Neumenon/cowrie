// Package gen1 implements GEN-1 SJSON: a compact binary JSON format
// with proto-tensor support for numeric arrays.
//
// GEN-1 is the "lightweight" codec that provides significant size savings
// without the full complexity of SJSON v2. It's perfect for:
//   - JSON APIs that want binary efficiency
//   - ML data with float arrays (embeddings, features)
//   - Structured logging with numeric payloads
//   - Graph data with adjacency lists
//
// Features:
//   - 11 core types: null, bool, int64, float64, string, bytes, object, arrays
//   - Proto-tensor support: homogeneous numeric arrays automatically use efficient binary encoding
//   - 6 graph types: Node, Edge, AdjList, NodeBatch, EdgeBatch, GraphShard
//   - Deterministic encoding (sorted object keys)
//   - Streaming support via StreamDecoder
//   - Zero external dependencies (stdlib only)
//
// Example usage:
//
//	// Encode
//	data, err := gen1.Encode(map[string]any{
//	    "embedding": []float64{0.1, 0.2, 0.3, ...},
//	    "id": "doc-123",
//	})
//
//	// Decode
//	result, err := gen1.Decode(data)
//
//	// JSON round-trip
//	sjsonBytes, _ := gen1.EncodeJSON(jsonBytes)
//	jsonBytes, _ := gen1.DecodeJSON(sjsonBytes)
package gen1
