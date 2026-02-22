// Package gen2 implements SJSON v2, a binary "JSON++" codec with extended types.
//
// SJSON v2 extends JSON with:
//   - Explicit integer types (int64, uint64)
//   - Decimal128 for high-precision decimals
//   - Native binary data (no base64)
//   - Datetime64 (nanosecond timestamps)
//   - UUID128 (native UUIDs)
//   - BigInt (arbitrary precision)
//   - Dictionary-coded object keys
//
// ML/Multimodal Extensions (v2.1):
//   - Tensor: N-dimensional arrays with dtype and shape
//   - TensorRef: References to stored tensors
//   - Image: Encoded image data with format metadata
//   - Audio: Audio samples with encoding metadata
//   - Adjlist: CSR adjacency lists for graphs
//   - RichText: Text with tokens and spans
//   - Delta: Semantic diff/patch operations
//
// Features:
//   - Dictionary-coded object keys for compact encoding
//   - Optional gzip/zstd compression
//   - FNV-1a schema fingerprinting
//   - Deterministic encoding mode
//   - Zero-copy tensor views
//   - Forward-compatible extension mechanism
//
// Example usage:
//
//	// Create values
//	v := gen2.Object(
//	    gen2.Member{Key: "name", Value: gen2.String("Alice")},
//	    gen2.Member{Key: "embedding", Value: gen2.Tensor(gen2.DTypeFloat32, []uint64{384}, data)},
//	)
//
//	// Encode
//	data, err := gen2.Encode(v)
//
//	// Decode
//	result, err := gen2.Decode(data)
//
//	// JSON bridge
//	jsonVal, err := gen2.FromJSON(jsonBytes)
//	jsonBytes, err := gen2.ToJSON(sjsonVal)
package gen2
