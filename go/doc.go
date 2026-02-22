// Package cowrie implements Cowrie v2, a binary "JSON++" codec with extended types.
//
// Cowrie v2 extends JSON with:
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
//	v := cowrie.Object(
//	    cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
//	    cowrie.Member{Key: "embedding", Value: cowrie.Tensor(cowrie.DTypeFloat32, []uint64{384}, data)},
//	)
//
//	// Encode
//	data, err := cowrie.Encode(v)
//
//	// Decode
//	result, err := cowrie.Decode(data)
//
//	// JSON bridge
//	jsonVal, err := cowrie.FromJSON(jsonBytes)
//	jsonBytes, err := cowrie.ToJSON(cowrieVal)
package cowrie
