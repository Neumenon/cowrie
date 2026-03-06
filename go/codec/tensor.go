package codec

import (
	"encoding/binary"
	"math"

	"github.com/Neumenon/cowrie/go"
)

// EncodeFloat32Tensor converts a float32 slice to an Cowrie Tensor value.
// This is ~48% smaller than JSON's text representation of floats.
// JSON: "scores": [0.123456, 0.789012, ...] = 10-20 bytes per float
// Cowrie Tensor: 4 bytes per float (native binary)
func EncodeFloat32Tensor(values []float32) *cowrie.Value {
	if len(values) == 0 {
		return cowrie.Array() // Empty array for empty input
	}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}
	return cowrie.Tensor(cowrie.DTypeFloat32, []uint64{uint64(len(values))}, data)
}

// DecodeFloat32Tensor converts an Cowrie Tensor value back to a float32 slice.
// Returns nil if the value is not a tensor or has wrong dtype.
func DecodeFloat32Tensor(v *cowrie.Value) []float32 {
	if v == nil || v.Type() != cowrie.TypeTensor {
		return nil
	}
	td := v.Tensor()
	if td.DType != cowrie.DTypeFloat32 {
		return nil
	}
	// Verify we have the right amount of data
	if len(td.Data) < 4 {
		return nil
	}
	count := len(td.Data) / 4
	result := make([]float32, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(td.Data[i*4:])
		result[i] = math.Float32frombits(bits)
	}
	return result
}

// IsFloat32Array checks if a Go value is a []float32 that would benefit from tensor encoding.
func IsFloat32Array(v any) ([]float32, bool) {
	if arr, ok := v.([]float32); ok {
		return arr, true
	}
	return nil, false
}

// EncodeFloat64Tensor converts a float64 slice to an Cowrie Tensor value.
// This is ~55% smaller than JSON's text representation of floats.
// JSON: "scores": [0.123456789012, ...] = 15-25 bytes per float
// Cowrie Tensor: 8 bytes per float (native binary float64)
func EncodeFloat64Tensor(values []float64) *cowrie.Value {
	if len(values) == 0 {
		return cowrie.Array() // Empty array for empty input
	}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}
	return cowrie.Tensor(cowrie.DTypeFloat64, []uint64{uint64(len(values))}, data)
}

// EncodeFloat64AsFloat32Tensor converts float64 slice to float32 tensor for 60% savings.
// Use when precision loss is acceptable (e.g., ML features, embeddings).
func EncodeFloat64AsFloat32Tensor(values []float64) *cowrie.Value {
	if len(values) == 0 {
		return cowrie.Array()
	}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(float32(v)))
	}
	return cowrie.Tensor(cowrie.DTypeFloat32, []uint64{uint64(len(values))}, data)
}

// TryEncodeNumericSlice attempts to encode []interface{} as a tensor if all elements are numeric.
// Returns nil if the slice is not purely numeric.
// Uses default precision (converts to float32).
func TryEncodeNumericSlice(arr []any) *cowrie.Value {
	return TryEncodeNumericSliceWithOpts(arr, false)
}

// TryEncodeNumericSliceWithOpts attempts to encode []interface{} as a tensor with precision control.
// If highPrecision is true, uses float64 encoding to preserve precision.
func TryEncodeNumericSliceWithOpts(arr []any, highPrecision bool) *cowrie.Value {
	if len(arr) == 0 {
		return nil
	}

	// Check if all elements are float64 (Go's default JSON number type)
	floats := make([]float64, len(arr))
	for i, v := range arr {
		switch x := v.(type) {
		case float64:
			floats[i] = x
		case float32:
			floats[i] = float64(x)
		case int:
			floats[i] = float64(x)
		case int64:
			floats[i] = float64(x)
		case int32:
			floats[i] = float64(x)
		default:
			return nil // Not a numeric slice
		}
	}

	// HighPrecision: preserve float64, otherwise use float32 for maximum compression
	if highPrecision {
		return EncodeFloat64Tensor(floats)
	}
	return EncodeFloat64AsFloat32Tensor(floats)
}

// DecodeFloat64Tensor converts an Cowrie Tensor value back to a float64 slice.
// Returns nil if the value is not a tensor or has wrong dtype.
func DecodeFloat64Tensor(v *cowrie.Value) []float64 {
	if v == nil || v.Type() != cowrie.TypeTensor {
		return nil
	}
	td := v.Tensor()
	if td.DType != cowrie.DTypeFloat64 {
		return nil
	}
	if len(td.Data) < 8 {
		return nil
	}
	count := len(td.Data) / 8
	result := make([]float64, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint64(td.Data[i*8:])
		result[i] = math.Float64frombits(bits)
	}
	return result
}

// DecodeTensorAuto decodes a tensor value respecting its dtype.
// Returns the appropriate Go slice type based on the tensor's dtype.
// For float32 tensors returns []float32, for float64 returns []float64, etc.
func DecodeTensorAuto(v *cowrie.Value) any {
	if v == nil || v.Type() != cowrie.TypeTensor {
		return nil
	}
	td := v.Tensor()

	switch td.DType {
	case cowrie.DTypeFloat32:
		return DecodeFloat32Tensor(v)
	case cowrie.DTypeFloat64:
		return DecodeFloat64Tensor(v)
	case cowrie.DTypeInt32:
		return decodeInt32Tensor(&td)
	case cowrie.DTypeInt64:
		return decodeInt64Tensor(&td)
	case cowrie.DTypeUint8:
		return td.Data // Already []byte
	default:
		// Return raw TensorData for unsupported types
		return &td
	}
}

// decodeInt32Tensor decodes int32 tensor data.
func decodeInt32Tensor(td *cowrie.TensorData) []int32 {
	if len(td.Data) < 4 {
		return nil
	}
	count := len(td.Data) / 4
	result := make([]int32, count)
	for i := 0; i < count; i++ {
		result[i] = int32(binary.LittleEndian.Uint32(td.Data[i*4:]))
	}
	return result
}

// decodeInt64Tensor decodes int64 tensor data.
func decodeInt64Tensor(td *cowrie.TensorData) []int64 {
	if len(td.Data) < 8 {
		return nil
	}
	count := len(td.Data) / 8
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = int64(binary.LittleEndian.Uint64(td.Data[i*8:]))
	}
	return result
}
