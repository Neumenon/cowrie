package cowrie

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// viewFloat32 returns a zero-copy view of byte slice as []float32.
// Returns (nil, false) if alignment or size is invalid.
func viewFloat32(data []byte) ([]float32, bool) {
	if len(data) == 0 {
		return nil, true
	}
	// Check size divisibility
	if len(data)%4 != 0 {
		return nil, false
	}
	// Check alignment
	if uintptr(unsafe.Pointer(&data[0]))%4 != 0 {
		return nil, false
	}
	count := len(data) / 4
	return unsafe.Slice((*float32)(unsafe.Pointer(&data[0])), count), true
}

// viewFloat64 returns a zero-copy view of byte slice as []float64.
// Returns (nil, false) if alignment or size is invalid.
func viewFloat64(data []byte) ([]float64, bool) {
	if len(data) == 0 {
		return nil, true
	}
	// Check size divisibility
	if len(data)%8 != 0 {
		return nil, false
	}
	// Check alignment
	if uintptr(unsafe.Pointer(&data[0]))%8 != 0 {
		return nil, false
	}
	count := len(data) / 8
	return unsafe.Slice((*float64)(unsafe.Pointer(&data[0])), count), true
}

// viewInt32 returns a zero-copy view of byte slice as []int32.
// Returns (nil, false) if alignment or size is invalid.
func viewInt32(data []byte) ([]int32, bool) {
	if len(data) == 0 {
		return nil, true
	}
	// Check size divisibility
	if len(data)%4 != 0 {
		return nil, false
	}
	// Check alignment
	if uintptr(unsafe.Pointer(&data[0]))%4 != 0 {
		return nil, false
	}
	count := len(data) / 4
	return unsafe.Slice((*int32)(unsafe.Pointer(&data[0])), count), true
}

// viewInt64 returns a zero-copy view of byte slice as []int64.
// Returns (nil, false) if alignment or size is invalid.
func viewInt64(data []byte) ([]int64, bool) {
	if len(data) == 0 {
		return nil, true
	}
	// Check size divisibility
	if len(data)%8 != 0 {
		return nil, false
	}
	// Check alignment
	if uintptr(unsafe.Pointer(&data[0]))%8 != 0 {
		return nil, false
	}
	count := len(data) / 8
	return unsafe.Slice((*int64)(unsafe.Pointer(&data[0])), count), true
}

// CopyFloat32 decodes tensor data as []float32 with a copy.
// This is the safe fallback when zero-copy view fails.
func CopyFloat32(td *TensorData) []float32 {
	if td.DType != DTypeFloat32 || len(td.Data) == 0 {
		return nil
	}
	return decodeTensorFloat32Copy(td.Data)
}

// CopyFloat64 decodes tensor data as []float64 with a copy.
func CopyFloat64(td *TensorData) []float64 {
	if td.DType != DTypeFloat64 || len(td.Data) == 0 {
		return nil
	}
	return decodeTensorFloat64Copy(td.Data)
}

// CopyInt32 decodes tensor data as []int32 with a copy.
func CopyInt32(td *TensorData) []int32 {
	if td.DType != DTypeInt32 || len(td.Data) == 0 {
		return nil
	}
	return decodeTensorInt32Copy(td.Data)
}

// CopyInt64 decodes tensor data as []int64 with a copy.
func CopyInt64(td *TensorData) []int64 {
	if td.DType != DTypeInt64 || len(td.Data) == 0 {
		return nil
	}
	return decodeTensorInt64Copy(td.Data)
}

// Float32Slice returns tensor data as []float32, preferring zero-copy.
// Falls back to copy if zero-copy view is not possible.
func (td *TensorData) Float32Slice() []float32 {
	if view, ok := td.ViewFloat32(); ok {
		return view
	}
	return CopyFloat32(td)
}

// Float64Slice returns tensor data as []float64, preferring zero-copy.
// Falls back to copy if zero-copy view is not possible.
func (td *TensorData) Float64Slice() []float64 {
	if view, ok := td.ViewFloat64(); ok {
		return view
	}
	return CopyFloat64(td)
}

// Int32Slice returns tensor data as []int32, preferring zero-copy.
// Falls back to copy if zero-copy view is not possible.
func (td *TensorData) Int32Slice() []int32 {
	if view, ok := td.ViewInt32(); ok {
		return view
	}
	return CopyInt32(td)
}

// Int64Slice returns tensor data as []int64, preferring zero-copy.
// Falls back to copy if zero-copy view is not possible.
func (td *TensorData) Int64Slice() []int64 {
	if view, ok := td.ViewInt64(); ok {
		return view
	}
	return CopyInt64(td)
}

// decodeTensorFloat32Copy decodes float32 data with a copy.
func decodeTensorFloat32Copy(data []byte) []float32 {
	count := len(data) / 4
	out := make([]float32, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		out[i] = math.Float32frombits(bits)
	}
	return out
}

// decodeTensorFloat64Copy decodes float64 data with a copy.
func decodeTensorFloat64Copy(data []byte) []float64 {
	count := len(data) / 8
	out := make([]float64, count)
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint64(data[i*8:])
		out[i] = math.Float64frombits(bits)
	}
	return out
}

// decodeTensorInt32Copy decodes int32 data with a copy.
func decodeTensorInt32Copy(data []byte) []int32 {
	count := len(data) / 4
	out := make([]int32, count)
	for i := 0; i < count; i++ {
		out[i] = int32(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return out
}

// decodeTensorInt64Copy decodes int64 data with a copy.
func decodeTensorInt64Copy(data []byte) []int64 {
	count := len(data) / 8
	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return out
}
