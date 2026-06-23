package cowrie

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// nativeLittleEndian is true when the host CPU stores multi-byte integers
// in little-endian order (the overwhelmingly common case: x86, ARM LE, etc.).
// It is computed once at package init via a uint16 byte-order probe.
var nativeLittleEndian = func() bool {
	var v uint16 = 0x0102
	b := (*[2]byte)(unsafe.Pointer(&v))
	return b[0] == 0x02
}()

// viewFloat32 returns a zero-copy view of byte slice as []float32.
// On little-endian hosts the wire bytes are reinterpreted in place (fast path).
// On big-endian hosts the function falls back to a byte-swapped copy so that
// the caller always sees correctly decoded values.
// Returns (nil, false) if size is not a multiple of 4.
// Returns (nil, true) for empty input.
func viewFloat32(data []byte) ([]float32, bool) {
	if len(data) == 0 {
		return nil, true
	}
	if len(data)%4 != 0 {
		return nil, false
	}
	count := len(data) / 4

	if !nativeLittleEndian {
		// Big-endian fallback: decode each element via binary.LittleEndian.
		out := make([]float32, count)
		for i := 0; i < count; i++ {
			bits := binary.LittleEndian.Uint32(data[i*4:])
			out[i] = math.Float32frombits(bits)
		}
		return out, true
	}

	// Little-endian fast path: check alignment before reinterpreting.
	if uintptr(unsafe.Pointer(&data[0]))%4 != 0 {
		return nil, false
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&data[0])), count), true
}

// viewFloat64 returns a zero-copy view of byte slice as []float64.
// On little-endian hosts the wire bytes are reinterpreted in place (fast path).
// On big-endian hosts the function falls back to a byte-swapped copy.
// Returns (nil, false) if size is not a multiple of 8.
// Returns (nil, true) for empty input.
func viewFloat64(data []byte) ([]float64, bool) {
	if len(data) == 0 {
		return nil, true
	}
	if len(data)%8 != 0 {
		return nil, false
	}
	count := len(data) / 8

	if !nativeLittleEndian {
		out := make([]float64, count)
		for i := 0; i < count; i++ {
			bits := binary.LittleEndian.Uint64(data[i*8:])
			out[i] = math.Float64frombits(bits)
		}
		return out, true
	}

	if uintptr(unsafe.Pointer(&data[0]))%8 != 0 {
		return nil, false
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(&data[0])), count), true
}

// viewInt32 returns a zero-copy view of byte slice as []int32.
// On little-endian hosts the wire bytes are reinterpreted in place (fast path).
// On big-endian hosts the function falls back to a byte-swapped copy.
// Returns (nil, false) if size is not a multiple of 4.
// Returns (nil, true) for empty input.
func viewInt32(data []byte) ([]int32, bool) {
	if len(data) == 0 {
		return nil, true
	}
	if len(data)%4 != 0 {
		return nil, false
	}
	count := len(data) / 4

	if !nativeLittleEndian {
		out := make([]int32, count)
		for i := 0; i < count; i++ {
			out[i] = int32(binary.LittleEndian.Uint32(data[i*4:]))
		}
		return out, true
	}

	if uintptr(unsafe.Pointer(&data[0]))%4 != 0 {
		return nil, false
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&data[0])), count), true
}

// viewInt64 returns a zero-copy view of byte slice as []int64.
// On little-endian hosts the wire bytes are reinterpreted in place (fast path).
// On big-endian hosts the function falls back to a byte-swapped copy.
// Returns (nil, false) if size is not a multiple of 8.
// Returns (nil, true) for empty input.
func viewInt64(data []byte) ([]int64, bool) {
	if len(data) == 0 {
		return nil, true
	}
	if len(data)%8 != 0 {
		return nil, false
	}
	count := len(data) / 8

	if !nativeLittleEndian {
		out := make([]int64, count)
		for i := 0; i < count; i++ {
			out[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
		}
		return out, true
	}

	if uintptr(unsafe.Pointer(&data[0]))%8 != 0 {
		return nil, false
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&data[0])), count), true
}

// ViewBytesFloat32 returns a zero-copy view of a raw byte slice as []float32,
// reusing the alignment-gated, endianness-aware viewFloat32 helper. It exists so
// that the codec package (columnar reader) can reinterpret column chunk bytes in
// place without duplicating the unsafe.Slice + big-endian fallback logic. Returns
// (nil, false) if the slice cannot be viewed (wrong length multiple, misaligned
// base pointer on a LE host). On a BE host it returns a byte-swapped copy with ok=true.
func ViewBytesFloat32(b []byte) ([]float32, bool) { return viewFloat32(b) }

// ViewBytesFloat64 returns a zero-copy view of raw bytes as []float64.
func ViewBytesFloat64(b []byte) ([]float64, bool) { return viewFloat64(b) }

// ViewBytesInt32 returns a zero-copy view of raw bytes as []int32.
func ViewBytesInt32(b []byte) ([]int32, bool) { return viewInt32(b) }

// ViewBytesInt64 returns a zero-copy view of raw bytes as []int64.
func ViewBytesInt64(b []byte) ([]int64, bool) { return viewInt64(b) }

// ViewBytesUint8 returns the byte slice itself as []uint8 (identity passthrough).
// It always succeeds (uint8 has elem size 1 and no alignment constraint), and the
// returned slice aliases the input bytes — true zero-copy.
func ViewBytesUint8(b []byte) ([]uint8, bool) { return b, true }

// DTypeElemSize returns the in-memory element size in bytes for a fixed-width
// DType, and whether the dtype is fixed-width. Sub-byte packed dtypes return
// (0, false). It is the exported form of the internal dtypeElemSize helper so the
// columnar codec can validate values length == rows*prod(shape_tail)*elemsize.
func DTypeElemSize(d DType) (uint64, bool) { return dtypeElemSize(d) }

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
