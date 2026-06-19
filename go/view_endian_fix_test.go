package cowrie

import (
	"encoding/binary"
	"math"
	"testing"
)

// TestNativeLittleEndian verifies that the endianness probe matches the known
// value for this build host (x86-64, which is always little-endian).
func TestNativeLittleEndian(t *testing.T) {
	if !nativeLittleEndian {
		t.Fatal("nativeLittleEndian is false on a host that should be little-endian")
	}
}

// TestViewFloat32_RoundTrip encodes known float32 values as little-endian bytes
// and confirms that viewFloat32 returns the exact original values.
func TestViewFloat32_RoundTrip(t *testing.T) {
	input := []float32{0.0, 1.0, -1.0, 3.14159, math.MaxFloat32, -math.MaxFloat32, math.SmallestNonzeroFloat32}
	data := make([]byte, len(input)*4)
	for i, v := range input {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	got, ok := viewFloat32(data)
	if !ok {
		t.Fatal("viewFloat32 returned ok=false for valid aligned data")
	}
	if len(got) != len(input) {
		t.Fatalf("viewFloat32 returned %d elements, want %d", len(got), len(input))
	}
	for i, want := range input {
		if got[i] != want {
			t.Errorf("viewFloat32[%d] = %v, want %v", i, got[i], want)
		}
	}
}

// TestViewFloat64_RoundTrip encodes known float64 values as little-endian bytes
// and confirms that viewFloat64 returns the exact original values.
func TestViewFloat64_RoundTrip(t *testing.T) {
	input := []float64{0.0, 1.0, -1.0, math.Pi, math.E, math.MaxFloat64, math.SmallestNonzeroFloat64}
	data := make([]byte, len(input)*8)
	for i, v := range input {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	got, ok := viewFloat64(data)
	if !ok {
		t.Fatal("viewFloat64 returned ok=false for valid aligned data")
	}
	if len(got) != len(input) {
		t.Fatalf("viewFloat64 returned %d elements, want %d", len(got), len(input))
	}
	for i, want := range input {
		if got[i] != want {
			t.Errorf("viewFloat64[%d] = %v, want %v", i, got[i], want)
		}
	}
}

// TestViewInt32_RoundTrip encodes known int32 values as little-endian bytes
// and confirms that viewInt32 returns the exact original values.
func TestViewInt32_RoundTrip(t *testing.T) {
	input := []int32{0, 1, -1, math.MaxInt32, math.MinInt32, 42, -100}
	data := make([]byte, len(input)*4)
	for i, v := range input {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(v))
	}

	got, ok := viewInt32(data)
	if !ok {
		t.Fatal("viewInt32 returned ok=false for valid aligned data")
	}
	if len(got) != len(input) {
		t.Fatalf("viewInt32 returned %d elements, want %d", len(got), len(input))
	}
	for i, want := range input {
		if got[i] != want {
			t.Errorf("viewInt32[%d] = %v, want %v", i, got[i], want)
		}
	}
}

// TestViewInt64_RoundTrip encodes known int64 values as little-endian bytes
// and confirms that viewInt64 returns the exact original values.
func TestViewInt64_RoundTrip(t *testing.T) {
	input := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1<<32 + 7, -(1 << 40)}
	data := make([]byte, len(input)*8)
	for i, v := range input {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(v))
	}

	got, ok := viewInt64(data)
	if !ok {
		t.Fatal("viewInt64 returned ok=false for valid aligned data")
	}
	if len(got) != len(input) {
		t.Fatalf("viewInt64 returned %d elements, want %d", len(got), len(input))
	}
	for i, want := range input {
		if got[i] != want {
			t.Errorf("viewInt64[%d] = %v, want %v", i, got[i], want)
		}
	}
}

// TestViewFloat32_Empty checks that empty input returns (nil, true) — valid empty.
func TestViewFloat32_Empty(t *testing.T) {
	got, ok := viewFloat32([]byte{})
	if !ok {
		t.Fatal("viewFloat32 should return ok=true for empty input")
	}
	if got != nil {
		t.Fatalf("viewFloat32 should return nil slice for empty input, got %v", got)
	}
}

// TestViewFloat64_Empty checks that empty input returns (nil, true).
func TestViewFloat64_Empty(t *testing.T) {
	got, ok := viewFloat64([]byte{})
	if !ok {
		t.Fatal("viewFloat64 should return ok=true for empty input")
	}
	if got != nil {
		t.Fatalf("viewFloat64 should return nil slice for empty input, got %v", got)
	}
}

// TestViewFloat32_BadSize checks that size-misaligned input returns (nil, false).
func TestViewFloat32_BadSize(t *testing.T) {
	_, ok := viewFloat32([]byte{0x01, 0x02, 0x03}) // 3 bytes: not divisible by 4
	if ok {
		t.Fatal("viewFloat32 should return ok=false for non-multiple-of-4 length")
	}
}

// TestViewFloat64_BadSize checks that size-misaligned input returns (nil, false).
func TestViewFloat64_BadSize(t *testing.T) {
	_, ok := viewFloat64([]byte{0x01, 0x02, 0x03, 0x04, 0x05}) // 5 bytes: not divisible by 8
	if ok {
		t.Fatal("viewFloat64 should return ok=false for non-multiple-of-8 length")
	}
}

// TestFallbackCopyAgreesWith_ViewFloat32 confirms that decodeTensorFloat32Copy
// (the big-endian fallback logic) produces the same values as the LE unsafe path
// on this little-endian host. This validates that both code paths are correct.
func TestFallbackCopyAgreesWith_ViewFloat32(t *testing.T) {
	input := []float32{1.1, 2.2, 3.3, -4.4, 0.0}
	data := make([]byte, len(input)*4)
	for i, v := range input {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	// Unsafe path (only valid on LE, which this host is)
	view, ok := viewFloat32(data)
	if !ok {
		t.Fatal("viewFloat32 failed")
	}

	// Copy path (the big-endian fallback uses the same byte-order logic)
	copy_ := decodeTensorFloat32Copy(data)

	if len(view) != len(copy_) {
		t.Fatalf("length mismatch: view=%d copy=%d", len(view), len(copy_))
	}
	for i := range view {
		if view[i] != copy_[i] {
			t.Errorf("index %d: view=%v copy=%v", i, view[i], copy_[i])
		}
	}
}

// TestFallbackCopyAgreesWith_ViewFloat64 confirms that decodeTensorFloat64Copy
// produces the same values as the unsafe LE view path.
func TestFallbackCopyAgreesWith_ViewFloat64(t *testing.T) {
	input := []float64{math.Pi, math.E, -1.0, 0.0, 1e308}
	data := make([]byte, len(input)*8)
	for i, v := range input {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	view, ok := viewFloat64(data)
	if !ok {
		t.Fatal("viewFloat64 failed")
	}
	copy_ := decodeTensorFloat64Copy(data)

	if len(view) != len(copy_) {
		t.Fatalf("length mismatch: view=%d copy=%d", len(view), len(copy_))
	}
	for i := range view {
		if view[i] != copy_[i] {
			t.Errorf("index %d: view=%v copy=%v", i, view[i], copy_[i])
		}
	}
}
