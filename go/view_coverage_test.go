package cowrie

import (
	"testing"
)

func TestViewFloat32_Coverage(t *testing.T) {
	data := encodeFloat32LE([]float32{1.0, 2.0, 3.0})
	result, ok := viewFloat32(data)
	if !ok {
		t.Fatal("viewFloat32 failed")
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}

	// Empty
	_, ok = viewFloat32(nil)
	if !ok {
		t.Error("nil should be ok")
	}

	// Bad size
	_, ok = viewFloat32([]byte{1, 2, 3})
	if ok {
		t.Error("bad size should fail")
	}
}

func TestViewFloat64_Coverage(t *testing.T) {
	data := encodeFloat64LE([]float64{1.0, 2.0})
	result, ok := viewFloat64(data)
	if !ok {
		t.Fatal("viewFloat64 failed")
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestViewInt32_Coverage(t *testing.T) {
	data := encodeInt32LE([]int32{10, 20, 30})
	result, ok := viewInt32(data)
	if !ok {
		t.Fatal("viewInt32 failed")
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
}

func TestViewInt64_Coverage(t *testing.T) {
	data := encodeInt64LE([]int64{100, 200})
	result, ok := viewInt64(data)
	if !ok {
		t.Fatal("viewInt64 failed")
	}
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestCopyInt32_Coverage(t *testing.T) {
	data := encodeInt32LE([]int32{1, 2, 3})
	td := &TensorData{
		DType: DTypeInt32,
		Dims: []uint64{3},
		Data:  data,
	}
	result := CopyInt32(td)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 1 {
		t.Error("first element")
	}
}

func TestCopyInt64_Coverage(t *testing.T) {
	data := encodeInt64LE([]int64{10, 20})
	td := &TensorData{
		DType: DTypeInt64,
		Dims: []uint64{2},
		Data:  data,
	}
	result := CopyInt64(td)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}
