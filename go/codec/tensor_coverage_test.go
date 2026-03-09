package codec_test

import (
	"encoding/binary"
	"testing"

	"github.com/Neumenon/cowrie/go"
	"github.com/Neumenon/cowrie/go/codec"
)

func TestDecodeTensorAuto_Int32(t *testing.T) {
	// Build int32 tensor data
	data := make([]byte, 3*4)
	for i := int32(0); i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(i+10))
	}
	tensor := cowrie.Tensor(cowrie.DTypeInt32, []uint64{3}, data)

	result := codec.DecodeTensorAuto(tensor)
	ints, ok := result.([]int32)
	if !ok {
		t.Fatalf("expected []int32, got %T", result)
	}
	if len(ints) != 3 || ints[0] != 10 || ints[1] != 11 || ints[2] != 12 {
		t.Errorf("unexpected result: %v", ints)
	}
}

func TestDecodeTensorAuto_Int64(t *testing.T) {
	data := make([]byte, 2*8)
	binary.LittleEndian.PutUint64(data[0:], 100)
	binary.LittleEndian.PutUint64(data[8:], 200)
	tensor := cowrie.Tensor(cowrie.DTypeInt64, []uint64{2}, data)

	result := codec.DecodeTensorAuto(tensor)
	ints, ok := result.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T", result)
	}
	if len(ints) != 2 || ints[0] != 100 || ints[1] != 200 {
		t.Errorf("unexpected result: %v", ints)
	}
}

func TestDecodeTensorAuto_Uint8(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	tensor := cowrie.Tensor(cowrie.DTypeUint8, []uint64{4}, data)

	result := codec.DecodeTensorAuto(tensor)
	bytes, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if len(bytes) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(bytes))
	}
}

func TestDecodeTensorAuto_UnknownDtype(t *testing.T) {
	// Use a dtype that isn't handled by specific decoders
	tensor := cowrie.Tensor(cowrie.DTypeBFloat16, []uint64{2}, []byte{0, 0, 0, 0})
	result := codec.DecodeTensorAuto(tensor)
	if result == nil {
		t.Fatal("expected non-nil for unknown dtype")
	}
}

func TestDecodeTensorAuto_Nil(t *testing.T) {
	result := codec.DecodeTensorAuto(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestDecodeTensorAuto_NonTensor(t *testing.T) {
	result := codec.DecodeTensorAuto(cowrie.Int64(42))
	if result != nil {
		t.Error("expected nil for non-tensor input")
	}
}

func TestDecodeFloat32Tensor_TooSmall(t *testing.T) {
	// Tensor with less than 4 bytes of data
	tensor := cowrie.Tensor(cowrie.DTypeFloat32, []uint64{1}, []byte{0, 0})
	result := codec.DecodeFloat32Tensor(tensor)
	if result != nil {
		t.Error("expected nil for too-small data")
	}
}

func TestDecodeFloat64Tensor_TooSmall(t *testing.T) {
	tensor := cowrie.Tensor(cowrie.DTypeFloat64, []uint64{1}, []byte{0, 0, 0, 0})
	result := codec.DecodeFloat64Tensor(tensor)
	if result != nil {
		t.Error("expected nil for too-small data")
	}
}

func TestDecodeFloat32Tensor_WrongDtype(t *testing.T) {
	tensor := cowrie.Tensor(cowrie.DTypeFloat64, []uint64{1}, make([]byte, 8))
	result := codec.DecodeFloat32Tensor(tensor)
	if result != nil {
		t.Error("expected nil for wrong dtype")
	}
}

func TestDecodeFloat64Tensor_WrongDtype(t *testing.T) {
	tensor := cowrie.Tensor(cowrie.DTypeFloat32, []uint64{1}, make([]byte, 4))
	result := codec.DecodeFloat64Tensor(tensor)
	if result != nil {
		t.Error("expected nil for wrong dtype")
	}
}

func TestEncodeFloat32Tensor_Empty(t *testing.T) {
	result := codec.EncodeFloat32Tensor(nil)
	if result.Type() != cowrie.TypeArray {
		t.Error("expected empty array for empty input")
	}
}

func TestEncodeFloat64Tensor_Empty(t *testing.T) {
	result := codec.EncodeFloat64Tensor(nil)
	if result.Type() != cowrie.TypeArray {
		t.Error("expected empty array for empty input")
	}
}

func TestEncodeFloat64AsFloat32Tensor_Empty(t *testing.T) {
	result := codec.EncodeFloat64AsFloat32Tensor(nil)
	if result.Type() != cowrie.TypeArray {
		t.Error("expected empty array for empty input")
	}
}

func TestTryEncodeNumericSlice_Mixed(t *testing.T) {
	// Mix of float64, float32, int, int64, int32
	arr := []any{float64(1.0), float32(2.0), int(3), int64(4), int32(5)}
	result := codec.TryEncodeNumericSlice(arr)
	if result == nil {
		t.Fatal("expected tensor for mixed numeric slice")
	}
}

func TestTryEncodeNumericSlice_Empty(t *testing.T) {
	result := codec.TryEncodeNumericSlice(nil)
	if result != nil {
		t.Error("expected nil for empty slice")
	}
}

func TestTryEncodeNumericSlice_NonNumeric(t *testing.T) {
	arr := []any{"a", "b"}
	result := codec.TryEncodeNumericSlice(arr)
	if result != nil {
		t.Error("expected nil for non-numeric slice")
	}
}

func TestTryEncodeNumericSliceWithOpts_HighPrecision(t *testing.T) {
	arr := []any{float64(1.0), float64(2.0)}
	result := codec.TryEncodeNumericSliceWithOpts(arr, true)
	if result == nil {
		t.Fatal("expected tensor")
	}
	td := result.Tensor()
	if td.DType != cowrie.DTypeFloat64 {
		t.Errorf("expected DTypeFloat64, got %v", td.DType)
	}
}
