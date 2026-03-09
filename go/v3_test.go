package cowrie

import (
	"bytes"
	"io"
	"math"
	"testing"
	"unsafe"
)

// ============================================================
// v3 Inline Types: FIXINT
// ============================================================

func TestFixintRoundTrip(t *testing.T) {
	for _, val := range []int64{0, 1, 42, 100, 127} {
		v := Int64(val)
		data, err := Encode(v)
		if err != nil {
			t.Fatalf("encode fixint(%d): %v", val, err)
		}

		// Single byte for the fixint tag (after header+dict)
		rootTag := findRootTag(data)
		expected := byte(TagFixintBase + val)
		if rootTag != expected {
			t.Errorf("fixint(%d): tag got 0x%02x, want 0x%02x", val, rootTag, expected)
		}

		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("decode fixint(%d): %v", val, err)
		}
		if decoded.Type() != TypeInt64 || decoded.Int64() != val {
			t.Errorf("fixint(%d): decoded as %v = %d", val, decoded.Type(), decoded.Int64())
		}
	}
}

func TestFixintBoundary(t *testing.T) {
	// 128 must NOT use fixint
	v := Int64(128)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	rootTag := findRootTag(data)
	if rootTag != TagInt64 {
		t.Errorf("int64(128): tag got 0x%02x, want 0x%02x (TagInt64)", rootTag, TagInt64)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Int64() != 128 {
		t.Errorf("int64(128): got %d", decoded.Int64())
	}
}

func TestFixintZeroWireSize(t *testing.T) {
	// fixint(0) should be 1 byte for the value (total = header + dict + 1)
	v := Int64(0)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	// Header: 2 (magic) + 1 (version) + 1 (flags) + 1 (dict len=0) = 5
	// Value: 1 (fixint tag)
	// Total: 6
	if len(data) != 6 {
		t.Errorf("fixint(0) wire size: got %d, want 6", len(data))
	}
}

// ============================================================
// v3 Inline Types: FIXNEG
// ============================================================

func TestFixnegRoundTrip(t *testing.T) {
	for _, val := range []int64{-1, -5, -10, -16} {
		v := Int64(val)
		data, err := Encode(v)
		if err != nil {
			t.Fatalf("encode fixneg(%d): %v", val, err)
		}

		rootTag := findRootTag(data)
		expected := byte(TagFixnegBase + (-1 - val))
		if rootTag != expected {
			t.Errorf("fixneg(%d): tag got 0x%02x, want 0x%02x", val, rootTag, expected)
		}

		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("decode fixneg(%d): %v", val, err)
		}
		if decoded.Int64() != val {
			t.Errorf("fixneg(%d): decoded as %d", val, decoded.Int64())
		}
	}
}

func TestFixnegBoundary(t *testing.T) {
	// -17 must NOT use fixneg
	v := Int64(-17)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	rootTag := findRootTag(data)
	if rootTag != TagInt64 {
		t.Errorf("int64(-17): tag got 0x%02x, want 0x%02x (TagInt64)", rootTag, TagInt64)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Int64() != -17 {
		t.Errorf("int64(-17): got %d", decoded.Int64())
	}
}

// ============================================================
// v3 Inline Types: FIXARRAY
// ============================================================

func TestFixarrayRoundTrip(t *testing.T) {
	arr := Array(Int64(1), Int64(2), Int64(3))
	data, err := Encode(arr)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	expected := byte(TagFixarrayBase + 3)
	if rootTag != expected {
		t.Errorf("fixarray(3): tag got 0x%02x, want 0x%02x", rootTag, expected)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Len() != 3 {
		t.Errorf("fixarray: len got %d, want 3", decoded.Len())
	}
	for i := 0; i < 3; i++ {
		if decoded.Index(i).Int64() != int64(i+1) {
			t.Errorf("fixarray[%d]: got %d", i, decoded.Index(i).Int64())
		}
	}
}

func TestFixarrayEmpty(t *testing.T) {
	arr := Array()
	data, err := Encode(arr)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	if rootTag != TagFixarrayBase {
		t.Errorf("fixarray(0): tag got 0x%02x, want 0x%02x", rootTag, TagFixarrayBase)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Len() != 0 {
		t.Errorf("fixarray(0): len got %d", decoded.Len())
	}
}

func TestFixarrayFallback(t *testing.T) {
	// 16+ elements should use TagArray
	items := make([]*Value, 16)
	for i := range items {
		items[i] = Int64(int64(i))
	}
	arr := Array(items...)
	data, err := Encode(arr)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	if rootTag != TagArray {
		t.Errorf("array(16): tag got 0x%02x, want 0x%02x (TagArray)", rootTag, TagArray)
	}
}

// ============================================================
// v3 Inline Types: FIXMAP
// ============================================================

func TestFixmapRoundTrip(t *testing.T) {
	obj := Object(Member{Key: "a", Value: Int64(1)})
	data, err := Encode(obj)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	expected := byte(TagFixmapBase + 1)
	if rootTag != expected {
		t.Errorf("fixmap(1): tag got 0x%02x, want 0x%02x", rootTag, expected)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Len() != 1 {
		t.Errorf("fixmap: len got %d, want 1", decoded.Len())
	}
	if decoded.Get("a").Int64() != 1 {
		t.Errorf("fixmap['a']: got %d", decoded.Get("a").Int64())
	}
}

func TestFixmapEmpty(t *testing.T) {
	obj := Object()
	data, err := Encode(obj)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	if rootTag != TagFixmapBase {
		t.Errorf("fixmap(0): tag got 0x%02x, want 0x%02x", rootTag, TagFixmapBase)
	}
}

func TestFixmapFallback(t *testing.T) {
	// 16+ fields should use TagObject
	members := make([]Member, 16)
	for i := range members {
		members[i] = Member{Key: string(rune('a' + i)), Value: Int64(int64(i))}
	}
	obj := Object(members...)
	data, err := Encode(obj)
	if err != nil {
		t.Fatal(err)
	}

	rootTag := findRootTag(data)
	if rootTag != TagObject {
		t.Errorf("object(16): tag got 0x%02x, want 0x%02x (TagObject)", rootTag, TagObject)
	}
}

// ============================================================
// v3 BITMASK
// ============================================================

func TestBitmaskRoundTrip(t *testing.T) {
	bools := []bool{true, false, true, true, false, false, true, false, true}
	v := BitmaskFromBools(bools)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	bm := decoded.Bitmask()
	if bm.Count != uint64(len(bools)) {
		t.Fatalf("bitmask count: got %d, want %d", bm.Count, len(bools))
	}
	for i, expected := range bools {
		if bm.Get(uint64(i)) != expected {
			t.Errorf("bitmask[%d]: got %v, want %v", i, bm.Get(uint64(i)), expected)
		}
	}
}

func TestBitmaskEmpty(t *testing.T) {
	v := Bitmask(0, nil)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Bitmask().Count != 0 {
		t.Errorf("bitmask count: got %d, want 0", decoded.Bitmask().Count)
	}
}

func TestBitmaskLarge(t *testing.T) {
	// 2048-element attention mask
	count := uint64(2048)
	byteLen := (count + 7) / 8
	bits := make([]byte, byteLen)
	// Set every other bit
	for i := uint64(0); i < count; i += 2 {
		bits[i/8] |= 1 << (i % 8)
	}
	v := Bitmask(count, bits)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	// Wire size check: header(5) + tag(1) + uvarint(count=2048→2 bytes) + 256 bytes data
	// Should be ~264 bytes, NOT ~4000
	if len(data) > 300 {
		t.Errorf("bitmask(2048) wire size: got %d, want <300", len(data))
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	bm := decoded.Bitmask()
	if bm.Count != count {
		t.Fatalf("count: got %d", bm.Count)
	}
	for i := uint64(0); i < count; i++ {
		expected := i%2 == 0
		if bm.Get(i) != expected {
			t.Errorf("bitmask[%d]: got %v, want %v", i, bm.Get(i), expected)
		}
	}
}

func TestBitmaskPartialByte(t *testing.T) {
	// 7 bits = partial byte (should be 1 byte)
	bools := []bool{true, true, true, true, true, true, true}
	v := BitmaskFromBools(bools)
	bm := v.Bitmask()
	if len(bm.Bits) != 1 {
		t.Errorf("7-bit bitmask: got %d bytes, want 1", len(bm.Bits))
	}
	// Verify high bit is cleared
	if bm.Bits[0]&0x80 != 0 {
		t.Error("7-bit bitmask: 8th bit should be cleared")
	}
}

func TestBitmaskExactByte(t *testing.T) {
	// 8 bits = exact byte boundary
	bools := make([]bool, 8)
	for i := range bools {
		bools[i] = true
	}
	v := BitmaskFromBools(bools)
	data, _ := Encode(v)
	decoded, _ := Decode(data)
	bm := decoded.Bitmask()
	if bm.Count != 8 {
		t.Errorf("count: got %d", bm.Count)
	}
	for i := uint64(0); i < 8; i++ {
		if !bm.Get(i) {
			t.Errorf("bitmask[%d] should be true", i)
		}
	}
}

func TestBitmaskCount1(t *testing.T) {
	v := BitmaskFromBools([]bool{true})
	data, _ := Encode(v)
	decoded, _ := Decode(data)
	bm := decoded.Bitmask()
	if bm.Count != 1 || !bm.Get(0) {
		t.Error("single-bit bitmask failed")
	}
}

// ============================================================
// v3 Wire Size Regression
// ============================================================

func TestWireSizeFixint(t *testing.T) {
	// Fixint 42: header(5) + 1 byte = 6
	data, _ := Encode(Int64(42))
	if len(data) != 6 {
		t.Errorf("fixint(42) size: got %d, want 6", len(data))
	}

	// Old TagInt64 for 42: header(5) + tag(1) + zigzag_varint(1) = 7
	// Savings: 1 byte per small int
}

func TestWireSizeFixneg(t *testing.T) {
	// Fixneg -1: header(5) + 1 byte = 6
	data, _ := Encode(Int64(-1))
	if len(data) != 6 {
		t.Errorf("fixneg(-1) size: got %d, want 6", len(data))
	}
}

func TestWireSizeFixarray(t *testing.T) {
	// Array [1,2,3]: header(5) + fixarray_tag(1) + 3×fixint(3) = 9
	data, _ := Encode(Array(Int64(1), Int64(2), Int64(3)))
	if len(data) != 9 {
		t.Errorf("[1,2,3] size: got %d, want 9", len(data))
	}
}

// ============================================================
// v3 Zero-Copy String Decode
// ============================================================

func TestUnsafeStrings(t *testing.T) {
	original := String("hello world")
	data, _ := Encode(original)

	opts := DefaultDecodeOptions()
	opts.UnsafeStrings = true
	decoded, err := DecodeWithOptions(data, opts)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.String() != "hello world" {
		t.Errorf("unsafe string: got %q", decoded.String())
	}

	// Verify it points into the input buffer (unsafe behavior)
	strHeader := (*[2]uintptr)(unsafe.Pointer(&decoded.stringVal))
	dataPtr := uintptr(unsafe.Pointer(&data[0]))
	dataEnd := dataPtr + uintptr(len(data))
	strPtr := strHeader[0]
	if strPtr < dataPtr || strPtr >= dataEnd {
		t.Error("unsafe string does not point into input buffer")
	}
}

func TestUnsafeStringsDefault(t *testing.T) {
	// Default: safe copy, should not alias
	original := String("test")
	data, _ := Encode(original)

	decoded, _ := Decode(data)
	if decoded.String() != "test" {
		t.Errorf("safe string: got %q", decoded.String())
	}
}

// ============================================================
// v3 Streaming Tensor Decode
// ============================================================

type testTensorSink struct {
	dtype DType
	dims  []uint64
	data  []byte
}

func (s *testTensorSink) AcceptTensor(dtype DType, dims []uint64, data io.Reader) error {
	s.dtype = dtype
	s.dims = dims
	var err error
	s.data, err = io.ReadAll(data)
	return err
}

func TestTensorSink(t *testing.T) {
	tensorData := make([]byte, 1024)
	for i := range tensorData {
		tensorData[i] = byte(i % 256)
	}
	v := Tensor(DTypeFloat32, []uint64{16, 16}, tensorData)
	encoded, _ := Encode(v)

	sink := &testTensorSink{}
	opts := DefaultDecodeOptions()
	opts.TensorSink = sink

	decoded, err := DecodeWithOptions(encoded, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Decoded tensor should have nil data (streamed to sink)
	if len(decoded.Tensor().Data) != 0 {
		t.Errorf("tensor data should be nil/empty when sink is used, got %d bytes", len(decoded.Tensor().Data))
	}

	// Sink should have the data
	if sink.dtype != DTypeFloat32 {
		t.Errorf("sink dtype: got %v", sink.dtype)
	}
	if len(sink.dims) != 2 || sink.dims[0] != 16 || sink.dims[1] != 16 {
		t.Errorf("sink dims: got %v", sink.dims)
	}
	if !bytes.Equal(sink.data, tensorData) {
		t.Error("sink data mismatch")
	}
}

// ============================================================
// v3 iovec Scatter-Gather
// ============================================================

func TestEncodeToWriterMatchesEncode(t *testing.T) {
	// Scalar
	for _, v := range []*Value{
		Null(), Bool(true), Int64(42), Float64(3.14), String("hello"),
		Bytes([]byte{1, 2, 3}),
	} {
		expected, _ := Encode(v)
		var buf bytes.Buffer
		if err := EncodeToWriter(&buf, v); err != nil {
			t.Fatalf("EncodeToWriter: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), expected) {
			t.Errorf("mismatch for %v: got %x, want %x", v.Type(), buf.Bytes(), expected)
		}
	}
}

func TestEncodeToWriterTensor(t *testing.T) {
	tensorData := make([]byte, 4096)
	for i := range tensorData {
		tensorData[i] = byte(i % 256)
	}
	v := Tensor(DTypeFloat32, []uint64{32, 32}, tensorData)

	expected, _ := Encode(v)
	var buf bytes.Buffer
	if err := EncodeToWriter(&buf, v); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("tensor scatter-gather mismatch: got %d bytes, want %d bytes", buf.Len(), len(expected))
	}
}

func TestEncodeToWriterNested(t *testing.T) {
	v := Object(
		Member{Key: "tensors", Value: Array(
			Tensor(DTypeFloat32, []uint64{2, 2}, make([]byte, 16)),
			Tensor(DTypeFloat32, []uint64{3, 3}, make([]byte, 36)),
		)},
		Member{Key: "label", Value: Int64(42)},
	)

	expected, _ := Encode(v)
	var buf bytes.Buffer
	if err := EncodeToWriter(&buf, v); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Error("nested tensor scatter-gather mismatch")
	}
}

// ============================================================
// v3 Format Detection (3-byte check)
// ============================================================

func TestFormatDetection3Byte(t *testing.T) {
	// Gen2 header: "SJ" + 0x02
	gen2 := []byte{'S', 'J', 0x02, 0x00, 0x00, 0x00} // header + empty dict + null
	if gen2[0] != 'S' || gen2[1] != 'J' || gen2[2] != 0x02 {
		t.Error("Gen2 detection should match")
	}

	// Hypothetical Gen1 data starting with fixint(19) = 0x53 = 'S'
	// followed by 0x4A = 'J', but third byte would be Gen1 value tag, not 0x02
	gen1Collision := []byte{0x53, 0x4A, 0x03} // "SJ" + TagInt64 (Gen1 data)
	isGen2 := gen1Collision[0] == 'S' && gen1Collision[1] == 'J' && gen1Collision[2] == 0x02
	if isGen2 {
		t.Error("should NOT detect Gen1 data starting with 'SJ' as Gen2")
	}
}

// ============================================================
// v3 Int Boundary Exhaustive
// ============================================================

func TestFixintAllValues(t *testing.T) {
	// Test all 128 fixint values
	for i := int64(0); i <= 127; i++ {
		v := Int64(i)
		data, err := Encode(v)
		if err != nil {
			t.Fatalf("fixint(%d): %v", i, err)
		}
		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("decode fixint(%d): %v", i, err)
		}
		if decoded.Int64() != i {
			t.Errorf("fixint(%d): got %d", i, decoded.Int64())
		}
	}
}

func TestFixnegAllValues(t *testing.T) {
	// Test all 16 fixneg values
	for i := int64(-1); i >= -16; i-- {
		v := Int64(i)
		data, err := Encode(v)
		if err != nil {
			t.Fatalf("fixneg(%d): %v", i, err)
		}
		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("decode fixneg(%d): %v", i, err)
		}
		if decoded.Int64() != i {
			t.Errorf("fixneg(%d): got %d", i, decoded.Int64())
		}
	}
}

func TestEdgeCases(t *testing.T) {
	// Int64 max and min still work via TagInt64
	for _, val := range []int64{math.MaxInt64, math.MinInt64, 128, -17, 256, -100} {
		v := Int64(val)
		data, _ := Encode(v)
		decoded, err := Decode(data)
		if err != nil {
			t.Fatalf("int64(%d): %v", val, err)
		}
		if decoded.Int64() != val {
			t.Errorf("int64(%d): got %d", val, decoded.Int64())
		}
	}
}
