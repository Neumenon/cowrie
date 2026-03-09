package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/Neumenon/cowrie/go"
)

// ============================================================
// compress.go: decompressGzip, decompressZstd
// ============================================================

func TestDecompressGzip(t *testing.T) {
	original := []byte("hello gzip world, this is a test of gzip compression")
	compressed, err := compressGzip(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressGzip(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestDecompressZstd(t *testing.T) {
	original := []byte("hello zstd world, this is a test of zstd compression")
	compressed, err := compressZstd(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressZstd(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestDecompressGzipInvalidData(t *testing.T) {
	_, err := decompressGzip([]byte("not gzip"))
	if err == nil {
		t.Fatal("expected error for invalid gzip data")
	}
}

func TestDecompressZstdInvalidData(t *testing.T) {
	_, err := decompressZstd([]byte("not zstd"))
	if err == nil {
		t.Fatal("expected error for invalid zstd data")
	}
}

// ============================================================
// cowrie_master_stream.go: IsLegacyStream
// ============================================================

func TestIsLegacyStream(t *testing.T) {
	// Too short
	if IsLegacyStream(nil) {
		t.Fatal("nil should not be legacy stream")
	}
	if IsLegacyStream([]byte{1, 2}) {
		t.Fatal("short data should not be legacy stream")
	}

	// Valid legacy: first 4 bytes = length < total
	data := make([]byte, 20)
	data[0] = 10 // length = 10
	if !IsLegacyStream(data) {
		t.Fatal("expected legacy stream detection")
	}

	// Zero length = not legacy
	data[0] = 0
	data[1] = 0
	data[2] = 0
	data[3] = 0
	if IsLegacyStream(data) {
		t.Fatal("zero length should not be legacy stream")
	}
}

// ============================================================
// encode_fast.go: FastEncodeBytes, encodeUint, encodeFloat32,
// getBuffer, putBuffer, isEmptyValue, encodeAny, fastToCowrieValue
// ============================================================

func TestFastEncodeBytes(t *testing.T) {
	type Item struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	item := Item{Name: "test", Count: 42}
	data, err := FastEncodeBytes(item)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Decode and verify
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["name"] != "test" {
		t.Fatalf("name mismatch: %v", result["name"])
	}
}

func TestFastEncodeBytesNil(t *testing.T) {
	data, err := FastEncodeBytes(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data for nil")
	}
}

func TestEncodeUintAndFloat32(t *testing.T) {
	type UintStruct struct {
		A uint   `json:"a"`
		B uint32 `json:"b"`
		C uint64 `json:"c"`
	}
	s := UintStruct{A: 42, B: 100, C: 999}
	data, err := FastEncode(s)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}

	type Float32Struct struct {
		X float32 `json:"x"`
	}
	fs := Float32Struct{X: 3.14}
	data2, err := FastEncode(fs)
	if err != nil {
		t.Fatal(err)
	}
	var result2 map[string]interface{}
	if err := DecodeBytes(data2, &result2); err != nil {
		t.Fatal(err)
	}
}

func TestIsEmptyValueComprehensive(t *testing.T) {
	// Test all kinds via struct encoding with omitempty
	type AllTypes struct {
		S   string        `json:"s,omitempty"`
		I   int           `json:"i,omitempty"`
		U   uint          `json:"u,omitempty"`
		F   float64       `json:"f,omitempty"`
		B   bool          `json:"b,omitempty"`
		Sl  []int         `json:"sl,omitempty"`
		M   map[string]int `json:"m,omitempty"`
		P   *int          `json:"p,omitempty"`
		Ifc interface{}   `json:"ifc,omitempty"`
	}

	// All zero values - should all be omitted
	empty := AllTypes{}
	data, err := FastEncode(empty)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty object with all omitempty zero values, got %d fields: %v", len(result), result)
	}

	// Non-zero values - should all be present
	n := 42
	full := AllTypes{
		S: "x", I: 1, U: 1, F: 1.0, B: true,
		Sl: []int{1}, M: map[string]int{"a": 1}, P: &n, Ifc: "hi",
	}
	data, err = FastEncode(full)
	if err != nil {
		t.Fatal(err)
	}
	result = nil
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 9 {
		t.Fatalf("expected 9 fields, got %d", len(result))
	}
}

func TestFastToCowrieValueSlice(t *testing.T) {
	// []float32 path
	v := fastToCowrieValue([]float32{1.0, 2.0, 3.0})
	if v.Type() != cowrie.TypeTensor {
		t.Fatalf("expected tensor, got %s", v.Type())
	}

	// map[string]any path
	v = fastToCowrieValue(map[string]interface{}{"a": 1})
	if v.Type() != cowrie.TypeObject {
		t.Fatalf("expected object, got %s", v.Type())
	}

	// []any path (numeric slices get tensorized)
	v = fastToCowrieValue([]interface{}{1, 2, 3})
	if v.Type() != cowrie.TypeArray && v.Type() != cowrie.TypeTensor {
		t.Fatalf("expected array or tensor, got %s", v.Type())
	}
	// []any with mixed types (non-numeric) should be array
	v = fastToCowrieValue([]interface{}{"a", "b"})
	if v.Type() != cowrie.TypeArray {
		t.Fatalf("expected array for string slice, got %s", v.Type())
	}

	// nil pointer to struct
	var s *struct{ X int }
	v = fastToCowrieValue(s)
	if v.Type() != cowrie.TypeNull {
		t.Fatalf("expected null for nil struct ptr, got %s", v.Type())
	}

	// Generic type
	v = fastToCowrieValue(42)
	if v.Type() != cowrie.TypeInt64 {
		t.Fatalf("expected int64, got %s", v.Type())
	}
}

// ============================================================
// reflect_cache.go: GetStructInfo
// ============================================================

func TestGetStructInfoPublic(t *testing.T) {
	type MyStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	info := GetStructInfo(reflect.TypeOf(MyStruct{}))
	if len(info.fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(info.fields))
	}
	if info.fieldMap["name"] != 0 || info.fieldMap["age"] != 1 {
		t.Fatalf("unexpected field map: %v", info.fieldMap)
	}
}

// ============================================================
// unmarshal.go: unmarshalPrimitive, unmarshalInterface,
// unmarshalTime, unmarshalBytes, unmarshalSlice
// ============================================================

func TestUnmarshalPrimitiveUint(t *testing.T) {
	// Uint from uint64
	type U struct {
		V uint `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Uint64(42)})
	var u U
	err := unmarshalValue(obj, reflect.ValueOf(&u).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if u.V != 42 {
		t.Fatalf("expected 42, got %d", u.V)
	}
}

func TestUnmarshalPrimitiveUintFromFloat(t *testing.T) {
	type U struct {
		V uint `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Float64(42.0)})
	var u U
	err := unmarshalValue(obj, reflect.ValueOf(&u).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if u.V != 42 {
		t.Fatalf("expected 42, got %d", u.V)
	}
}

func TestUnmarshalPrimitiveUintFromInt64(t *testing.T) {
	type U struct {
		V uint `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Int64(42)})
	var u U
	err := unmarshalValue(obj, reflect.ValueOf(&u).Elem())
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshalPrimitiveFloatFromInt(t *testing.T) {
	type F struct {
		V float64 `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Int64(42)})
	var f F
	err := unmarshalValue(obj, reflect.ValueOf(&f).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if f.V != 42.0 {
		t.Fatalf("expected 42.0, got %f", f.V)
	}
}

func TestUnmarshalPrimitiveFloatFromUint(t *testing.T) {
	type F struct {
		V float64 `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Uint64(42)})
	var f F
	err := unmarshalValue(obj, reflect.ValueOf(&f).Elem())
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshalPrimitiveIntFromFloat(t *testing.T) {
	type I struct {
		V int `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Float64(42.0)})
	var i I
	err := unmarshalValue(obj, reflect.ValueOf(&i).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if i.V != 42 {
		t.Fatalf("expected 42, got %d", i.V)
	}
}

func TestUnmarshalPrimitiveIntFromUint(t *testing.T) {
	type I struct {
		V int `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Uint64(42)})
	var i I
	err := unmarshalValue(obj, reflect.ValueOf(&i).Elem())
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshalPrimitiveStringFromNonString(t *testing.T) {
	// Non-string value to string target
	type S struct {
		V string `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Int64(42)})
	var s S
	err := unmarshalValue(obj, reflect.ValueOf(&s).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if s.V != "42" {
		t.Fatalf("expected '42', got %q", s.V)
	}
}

func TestUnmarshalPrimitiveTypeError(t *testing.T) {
	// Bool from non-bool
	type B struct {
		V bool `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("true")})
	var b B
	err := unmarshalValue(obj, reflect.ValueOf(&b).Elem())
	if err == nil {
		t.Fatal("expected error for bool from string")
	}
}

func TestUnmarshalInterface(t *testing.T) {
	// Null value
	var target interface{}
	rv := reflect.ValueOf(&target).Elem()
	err := unmarshalInterface(cowrie.Null(), rv)
	if err != nil {
		t.Fatal(err)
	}
	if target != nil {
		t.Fatalf("expected nil, got %v", target)
	}

	// String value
	err = unmarshalInterface(cowrie.String("hello"), rv)
	if err != nil {
		t.Fatal(err)
	}
	if target != "hello" {
		t.Fatalf("expected 'hello', got %v", target)
	}
}

func TestUnmarshalTimeFromString(t *testing.T) {
	type T struct {
		V time.Time `json:"v"`
	}
	// RFC3339Nano
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("2024-01-15T10:30:00.123456789Z")})
	var ts T
	err := unmarshalValue(obj, reflect.ValueOf(&ts).Elem())
	if err != nil {
		t.Fatal(err)
	}

	// RFC3339 (no nano)
	obj = cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("2024-01-15T10:30:00Z")})
	err = unmarshalValue(obj, reflect.ValueOf(&ts).Elem())
	if err != nil {
		t.Fatal(err)
	}

	// Invalid string
	obj = cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("not-a-time")})
	err = unmarshalValue(obj, reflect.ValueOf(&ts).Elem())
	if err == nil {
		t.Fatal("expected error for invalid time string")
	}
}

func TestUnmarshalTimeFromInt64(t *testing.T) {
	type T struct {
		V time.Time `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Int64(1705312200)})
	var ts T
	err := unmarshalValue(obj, reflect.ValueOf(&ts).Elem())
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshalTimeTypeError(t *testing.T) {
	type T struct {
		V time.Time `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Bool(true)})
	var ts T
	err := unmarshalValue(obj, reflect.ValueOf(&ts).Elem())
	if err == nil {
		t.Fatal("expected error for time from bool")
	}
}

func TestUnmarshalBytesFromString(t *testing.T) {
	type B struct {
		V []byte `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("hello")})
	var b B
	err := unmarshalValue(obj, reflect.ValueOf(&b).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if string(b.V) != "hello" {
		t.Fatalf("expected 'hello', got %q", b.V)
	}
}

func TestUnmarshalBytesFromNull(t *testing.T) {
	type B struct {
		V []byte `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Null()})
	var b B
	err := unmarshalValue(obj, reflect.ValueOf(&b).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if b.V != nil {
		t.Fatalf("expected nil, got %v", b.V)
	}
}

func TestUnmarshalBytesTypeError(t *testing.T) {
	type B struct {
		V []byte `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.Int64(42)})
	var b B
	err := unmarshalValue(obj, reflect.ValueOf(&b).Elem())
	if err == nil {
		t.Fatal("expected error for bytes from int")
	}
}

func TestUnmarshalMapNonStringKey(t *testing.T) {
	// map[int]string - non-string key should error
	v := cowrie.Object(cowrie.Member{Key: "a", Value: cowrie.String("b")})
	target := make(map[int]string)
	rv := reflect.ValueOf(&target).Elem()
	err := unmarshalMap(v, rv)
	if err == nil {
		t.Fatal("expected error for non-string map key")
	}
}

func TestUnmarshalMapTypeError(t *testing.T) {
	v := cowrie.Array(cowrie.String("a"))
	target := make(map[string]string)
	rv := reflect.ValueOf(&target).Elem()
	err := unmarshalMap(v, rv)
	if err == nil {
		t.Fatal("expected error for map from array")
	}
}

func TestUnmarshalRawMessage(t *testing.T) {
	type R struct {
		Data json.RawMessage `json:"data"`
	}
	inner := cowrie.Object(cowrie.Member{Key: "x", Value: cowrie.Int64(1)})
	obj := cowrie.Object(cowrie.Member{Key: "data", Value: inner})
	var r R
	err := unmarshalValue(obj, reflect.ValueOf(&r).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Data) == 0 {
		t.Fatal("expected non-empty raw message")
	}
}

func TestUnmarshalFloat64SliceFromTensor(t *testing.T) {
	// float64 slice from float32 tensor
	tensor := EncodeFloat32Tensor([]float32{1.0, 2.0, 3.0})
	type F struct {
		V []float64 `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: tensor})
	var f F
	err := unmarshalValue(obj, reflect.ValueOf(&f).Elem())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.V) != 3 {
		t.Fatalf("expected 3, got %d", len(f.V))
	}
}

func TestUnmarshalSliceTypeError(t *testing.T) {
	type S struct {
		V []int `json:"v"`
	}
	obj := cowrie.Object(cowrie.Member{Key: "v", Value: cowrie.String("not-array")})
	var s S
	err := unmarshalValue(obj, reflect.ValueOf(&s).Elem())
	if err == nil {
		t.Fatal("expected error for slice from string")
	}
}

// ============================================================
// cowrie.go: tensorDataToSlice (float64, int32, int64, unknown)
// ============================================================

func TestTensorDataToSliceFloat64(t *testing.T) {
	tensor := EncodeFloat64Tensor([]float64{1.0, 2.0, 3.0})
	td := tensor.Tensor()
	result := tensorDataToSlice(&td)
	f64s, ok := result.([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", result)
	}
	if len(f64s) != 3 {
		t.Fatalf("expected 3, got %d", len(f64s))
	}
}

func TestTensorDataToSliceInt32(t *testing.T) {
	// Build int32 tensor manually
	data := make([]byte, 3*4)
	for i, v := range []int32{1, 2, 3} {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(v))
	}
	tensor := cowrie.Tensor(cowrie.DTypeInt32, []uint64{3}, data)
	td := tensor.Tensor()
	result := tensorDataToSlice(&td)
	i32s, ok := result.([]int32)
	if !ok {
		t.Fatalf("expected []int32, got %T", result)
	}
	if len(i32s) != 3 {
		t.Fatalf("expected 3, got %d", len(i32s))
	}
}

func TestTensorDataToSliceInt64(t *testing.T) {
	// Build int64 tensor manually
	data := make([]byte, 3*8)
	for i, v := range []int64{1, 2, 3} {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(v))
	}
	tensor := cowrie.Tensor(cowrie.DTypeInt64, []uint64{3}, data)
	td := tensor.Tensor()
	result := tensorDataToSlice(&td)
	i64s, ok := result.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T", result)
	}
	if len(i64s) != 3 {
		t.Fatalf("expected 3, got %d", len(i64s))
	}
}

func TestTensorDataToSliceNil(t *testing.T) {
	result := tensorDataToSlice(nil)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestTensorDataToSliceEmpty(t *testing.T) {
	td := &cowrie.TensorData{Data: nil}
	result := tensorDataToSlice(td)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestTensorDataToSliceUnknownDType(t *testing.T) {
	td := &cowrie.TensorData{DType: 255, Data: []byte{1, 2, 3}}
	result := tensorDataToSlice(td)
	b, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte for unknown dtype, got %T", result)
	}
	if len(b) != 3 {
		t.Fatalf("expected 3, got %d", len(b))
	}
}

// ============================================================
// stream.go: Sync, Position, Remaining
// ============================================================

func TestStreamSync(t *testing.T) {
	// Writer that doesn't support Sync
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	if err := sw.Sync(); err != nil {
		t.Fatal(err)
	}

	// Writer that supports Sync
	syncer := &syncWriter{}
	sw2 := NewStreamWriter(syncer)
	if err := sw2.Sync(); err != nil {
		t.Fatal(err)
	}
	if !syncer.synced {
		t.Fatal("expected sync to be called")
	}
}

type syncWriter struct {
	bytes.Buffer
	synced bool
}

func (s *syncWriter) Sync() error {
	s.synced = true
	return nil
}

func TestStreamReaderPositionRemaining(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)
	sw.Write(map[string]interface{}{"a": 1})
	sw.Write(map[string]interface{}{"b": 2})

	sr := NewStreamReader(buf.Bytes())
	if sr.Position() != 0 {
		t.Fatalf("expected position 0, got %d", sr.Position())
	}
	total := sr.Remaining()
	if total == 0 {
		t.Fatal("expected non-zero remaining")
	}

	var v map[string]interface{}
	sr.Next(&v)
	if sr.Position() == 0 {
		t.Fatal("expected position to advance")
	}
	if sr.Remaining() >= total {
		t.Fatal("expected remaining to decrease")
	}
}

func TestStreamWriterError(t *testing.T) {
	// Write to failing writer
	sw := NewStreamWriter(&failWriter{})
	err := sw.Write(map[string]interface{}{"a": 1})
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

type failWriter struct{}

func (f *failWriter) Write(p []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

// ============================================================
// CowrieCodec: Decode error paths
// ============================================================

func TestCowrieCodecDecodeNonPointer(t *testing.T) {
	c := CowrieCodec{}
	data, _ := EncodeBytes(map[string]interface{}{"a": 1})
	var v map[string]interface{}
	err := c.Decode(bytes.NewReader(data), v) // not a pointer
	if err == nil {
		t.Fatal("expected error for non-pointer target")
	}
}

func TestCowrieCodecDecodeHighPrecision(t *testing.T) {
	c := CowrieCodec{HighPrecision: true}
	var buf bytes.Buffer
	input := map[string]interface{}{
		"floats": []float64{1.0, 2.0, 3.0},
	}
	if err := c.Encode(&buf, input); err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := c.Decode(bytes.NewReader(buf.Bytes()), &result); err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// encode_fast.go: hasOmitEmpty edge cases
// ============================================================

func TestHasOmitEmpty(t *testing.T) {
	if hasOmitEmpty("") {
		t.Fatal("empty should not have omitempty")
	}
	if !hasOmitEmpty("omitempty") {
		t.Fatal("should detect omitempty")
	}
	if !hasOmitEmpty("string,omitempty") {
		t.Fatal("should detect omitempty after comma")
	}
	if hasOmitEmpty("string,other") {
		t.Fatal("should not detect without omitempty")
	}
	if !hasOmitEmpty("omitempty,string") {
		t.Fatal("should detect omitempty at start")
	}
}

// ============================================================
// encode_fast.go: encodeRawMessage edge cases
// ============================================================

func TestEncodeRawMessageEmpty(t *testing.T) {
	type R struct {
		Data json.RawMessage `json:"data"`
	}
	// Empty RawMessage should encode as null
	r := R{Data: json.RawMessage{}}
	data, err := FastEncode(r)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["data"] != nil {
		t.Fatalf("expected nil for empty raw message, got %v", result["data"])
	}
}

func TestEncodeRawMessageInvalidJSON(t *testing.T) {
	type R struct {
		Data json.RawMessage `json:"data"`
	}
	r := R{Data: json.RawMessage("not valid json")}
	data, err := FastEncode(r)
	if err != nil {
		t.Fatal(err)
	}
	// Should be preserved as string
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// Embedded struct encoding/decoding
// ============================================================

func TestFastEncodeEmbeddedStruct(t *testing.T) {
	type Base struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type Extended struct {
		Base
		Extra string `json:"extra"`
	}
	v := Extended{Base: Base{ID: "123", Name: "test"}, Extra: "stuff"}
	data, err := FastEncode(v)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["id"] != "123" || result["name"] != "test" || result["extra"] != "stuff" {
		t.Fatalf("unexpected result: %v", result)
	}
}

// ============================================================
// encode_fast.go: encodeSlice generic and encodeMap
// ============================================================

func TestFastEncodeSliceOfStructs(t *testing.T) {
	type Item struct {
		V int `json:"v"`
	}
	items := []Item{{V: 1}, {V: 2}}
	data, err := FastEncode(items)
	if err != nil {
		t.Fatal(err)
	}
	var result []map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
}

func TestFastEncodePointerFields(t *testing.T) {
	type S struct {
		P *string `json:"p"`
		Q *int    `json:"q,omitempty"`
	}
	s := "hello"
	v := S{P: &s, Q: nil}
	data, err := FastEncode(v)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["p"] != "hello" {
		t.Fatalf("expected 'hello', got %v", result["p"])
	}
}

func TestFastEncodeEncodeFloat32Slice(t *testing.T) {
	type S struct {
		V []float32 `json:"v"`
	}
	v := S{V: []float32{1.0, 2.0}}
	data, err := FastEncode(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}

	// Nil float32 slice
	v2 := S{V: nil}
	data, err = FastEncode(v2)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := DecodeBytes(data, &result); err != nil {
		t.Fatal(err)
	}
}
