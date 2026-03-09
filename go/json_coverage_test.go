package cowrie

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFromAnyStrict_AllTypes(t *testing.T) {
	// nil
	v := FromAny(nil)
	if v.Type() != TypeNull {
		t.Error("nil should be null")
	}

	// bool
	v = FromAny(true)
	if v.Type() != TypeBool {
		t.Error("bool")
	}

	// float64 (integer range)
	v = FromAny(float64(42))
	if v.Type() != TypeInt64 || v.Int64() != 42 {
		t.Error("float64 int")
	}

	// float64 (non-integer)
	v = FromAny(float64(3.14))
	if v.Type() != TypeFloat64 {
		t.Error("float64")
	}

	// json.Number (float)
	v = FromAny(json.Number("3.14"))
	if v.Type() != TypeFloat64 {
		t.Error("json.Number float")
	}

	// json.Number (int)
	v = FromAny(json.Number("42"))
	if v.Type() != TypeInt64 || v.Int64() != 42 {
		t.Error("json.Number int")
	}

	// string
	v = FromAny("hello")
	if v.Type() != TypeString {
		t.Error("string")
	}

	// []any
	v = FromAny([]any{1, "two", true})
	if v.Type() != TypeArray || len(v.Array()) != 3 {
		t.Error("array")
	}

	// map[string]any
	v = FromAny(map[string]any{"x": 1, "y": "z"})
	if v.Type() != TypeObject {
		t.Error("object")
	}

	// Direct Go types
	v = FromAny(int(42))
	if v.Int64() != 42 {
		t.Error("int")
	}
	v = FromAny(int32(42))
	if v.Int64() != 42 {
		t.Error("int32")
	}
	v = FromAny(int64(42))
	if v.Int64() != 42 {
		t.Error("int64")
	}
	v = FromAny(uint(42))
	if v.Uint64() != 42 {
		t.Error("uint")
	}
	v = FromAny(uint32(42))
	if v.Uint64() != 42 {
		t.Error("uint32")
	}
	v = FromAny(uint64(42))
	if v.Uint64() != 42 {
		t.Error("uint64")
	}
	v = FromAny([]byte{1, 2, 3})
	if v.Type() != TypeBytes {
		t.Error("bytes")
	}
	now := time.Now()
	v = FromAny(now)
	if v.Type() != TypeDatetime64 {
		t.Error("time")
	}

	// Fallback via JSON marshal (struct)
	type testStruct struct {
		X int    `json:"x"`
		Y string `json:"y"`
	}
	v = FromAny(testStruct{X: 1, Y: "hello"})
	if v.Type() != TypeObject {
		t.Error("struct fallback")
	}
}

func TestFromAnyEnriched_Types(t *testing.T) {
	// nil
	v := FromAnyEnriched(nil)
	if v.Type() != TypeNull {
		t.Error("nil")
	}

	// bool
	v = FromAnyEnriched(true)
	if v.Type() != TypeBool {
		t.Error("bool")
	}

	// float64
	v = FromAnyEnriched(float64(42))
	if v.Int64() != 42 {
		t.Error("float64 int range")
	}

	v = FromAnyEnriched(float64(3.14))
	if v.Type() != TypeFloat64 {
		t.Error("float64")
	}

	// json.Number
	v = FromAnyEnriched(json.Number("3.14"))
	if v.Type() != TypeFloat64 {
		t.Error("json.Number float")
	}

	v = FromAnyEnriched(json.Number("42"))
	if v.Int64() != 42 {
		t.Error("json.Number int")
	}

	// string with datetime inference
	v = FromAnyEnriched("2024-01-01T00:00:00Z")
	if v.Type() != TypeDatetime64 {
		t.Error("datetime inference")
	}

	// UUID inference
	v = FromAnyEnriched("550e8400-e29b-41d4-a716-446655440000")
	if v.Type() != TypeUUID128 {
		t.Error("uuid inference")
	}

	// array
	v = FromAnyEnriched([]any{1, 2})
	if v.Type() != TypeArray {
		t.Error("array")
	}

	// map
	v = FromAnyEnriched(map[string]any{"x": 1})
	if v.Type() != TypeObject {
		t.Error("map")
	}

	// direct go types
	v = FromAnyEnriched(int(5))
	if v.Int64() != 5 {
		t.Error("int")
	}
	v = FromAnyEnriched(int32(5))
	if v.Int64() != 5 {
		t.Error("int32")
	}
	v = FromAnyEnriched(uint(5))
	if v.Uint64() != 5 {
		t.Error("uint")
	}
	v = FromAnyEnriched(uint32(5))
	if v.Uint64() != 5 {
		t.Error("uint32")
	}
	v = FromAnyEnriched([]byte{1})
	if v.Type() != TypeBytes {
		t.Error("bytes")
	}
}

func TestToAny_AllTypes(t *testing.T) {
	// nil
	if ToAny(nil) != nil {
		t.Error("nil")
	}

	// null
	if ToAny(Null()) != nil {
		t.Error("null")
	}

	// bool
	b, ok := ToAny(Bool(true)).(bool)
	if !ok || !b {
		t.Error("bool")
	}

	// int64
	i, ok := ToAny(Int64(42)).(int64)
	if !ok || i != 42 {
		t.Error("int64")
	}

	// uint64
	u, ok := ToAny(Uint64(100)).(uint64)
	if !ok || u != 100 {
		t.Error("uint64")
	}

	// float64
	f, ok := ToAny(Float64(3.14)).(float64)
	if !ok || f != 3.14 {
		t.Error("float64")
	}

	// string
	s, ok := ToAny(String("hello")).(string)
	if !ok || s != "hello" {
		t.Error("string")
	}

	// bytes -> base64
	bs, ok := ToAny(Bytes([]byte{1, 2, 3})).(string)
	if !ok || bs == "" {
		t.Error("bytes")
	}

	// datetime64
	now := time.Now()
	ds, ok := ToAny(Datetime(now)).(string)
	if !ok || ds == "" {
		t.Error("datetime64")
	}

	// uuid128
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	us, ok := ToAny(UUID128(uuid)).(string)
	if !ok || us == "" {
		t.Error("uuid128")
	}

	// array
	arr, ok := ToAny(Array(Int64(1), Int64(2))).([]any)
	if !ok || len(arr) != 2 {
		t.Error("array")
	}

	// object
	obj, ok := ToAny(Object(Member{Key: "x", Value: Int64(1)})).(map[string]any)
	if !ok || obj["x"] == nil {
		t.Error("object")
	}

	// tensor
	td := Tensor(DTypeFloat32, []uint64{2}, encodeFloat32LE([]float32{1.0, 2.0}))
	ta := ToAny(td)
	if ta == nil {
		t.Error("tensor")
	}

	// image
	img := Image(ImageFormatPNG, 10, 10, []byte{0xFF})
	ia := ToAny(img)
	if ia == nil {
		t.Error("image")
	}

	// audio
	aud := Audio(AudioEncodingPCMInt16, 44100, 1, []byte{0x00, 0x01})
	aa := ToAny(aud)
	if aa == nil {
		t.Error("audio")
	}

	// node (falls through to default in ToAny - returns nil)
	nd := Node("n1", []string{"Label"}, map[string]any{"k": "v"})
	_ = ToAny(nd) // exercises the code path

	// edge (falls through to default in ToAny - returns nil)
	ed := Edge("n1", "n2", "REL", nil)
	_ = ToAny(ed) // exercises the code path

	// bitmask (falls through to default in ToAny - returns nil)
	bm := BitmaskFromBools([]bool{true, false, true})
	_ = ToAny(bm) // exercises the code path

	// unknown ext
	ue := UnknownExtension(42, []byte{0x01})
	ua := ToAny(ue)
	if ua == nil {
		t.Error("unknown_ext")
	}
}

func TestToJSON_Coverage(t *testing.T) {
	v := Object(Member{Key: "x", Value: Int64(42)})
	data, err := ToJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestToJSONIndent_Coverage(t *testing.T) {
	v := Object(Member{Key: "x", Value: Int64(42)})
	data, err := ToJSONIndent(v, "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestFromJSON_Coverage(t *testing.T) {
	data := []byte(`{"x": 42, "y": "hello"}`)
	v, err := FromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type() != TypeObject {
		t.Error("expected object")
	}
}

func TestFromJSONEnriched_Coverage(t *testing.T) {
	data := []byte(`{"ts": "2024-01-01T00:00:00Z", "name": "test"}`)
	v, err := FromJSONEnriched(data)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type() != TypeObject {
		t.Error("expected object")
	}
}
