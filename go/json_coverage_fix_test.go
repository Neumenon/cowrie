package cowrie

import (
	"encoding/json"
	"testing"
)

// roundTripTyped runs Value -> ToAny -> JSON -> FromJSON and returns the result.
func roundTripTyped(t *testing.T, v *Value) *Value {
	t.Helper()
	b, err := json.Marshal(ToAny(v))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	return got
}

// TestJSONReconstruct_AllDtypes exercises stringToDtype for every byte-aligned
// dtype by round-tripping a rank-0 tensor of each through the JSON bridge.
func TestJSONReconstruct_AllDtypes(t *testing.T) {
	// Every dtype the JSON projection supports (dtypeToString/stringToDtype).
	// DTypeBool and the sub-byte quantized types have no JSON projection.
	dtypes := []DType{
		DTypeFloat32, DTypeFloat16, DTypeBFloat16, DTypeFloat64,
		DTypeInt8, DTypeInt16, DTypeInt32, DTypeInt64,
		DTypeUint8, DTypeUint16, DTypeUint32, DTypeUint64,
	}
	for _, dt := range dtypes {
		got := roundTripTyped(t, Tensor(dt, []uint64{}, nil))
		if got.typ != TypeTensor {
			t.Fatalf("dtype %v: got type %v, want tensor", dt, got.typ)
		}
		if got.tensorVal.DType != dt {
			t.Errorf("dtype round-trip: got %v want %v", got.tensorVal.DType, dt)
		}
	}
}

// TestJSONReconstruct_ImageFormats exercises stringToImageFormat.
func TestJSONReconstruct_ImageFormats(t *testing.T) {
	for _, f := range []ImageFormat{
		ImageFormatJPEG, ImageFormatPNG, ImageFormatWEBP, ImageFormatAVIF, ImageFormatBMP,
	} {
		got := roundTripTyped(t, Image(f, 2, 3, []byte{1, 2}))
		if got.typ != TypeImage || got.imageVal.Format != f {
			t.Errorf("image format %v round-trip failed (type %v)", f, got.typ)
		}
	}
}

// TestJSONReconstruct_AudioEncodings exercises stringToAudioEncoding.
func TestJSONReconstruct_AudioEncodings(t *testing.T) {
	for _, e := range []AudioEncoding{
		AudioEncodingPCMInt16, AudioEncodingPCMFloat32, AudioEncodingOPUS, AudioEncodingAAC,
	} {
		got := roundTripTyped(t, Audio(e, 8000, 1, []byte{1}))
		if got.typ != TypeAudio || got.audioVal.Encoding != e {
			t.Errorf("audio encoding %v round-trip failed (type %v)", e, got.typ)
		}
	}
}

// TestJSONReconstruct_RemainingTypes covers the tensor_ref/edge/batch branches.
func TestJSONReconstruct_RemainingTypes(t *testing.T) {
	if got := roundTripTyped(t, TensorRef(7, []byte{9, 9})); got.typ != TypeTensorRef {
		t.Errorf("tensor_ref: got %v", got.typ)
	}
	if got := roundTripTyped(t, Edge("a", "b", "KNOWS", map[string]any{"w": int64(1)})); got.typ != TypeEdge {
		t.Errorf("edge: got %v", got.typ)
	}
	if got := roundTripTyped(t, UnknownExtension(99, []byte{1, 2, 3})); got.typ != TypeUnknownExt {
		t.Errorf("unknown_ext: got %v", got.typ)
	}
	nb := NodeBatch([]NodeData{{ID: "n1", Labels: []string{"L"}, Props: map[string]any{"x": int64(1)}}})
	if got := roundTripTyped(t, nb); got.typ != TypeNodeBatch {
		t.Errorf("node_batch: got %v", got.typ)
	}
	eb := EdgeBatch([]EdgeData{{From: "a", To: "b", Type: "R", Props: map[string]any{"w": int64(2)}}})
	if got := roundTripTyped(t, eb); got.typ != TypeEdgeBatch {
		t.Errorf("edge_batch: got %v", got.typ)
	}
}

// TestJSONReconstruct_Malformed covers the nil-return (fall back to plain object)
// branches of tryReconstructTyped: missing required fields, bad enum strings, and
// unrecognized _type values must NOT be reconstructed as typed values.
func TestJSONReconstruct_Malformed(t *testing.T) {
	cases := []map[string]any{
		{"_type": "tensor"},
		{"_type": "tensor", "dtype": "nonsense", "dims": []any{}, "data": ""},
		{"_type": "image", "format": "png"},
		{"_type": "image", "format": "nonsense", "width": 1, "height": 1, "data": ""},
		{"_type": "audio"},
		{"_type": "audio", "encoding": "nonsense", "rate": 1, "channels": 1, "data": ""},
		{"_type": "tensor_ref"},
		{"_type": "node"},
		{"_type": "edge", "fromId": "a"},
		{"_type": "node_batch"},
		{"_type": "edge_batch"},
		{"_type": "bitmask"},
		{"_type": "unknown_ext"},
		{"_type": "totally_unknown"},
		{"_type": 123}, // _type not a string
	}
	for i, m := range cases {
		b, _ := json.Marshal(m)
		got, err := FromJSON(b)
		if err != nil {
			t.Fatalf("case %d FromJSON err: %v", i, err)
		}
		if got.typ != TypeObject {
			t.Errorf("malformed case %d (%v): expected plain object, got %v", i, m["_type"], got.typ)
		}
	}
}
