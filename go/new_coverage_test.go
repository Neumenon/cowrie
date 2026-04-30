package cowrie

import (
	"bytes"
	"testing"
)

func TestConstructors_ExtTypes(t *testing.T) {
	// TensorRef
	tr := TensorRef(1, []byte{0xAA, 0xBB})
	if tr.Type() != TypeTensorRef {
		t.Errorf("TensorRef type: %v", tr.Type())
	}
	trd := tr.TensorRef()
	if trd.StoreID != 1 || len(trd.Key) != 2 {
		t.Errorf("TensorRef data: %v", trd)
	}

	// Image
	img := Image(ImageFormatJPEG, 100, 200, []byte{0xFF, 0xFE})
	if img.Type() != TypeImage {
		t.Error("Image type")
	}
	imgd := img.Image()
	if imgd.Width != 100 || imgd.Height != 200 {
		t.Errorf("Image data: %v", imgd)
	}

	// Audio
	aud := Audio(0, 44100, 2, []byte{0x00, 0x01})
	if aud.Type() != TypeAudio {
		t.Error("Audio type")
	}
	audd := aud.Audio()
	if audd.SampleRate != 44100 || audd.Channels != 2 {
		t.Errorf("Audio data: %v", audd)
	}

	// UnknownExtension
	ue := UnknownExtension(999, []byte{0x01, 0x02})
	if ue.Type() != TypeUnknownExt {
		t.Error("UnknownExt type")
	}

	// ObjectFromMap
	m := map[string]*Value{
		"a": Int64(1),
		"b": String("hello"),
	}
	obj := ObjectFromMap(m)
	if obj.Type() != TypeObject {
		t.Error("ObjectFromMap type")
	}
	if obj.Get("a").Int64() != 1 {
		t.Error("ObjectFromMap get 'a'")
	}
}

func TestConstructors_Bitmask(t *testing.T) {
	// Bitmask from raw bits
	bm := Bitmask(5, []byte{0x1F})
	if bm.Type() != TypeBitmask {
		t.Error("Bitmask type")
	}
	bmd := bm.Bitmask()
	if bmd.Count != 5 {
		t.Errorf("Bitmask count: got %d", bmd.Count)
	}

	// BitmaskFromBools
	bools := []bool{true, false, true, true, false, false, true, false}
	bmb := BitmaskFromBools(bools)
	if bmb.Type() != TypeBitmask {
		t.Error("BitmaskFromBools type")
	}
	bmbd := bmb.Bitmask()
	if bmbd.Count != 8 {
		t.Errorf("BitmaskFromBools count: got %d", bmbd.Count)
	}
}

func TestConstructors_GraphTypes(t *testing.T) {
	// Node
	n := Node("n1", []string{"Person", "User"}, map[string]any{"name": "Alice"})
	if n.Type() != TypeNode {
		t.Error("Node type")
	}

	// Edge
	e := Edge("n1", "n2", "KNOWS", map[string]any{"since": "2020"})
	if e.Type() != TypeEdge {
		t.Error("Edge type")
	}

	// NodeBatch
	nb := NodeBatch([]NodeData{
		{ID: "n1", Labels: []string{"A"}},
		{ID: "n2", Labels: []string{"B"}},
	})
	if nb.Type() != TypeNodeBatch {
		t.Error("NodeBatch type")
	}

	// EdgeBatch
	eb := EdgeBatch([]EdgeData{
		{From: "n1", To: "n2", Type: "REL"},
	})
	if eb.Type() != TypeEdgeBatch {
		t.Error("EdgeBatch type")
	}

}

func TestEncodeDecode_ExtTypes(t *testing.T) {
	types := []struct {
		name string
		val  *Value
	}{
		{"tensor_ref", TensorRef(1, []byte{0xAA})},
		{"image", Image(ImageFormatPNG, 10, 10, []byte{0xFF})},
		{"audio", Audio(AudioEncodingPCMInt16, 44100, 1, []byte{0x00, 0x01})},
		{"bitmask", BitmaskFromBools([]bool{true, false, true})},
		{"node", Node("n1", []string{"A"}, map[string]any{"x": "y"})},
		{"edge", Edge("n1", "n2", "T", nil)},
		{"node_batch", NodeBatch([]NodeData{{ID: "n1", Labels: []string{"A"}}})},
		{"edge_batch", EdgeBatch([]EdgeData{{From: "n1", To: "n2", Type: "T"}})},
		{"unknown_ext", UnknownExtension(42, []byte{0x01})},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.val)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if decoded.Type() != tt.val.Type() {
				t.Errorf("type mismatch: got %v, want %v", decoded.Type(), tt.val.Type())
			}
		})
	}
}

func TestEncodeToWriter(t *testing.T) {
	val := Object(
		Member{Key: "x", Value: Int64(42)},
	)
	var buf bytes.Buffer
	err := EncodeToWriter(&buf, val)
	if err != nil {
		t.Fatalf("EncodeToWriter failed: %v", err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode from writer output failed: %v", err)
	}
	if decoded.Get("x").Int64() != 42 {
		t.Error("expected x=42")
	}
}

func TestEncodeToWriter_Tensor(t *testing.T) {
	// Test zero-copy tensor path
	val := Object(
		Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{3}, encodeFloat32LE([]float32{1.0, 2.0, 3.0}))},
	)
	var buf bytes.Buffer
	err := EncodeToWriter(&buf, val)
	if err != nil {
		t.Fatalf("EncodeToWriter with tensor failed: %v", err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Get("embedding").Type() != TypeTensor {
		t.Error("expected tensor type")
	}
}

func TestEncodeTo(t *testing.T) {
	val := Object(Member{Key: "y", Value: String("hello")})
	var buf bytes.Buffer
	err := EncodeTo(&buf, val)
	if err != nil {
		t.Fatalf("EncodeTo failed: %v", err)
	}
	decoded, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Get("y").String() != "hello" {
		t.Error("expected y=hello")
	}
}

func TestEncodeAppend(t *testing.T) {
	val := Object(Member{Key: "z", Value: Bool(true)})
	prefix := []byte{0xAA, 0xBB}
	data, err := EncodeAppend(prefix, val)
	if err != nil {
		t.Fatalf("EncodeAppend failed: %v", err)
	}
	// Should start with prefix bytes
	if data[0] != 0xAA || data[1] != 0xBB {
		t.Error("prefix not preserved")
	}
	// Remaining should be decodable
	decoded, err := Decode(data[2:])
	if err != nil {
		t.Fatalf("Decode after EncodeAppend failed: %v", err)
	}
	if !decoded.Get("z").Bool() {
		t.Error("expected z=true")
	}
}

func TestEncodeWithOptions(t *testing.T) {
	val := Object(
		Member{Key: "data", Value: String("test")},
		Member{Key: "nullfield", Value: Null()},
	)

	// Deterministic
	data, err := EncodeWithOptions(val, EncodeOptions{Deterministic: true})
	if err != nil {
		t.Fatalf("EncodeWithOptions deterministic failed: %v", err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Get("data").String() != "test" {
		t.Error("expected data=test")
	}

	// OmitNull
	data2, err := EncodeWithOptions(val, EncodeOptions{OmitNull: true})
	if err != nil {
		t.Fatalf("EncodeWithOptions omit-null failed: %v", err)
	}
	decoded2, err := Decode(data2)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded2.Get("nullfield") != nil {
		t.Error("expected null field to be omitted")
	}
}
