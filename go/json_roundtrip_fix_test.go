package cowrie

import (
	"encoding/json"
	"math/big"
	"testing"
)

// TestJSONRoundTrip_Tensor verifies that a Tensor value survives ToAny ->
// json.Marshal -> FromJSON with the correct type and key fields intact.
func TestJSONRoundTrip_Tensor(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	dims := []uint64{2, 2}
	orig := Tensor(DTypeFloat32, dims, data)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeTensor {
		t.Fatalf("expected TypeTensor, got type %v (raw JSON: %s)", got.typ, b)
	}
	td := got.tensorVal
	if td.DType != DTypeFloat32 {
		t.Errorf("dtype: want %v, got %v", DTypeFloat32, td.DType)
	}
	if len(td.Dims) != 2 || td.Dims[0] != 2 || td.Dims[1] != 2 {
		t.Errorf("dims: want [2,2], got %v", td.Dims)
	}
	if len(td.Data) != 4 || td.Data[0] != 0x01 || td.Data[3] != 0x04 {
		t.Errorf("data mismatch: got %v", td.Data)
	}
}

// TestJSONRoundTrip_Image verifies Image round-trips through JSON.
func TestJSONRoundTrip_Image(t *testing.T) {
	imgData := []byte{0xFF, 0xD8, 0xFF, 0xE0}
	orig := Image(ImageFormatJPEG, 320, 240, imgData)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeImage {
		t.Fatalf("expected TypeImage, got type %v (raw JSON: %s)", got.typ, b)
	}
	img := got.imageVal
	if img.Format != ImageFormatJPEG {
		t.Errorf("format: want JPEG, got %v", img.Format)
	}
	if img.Width != 320 || img.Height != 240 {
		t.Errorf("dimensions: want 320x240, got %dx%d", img.Width, img.Height)
	}
	if len(img.Data) != 4 || img.Data[0] != 0xFF {
		t.Errorf("data mismatch: got %v", img.Data)
	}
}

// TestJSONRoundTrip_Audio verifies Audio round-trips through JSON.
func TestJSONRoundTrip_Audio(t *testing.T) {
	audioData := []byte{0x00, 0x01, 0x00, 0x02}
	orig := Audio(AudioEncodingPCMInt16, 44100, 2, audioData)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeAudio {
		t.Fatalf("expected TypeAudio, got type %v (raw JSON: %s)", got.typ, b)
	}
	aud := got.audioVal
	if aud.Encoding != AudioEncodingPCMInt16 {
		t.Errorf("encoding: want PCMInt16, got %v", aud.Encoding)
	}
	if aud.SampleRate != 44100 {
		t.Errorf("sample rate: want 44100, got %v", aud.SampleRate)
	}
	if aud.Channels != 2 {
		t.Errorf("channels: want 2, got %v", aud.Channels)
	}
}

// TestJSONRoundTrip_Node verifies Node round-trips through JSON.
func TestJSONRoundTrip_Node(t *testing.T) {
	orig := Node("node-1", []string{"Person", "User"}, map[string]any{"age": 42})

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeNode {
		t.Fatalf("expected TypeNode, got type %v (raw JSON: %s)", got.typ, b)
	}
	nd := got.nodeVal
	if nd.ID != "node-1" {
		t.Errorf("id: want node-1, got %q", nd.ID)
	}
	if len(nd.Labels) != 2 || nd.Labels[0] != "Person" || nd.Labels[1] != "User" {
		t.Errorf("labels: want [Person User], got %v", nd.Labels)
	}
}

// TestJSONRoundTrip_Bitmask verifies Bitmask round-trips through JSON.
func TestJSONRoundTrip_Bitmask(t *testing.T) {
	// 3 bits: true, false, true → bits byte = 0b00000101 = 0x05
	orig := Bitmask(3, []byte{0x05})

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeBitmask {
		t.Fatalf("expected TypeBitmask, got type %v (raw JSON: %s)", got.typ, b)
	}
	bm := got.bitmaskVal
	if bm.Count != 3 {
		t.Errorf("count: want 3, got %v", bm.Count)
	}
	if len(bm.Bits) != 1 || bm.Bits[0] != 0x05 {
		t.Errorf("bits: want [0x05], got %v", bm.Bits)
	}
}

// TestJSONRoundTrip_UnknownExt verifies UnknownExtension round-trips through JSON.
func TestJSONRoundTrip_UnknownExt(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	orig := UnknownExtension(42, payload)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeUnknownExt {
		t.Fatalf("expected TypeUnknownExt, got type %v (raw JSON: %s)", got.typ, b)
	}
	ext := got.unknownExtVal
	if ext.ExtType != 42 {
		t.Errorf("ext_type: want 42, got %v", ext.ExtType)
	}
	if len(ext.Payload) != 4 || ext.Payload[0] != 0xDE || ext.Payload[3] != 0xEF {
		t.Errorf("payload mismatch: got %v", ext.Payload)
	}
}

// TestJSONRoundTrip_BigInt verifies that integers larger than 2^53 are preserved
// exactly through the JSON bridge without float64 precision loss.
func TestJSONRoundTrip_BigInt(t *testing.T) {
	// 2^53 + 1 = 9007199254740993 — one beyond JS safe integer range.
	// As a plain int64 value it should round-trip exactly.
	large := int64(9007199254740993)
	orig := Int64(large)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	// ToAny emits an out-of-safe-range int64 as a decimal string, so FromJSON
	// parses it back as an exact string (no float64 precision loss).
	if got.typ != TypeString || got.stringVal != "9007199254740993" {
		t.Fatalf("large int64 round-trip: got typ=%v val=%q", got.typ, got.stringVal)
	}

	// Also test with a BigInt Value, which always serialises as a string.
	bi := new(big.Int)
	bi.SetInt64(large)
	origBig := BigInt(bigIntToTwosComplement(bi))

	b2, err := json.Marshal(ToAny(origBig))
	if err != nil {
		t.Fatalf("json.Marshal BigInt: %v", err)
	}

	got2, err := FromJSON(b2)
	if err != nil {
		t.Fatalf("FromJSON BigInt: %v", err)
	}
	// The value should be a string (the bigint decimal representation).
	if got2.typ != TypeString {
		t.Fatalf("expected TypeString for BigInt projection, got %v", got2.typ)
	}
	if got2.stringVal != bi.String() {
		t.Errorf("BigInt round-trip: want %q, got %q", bi.String(), got2.stringVal)
	}

	// More importantly: a raw JSON number beyond 2^53 should not lose precision.
	// Inject a raw large-number JSON and check that FromJSON parses it exactly.
	rawJSON := []byte(`9007199254740993`)
	gotRaw, err := FromJSON(rawJSON)
	if err != nil {
		t.Fatalf("FromJSON large number: %v", err)
	}
	// With UseNumber the value lands as Int64 (fits int64).
	if gotRaw.typ != TypeInt64 {
		t.Fatalf("expected TypeInt64 for large number, got type %v", gotRaw.typ)
	}
	if gotRaw.int64Val != large {
		t.Errorf("large int precision: want %d, got %d", large, gotRaw.int64Val)
	}
}

// TestJSONRoundTrip_PlainObjectNotCorrupted ensures that a plain object
// without a _type key is not mistakenly reconstructed as a typed value.
func TestJSONRoundTrip_PlainObjectNotCorrupted(t *testing.T) {
	orig := Object(
		Member{Key: "foo", Value: String("bar")},
		Member{Key: "count", Value: Int64(7)},
	)

	b, err := json.Marshal(ToAny(orig))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, err := FromJSON(b)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.typ != TypeObject {
		t.Fatalf("expected TypeObject, got %v (raw JSON: %s)", got.typ, b)
	}
}

// TestJSONRoundTrip_UnknownTypeNotCorrupted ensures that an object with an
// unrecognized _type string is kept as a plain object.
func TestJSONRoundTrip_UnknownTypeNotCorrupted(t *testing.T) {
	rawJSON := []byte(`{"_type":"future_type","value":42}`)

	got, err := FromJSON(rawJSON)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	// Must remain a plain object, not crash or reconstruct incorrectly.
	if got.typ != TypeObject {
		t.Fatalf("expected TypeObject for unknown _type, got %v", got.typ)
	}
}
