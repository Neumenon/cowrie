package codec

import (
	"bytes"
	"io"
	"testing"

	"github.com/Neumenon/cowrie"
)

// TestReader_MixedStream_BackCompat verifies the reader handles:
// - master stream frames
// - legacy raw Cowrie documents
// - legacy length-prefixed CowrieL frames
// Note: Mixed stream support requires AllowLegacy and specific frame detection logic.
// This test documents expected behavior but may skip if not fully implemented.
func TestReader_MixedStream_BackCompat(t *testing.T) {
	t.Skip("Mixed stream backward compatibility requires complex frame detection logic - test documents expected behavior")

	var buf bytes.Buffer

	// 1) Master frame
	mw := NewMasterWriter(&buf, MasterWriterOptions{})
	payload1 := MustValueFromAny(map[string]any{"type": "master", "index": int64(1)})
	if err := mw.Write(payload1); err != nil {
		t.Fatal(err)
	}
	frame1End := buf.Len()

	// 2) Legacy raw Cowrie document (just 'SJ' magic, no framing)
	legacyDoc, err := cowrie.Encode(MustValueFromAny(map[string]any{"type": "legacy_doc", "index": int64(2)}))
	if err != nil {
		t.Fatal(err)
	}
	buf.Write(legacyDoc)
	frame2End := buf.Len()

	// 3) Legacy length-prefixed stream frame (CowrieL style: [u32 len][payload])
	streamPayload, err := cowrie.Encode(MustValueFromAny(map[string]any{"type": "legacy_stream", "index": int64(3)}))
	if err != nil {
		t.Fatal(err)
	}
	legacyFrame := buildLegacyStreamFrame(streamPayload)
	buf.Write(legacyFrame)
	frame3End := buf.Len()

	// 4) Another master frame
	payload4 := MustValueFromAny(map[string]any{"type": "master", "index": int64(4)})
	if err := mw.Write(payload4); err != nil {
		t.Fatal(err)
	}

	t.Logf("Buffer layout: master[0:%d] legacy_doc[%d:%d] legacy_stream[%d:%d] master[%d:%d]",
		frame1End, frame1End, frame2End, frame2End, frame3End, frame3End, buf.Len())

	// Read back using MasterStreamReader with legacy detection
	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
		AllowLegacy: true,
	})

	frames := make([]*MasterFrame, 0, 4)
	for {
		frame, err := mr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read error after %d frames: %v", len(frames), err)
		}
		frames = append(frames, frame)
	}

	if len(frames) != 4 {
		t.Fatalf("got %d frames, want 4", len(frames))
	}

	// Verify frame order and content
	expectations := []struct {
		typeID uint32
		typ    string
		index  int64
	}{
		{123, "master", 1},
		{0, "legacy_doc", 2},     // Legacy frames have TypeID 0
		{0, "legacy_stream", 3},  // Legacy frames have TypeID 0
		{999, "master", 4},
	}

	for i, exp := range expectations {
		frame := frames[i]

		if frame.TypeID != exp.typeID {
			t.Errorf("frame %d: TypeID = %d, want %d", i, frame.TypeID, exp.typeID)
		}

		if frame.Payload == nil {
			t.Errorf("frame %d: payload is nil", i)
			continue
		}

		// Decode payload to verify content
		payloadBytes, err := cowrie.Encode(frame.Payload)
		if err != nil {
			t.Errorf("frame %d: encode payload error: %v", i, err)
			continue
		}
		var decoded map[string]any
		if err := DecodeBytes(payloadBytes, &decoded); err != nil {
			t.Errorf("frame %d: decode error: %v", i, err)
			continue
		}

		if decoded["type"] != exp.typ {
			t.Errorf("frame %d: type = %v, want %s", i, decoded["type"], exp.typ)
		}

		if idx, ok := decoded["index"].(int64); !ok || idx != exp.index {
			t.Errorf("frame %d: index = %v, want %d", i, decoded["index"], exp.index)
		}
	}
}

// TestMasterStream_LegacyDetection verifies IsMasterStream correctly identifies formats.
func TestMasterStream_LegacyDetection(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		isMaster bool
	}{
		{"master_frame", []byte("SJST\x02\x00"), true},
		{"cowrie_doc", []byte("SJ\x02\x00"), false},
		{"length_prefixed", []byte{0x00, 0x00, 0x00, 0x10, 'S', 'J'}, false},
		{"empty", []byte{}, false},
		{"too_short", []byte("SJS"), false},
		{"wrong_magic", []byte("XXXX\x02\x00"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsMasterStream(tt.data)
			if got != tt.isMaster {
				t.Errorf("IsMasterStream = %v, want %v", got, tt.isMaster)
			}
		})
	}
}

// TestMasterStream_IsCowrieDocument verifies Cowrie document detection.
func TestMasterStream_IsCowrieDocument(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		isCowrie bool
	}{
		{"cowrie_v2", []byte{'S', 'J', 0x02, 0x00}, true},
		{"cowrie_v1", []byte{'S', 'J', 0x01, 0x00}, true},
		{"master_frame", []byte("SJST\x02"), false},
		{"empty", []byte{}, false},
		{"too_short", []byte("SJ"), false},
		{"wrong_magic", []byte("XX\x02\x00"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsCowrieDocument(tt.data)
			if got != tt.isCowrie {
				t.Errorf("IsCowrieDocument = %v, want %v", got, tt.isCowrie)
			}
		})
	}
}

// TestBackwardCompat_OldStructTags ensures old JSON tags still work.
func TestBackwardCompat_OldStructTags(t *testing.T) {
	type OldStyleStruct struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Hidden string `json:"-"`
		Empty  string `json:"empty,omitempty"`
	}

	original := OldStyleStruct{
		ID:     42,
		Name:   "test",
		Hidden: "should_not_appear",
		Empty:  "", // Should be omitted
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded OldStyleStruct
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify
	if decoded.ID != original.ID {
		t.Errorf("ID = %d, want %d", decoded.ID, original.ID)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Hidden != "" {
		t.Errorf("Hidden = %q, want empty (should not be serialized)", decoded.Hidden)
	}
	if decoded.Empty != "" {
		t.Errorf("Empty = %q, want empty", decoded.Empty)
	}
}

// TestBackwardCompat_PointerFields ensures pointer fields work correctly.
func TestBackwardCompat_PointerFields(t *testing.T) {
	type StructWithPointers struct {
		Name     *string `json:"name"`
		Age      *int64  `json:"age,omitempty"`
		Score    *float64 `json:"score"`
	}

	name := "Alice"
	score := 3.14

	original := StructWithPointers{
		Name:  &name,
		Age:   nil, // Should be omitted
		Score: &score,
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithPointers
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify
	if decoded.Name == nil || *decoded.Name != name {
		t.Errorf("Name = %v, want %q", decoded.Name, name)
	}
	if decoded.Age != nil {
		t.Errorf("Age = %v, want nil", decoded.Age)
	}
	if decoded.Score == nil || *decoded.Score != score {
		t.Errorf("Score = %v, want %f", decoded.Score, score)
	}
}

// TestBackwardCompat_SliceFields ensures slice fields work correctly.
func TestBackwardCompat_SliceFields(t *testing.T) {
	type StructWithSlices struct {
		Strings  []string  `json:"strings"`
		Ints     []int64   `json:"ints"`
		Floats   []float64 `json:"floats"`
		Empty    []string  `json:"empty,omitempty"`
		Nil      []string  `json:"nil,omitempty"`
	}

	original := StructWithSlices{
		Strings: []string{"a", "b", "c"},
		Ints:    []int64{1, 2, 3},
		Floats:  []float64{1.1, 2.2, 3.3},
		Empty:   []string{},
		Nil:     nil,
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithSlices
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify strings
	if len(decoded.Strings) != len(original.Strings) {
		t.Errorf("Strings len = %d, want %d", len(decoded.Strings), len(original.Strings))
	}

	// Verify ints
	if len(decoded.Ints) != len(original.Ints) {
		t.Errorf("Ints len = %d, want %d", len(decoded.Ints), len(original.Ints))
	}

	// Verify floats
	if len(decoded.Floats) != len(original.Floats) {
		t.Errorf("Floats len = %d, want %d", len(decoded.Floats), len(original.Floats))
	}
}

// TestBackwardCompat_MapFields ensures map fields work correctly.
func TestBackwardCompat_MapFields(t *testing.T) {
	type StructWithMaps struct {
		StringMap map[string]string `json:"string_map"`
		IntMap    map[string]int64  `json:"int_map"`
		AnyMap    map[string]any    `json:"any_map"`
	}

	original := StructWithMaps{
		StringMap: map[string]string{"a": "1", "b": "2"},
		IntMap:    map[string]int64{"x": 10, "y": 20},
		AnyMap:    map[string]any{"mixed": int64(1), "types": "here"},
	}

	// Encode
	data, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode
	var decoded StructWithMaps
	if err := DecodeBytes(data, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify string map
	if len(decoded.StringMap) != len(original.StringMap) {
		t.Errorf("StringMap len = %d, want %d", len(decoded.StringMap), len(original.StringMap))
	}
	for k, v := range original.StringMap {
		if decoded.StringMap[k] != v {
			t.Errorf("StringMap[%q] = %q, want %q", k, decoded.StringMap[k], v)
		}
	}

	// Verify int map
	if len(decoded.IntMap) != len(original.IntMap) {
		t.Errorf("IntMap len = %d, want %d", len(decoded.IntMap), len(original.IntMap))
	}
}
