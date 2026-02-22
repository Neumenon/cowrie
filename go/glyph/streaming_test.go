package glyph

import (
	"testing"
)

func TestNewStreamDict(t *testing.T) {
	opts := StreamDictOptions{
		MaxEntries: 1000,
		SessionID:  12345,
	}
	d := NewStreamDict(opts)

	if d.SessionID() != 12345 {
		t.Errorf("expected sessionID 12345, got %d", d.SessionID())
	}
	if d.Len() != 0 {
		t.Errorf("expected len 0, got %d", d.Len())
	}
	if d.IsFrozen() {
		t.Error("new dict should not be frozen")
	}
}

func TestStreamDict_Add(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())

	idx1 := d.Add("hello")
	if idx1 != 0 {
		t.Errorf("first Add should return 0, got %d", idx1)
	}

	idx2 := d.Add("world")
	if idx2 != 1 {
		t.Errorf("second Add should return 1, got %d", idx2)
	}

	// Adding same key should return existing index
	idx3 := d.Add("hello")
	if idx3 != 0 {
		t.Errorf("duplicate Add should return 0, got %d", idx3)
	}

	if d.Len() != 2 {
		t.Errorf("expected len 2, got %d", d.Len())
	}
}

func TestStreamDict_Lookup(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())
	d.Add("hello")
	d.Add("world")

	if d.Lookup("hello") != 0 {
		t.Error("Lookup(hello) should return 0")
	}
	if d.Lookup("world") != 1 {
		t.Error("Lookup(world) should return 1")
	}
	if d.Lookup("unknown") != -1 {
		t.Error("Lookup(unknown) should return -1")
	}
}

func TestStreamDict_Get(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())
	d.Add("hello")
	d.Add("world")

	if d.Get(0) != "hello" {
		t.Errorf("Get(0) = %q, expected 'hello'", d.Get(0))
	}
	if d.Get(1) != "world" {
		t.Errorf("Get(1) = %q, expected 'world'", d.Get(1))
	}
	if d.Get(100) != "" {
		t.Errorf("Get(100) = %q, expected ''", d.Get(100))
	}
}

func TestStreamDict_Freeze(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())
	d.Add("hello")

	d.Freeze()

	if !d.IsFrozen() {
		t.Error("expected frozen")
	}

	// Should not add when frozen
	idx := d.Add("world")
	if idx != 0xFFFF {
		t.Errorf("expected 0xFFFF when frozen, got %d", idx)
	}

	if d.Len() != 1 {
		t.Errorf("expected len 1, got %d", d.Len())
	}

	d.Unfreeze()

	if d.IsFrozen() {
		t.Error("expected not frozen after Unfreeze")
	}

	idx = d.Add("world")
	if idx != 1 {
		t.Errorf("expected 1 after unfreeze, got %d", idx)
	}
}

func TestStreamDict_Encode(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())
	d.Add("hello")
	d.Add("world")

	idx, ok := d.Encode("hello")
	if !ok || idx != 0 {
		t.Errorf("Encode(hello) = (%d, %v), expected (0, true)", idx, ok)
	}

	idx, ok = d.Encode("unknown")
	if ok {
		t.Errorf("Encode(unknown) = (%d, %v), expected (_, false)", idx, ok)
	}
}

func TestStreamDict_EncodeOrAdd(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())

	idx, ok := d.EncodeOrAdd("hello")
	if !ok || idx != 0 {
		t.Errorf("EncodeOrAdd(hello) = (%d, %v), expected (0, true)", idx, ok)
	}

	idx, ok = d.EncodeOrAdd("hello")
	if !ok || idx != 0 {
		t.Errorf("EncodeOrAdd(hello) again = (%d, %v), expected (0, true)", idx, ok)
	}

	idx, ok = d.EncodeOrAdd("world")
	if !ok || idx != 1 {
		t.Errorf("EncodeOrAdd(world) = (%d, %v), expected (1, true)", idx, ok)
	}
}

func TestStreamDict_MaxEntries(t *testing.T) {
	opts := StreamDictOptions{MaxEntries: 3}
	d := NewStreamDict(opts)

	d.Add("a")
	d.Add("b")
	d.Add("c")

	idx := d.Add("d")
	if idx != 0xFFFF {
		t.Errorf("expected 0xFFFF when full, got %d", idx)
	}
}

func TestStreamDict_Serialize(t *testing.T) {
	d := NewStreamDict(StreamDictOptions{SessionID: 12345})
	d.Add("hello")
	d.Add("world")
	d.Add("test")

	data := d.Serialize()

	// Check magic
	if string(data[0:4]) != "GDCT" {
		t.Errorf("wrong magic: %v", data[0:4])
	}

	// Deserialize
	d2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}

	if d2.SessionID() != 12345 {
		t.Errorf("sessionID mismatch: %d", d2.SessionID())
	}
	if d2.Len() != 3 {
		t.Errorf("len mismatch: %d", d2.Len())
	}
	if d2.Get(0) != "hello" {
		t.Errorf("entry 0 mismatch: %q", d2.Get(0))
	}
	if d2.Get(1) != "world" {
		t.Errorf("entry 1 mismatch: %q", d2.Get(1))
	}
	if d2.Get(2) != "test" {
		t.Errorf("entry 2 mismatch: %q", d2.Get(2))
	}
}

func TestDeserialize_Invalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{1, 2, 3}},
		{"wrong_magic", []byte{'X', 'Y', 'Z', 'W', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Deserialize(tc.data)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestNewStreamSession(t *testing.T) {
	opts := SessionOptions{
		SessionID:   12345,
		LearnFrames: 5,
	}
	s := NewStreamSession(opts)

	if s.SessionID() != 12345 {
		t.Errorf("expected sessionID 12345, got %d", s.SessionID())
	}
	if !s.IsLearning() {
		t.Error("new session should be in learning mode")
	}
}

func TestStreamSession_LearnKeys(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 10})

	v := Map(
		FieldVal("name", Str("test")),
		FieldVal("value", Int(42)),
	)

	s.LearnKeys(v)

	dict := s.Dict()
	if dict.Lookup("name") < 0 {
		t.Error("expected 'name' to be learned")
	}
	if dict.Lookup("value") < 0 {
		t.Error("expected 'value' to be learned")
	}
}

func TestStreamSession_LearnKeys_Nested(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 10})

	v := Map(
		FieldVal("outer", Map(
			FieldVal("inner", Str("test")),
		)),
		FieldVal("list", List(
			Map(FieldVal("item", Int(1))),
		)),
	)

	s.LearnKeys(v)

	dict := s.Dict()
	if dict.Lookup("outer") < 0 {
		t.Error("expected 'outer' to be learned")
	}
	if dict.Lookup("inner") < 0 {
		t.Error("expected 'inner' to be learned")
	}
	if dict.Lookup("item") < 0 {
		t.Error("expected 'item' to be learned")
	}
}

func TestStreamSession_NextSeq(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 3})

	if s.NextSeq() != 0 {
		t.Error("first seq should be 0")
	}
	if s.NextSeq() != 1 {
		t.Error("second seq should be 1")
	}
	if s.NextSeq() != 2 {
		t.Error("third seq should be 2")
	}

	// After LearnFrames, should no longer be learning
	if s.IsLearning() {
		t.Error("should not be learning after LearnFrames")
	}
}

func TestStreamSession_EncodeKey(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 10})

	// During learning, should add keys
	idx, ok := s.EncodeKey("hello")
	if !ok {
		t.Error("expected key to be added during learning")
	}
	if idx != 0 {
		t.Errorf("expected index 0, got %d", idx)
	}

	// Same key should return same index
	idx, ok = s.EncodeKey("hello")
	if !ok || idx != 0 {
		t.Errorf("expected (0, true), got (%d, %v)", idx, ok)
	}
}

func TestStreamSession_SaveDict(t *testing.T) {
	s := NewStreamSession(SessionOptions{SessionID: 999})
	s.EncodeKey("key1")
	s.EncodeKey("key2")

	data := s.SaveDict()

	// Should be valid serialized dict
	d, err := Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}

	if d.Len() != 2 {
		t.Errorf("expected 2 entries, got %d", d.Len())
	}
}

func TestEncodeDictFrame(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 10})

	v := Map(
		FieldVal("name", Str("test")),
		FieldVal("count", Int(42)),
	)

	frame := EncodeDictFrame(v, s)

	// Should have some data
	if len(frame) < 10 {
		t.Errorf("frame too short: %d bytes", len(frame))
	}

	// Check flags
	flags := frame[0]
	if flags&FrameFlagCompact == 0 {
		t.Error("expected compact flag")
	}

	t.Logf("Frame size: %d bytes", len(frame))
}

func TestEncodeDictFrame_MultipleCalls(t *testing.T) {
	s := NewStreamSession(SessionOptions{LearnFrames: 10})

	v1 := Map(FieldVal("key1", Str("value1")))
	v2 := Map(FieldVal("key1", Str("value2")), FieldVal("key2", Int(2)))

	frame1 := EncodeDictFrame(v1, s)
	frame2 := EncodeDictFrame(v2, s)

	// Second frame should potentially be smaller if key1 is cached
	t.Logf("Frame1: %d bytes, Frame2: %d bytes", len(frame1), len(frame2))

	// Dict should have both keys
	dict := s.Dict()
	if dict.Lookup("key1") < 0 || dict.Lookup("key2") < 0 {
		t.Error("dict should contain both keys")
	}
}

func TestStreamDict_Version(t *testing.T) {
	d := NewStreamDict(DefaultStreamDictOptions())

	v1 := d.Version()
	d.Add("hello")
	v2 := d.Version()
	d.Add("world")
	v3 := d.Version()

	if v2 <= v1 || v3 <= v2 {
		t.Errorf("version should increment: %d, %d, %d", v1, v2, v3)
	}
}

func TestStreamSession_InitialDict(t *testing.T) {
	// Pre-build a dictionary
	preDict := NewStreamDict(StreamDictOptions{SessionID: 100})
	preDict.Add("preloaded1")
	preDict.Add("preloaded2")

	// Create session with initial dict
	s := NewStreamSession(SessionOptions{
		InitialDict: preDict,
	})

	dict := s.Dict()
	if dict.Lookup("preloaded1") < 0 {
		t.Error("expected preloaded1 in dict")
	}
	if dict.Lookup("preloaded2") < 0 {
		t.Error("expected preloaded2 in dict")
	}
}

func TestStreamDictOptions_Defaults(t *testing.T) {
	opts := DefaultStreamDictOptions()

	if opts.MaxEntries != 4096 {
		t.Errorf("expected MaxEntries 4096, got %d", opts.MaxEntries)
	}
}

func TestStreamDict_PreloadKeys(t *testing.T) {
	opts := StreamDictOptions{
		PreloadKeys: []string{"a", "b", "c"},
	}
	d := NewStreamDict(opts)

	if d.Len() != 3 {
		t.Errorf("expected 3 preloaded keys, got %d", d.Len())
	}
	if d.Get(0) != "a" || d.Get(1) != "b" || d.Get(2) != "c" {
		t.Error("preloaded keys mismatch")
	}
}

// Benchmarks

func BenchmarkStreamDict_Add(b *testing.B) {
	d := NewStreamDict(DefaultStreamDictOptions())
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = string(rune(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Add(keys[i%len(keys)])
	}
}

func BenchmarkStreamDict_Encode(b *testing.B) {
	d := NewStreamDict(DefaultStreamDictOptions())
	for i := 0; i < 100; i++ {
		d.Add(string(rune(i)))
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		d.Add(k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Encode(keys[i%len(keys)])
	}
}

func BenchmarkStreamDict_Serialize(b *testing.B) {
	d := NewStreamDict(DefaultStreamDictOptions())
	for i := 0; i < 100; i++ {
		d.Add(string(rune('a' + i%26)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Serialize()
	}
}

func BenchmarkEncodeDictFrame(b *testing.B) {
	s := NewStreamSession(SessionOptions{LearnFrames: 100})

	v := Map(
		FieldVal("name", Str("test")),
		FieldVal("value", Int(42)),
		FieldVal("nested", Map(
			FieldVal("a", Int(1)),
			FieldVal("b", Int(2)),
		)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDictFrame(v, s)
	}
}
