package codec

import (
	"bytes"
	"io"
	"testing"

	"github.com/Neumenon/cowrie/go"
)

func TestMasterStreamRoundTrip(t *testing.T) {
	data := map[string]any{
		"id":     int64(12345),
		"name":   "test",
		"values": []float32{1.0, 2.0, 3.0},
	}

	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	writer := NewMasterWriter(&buf, opts)

	err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read it back
	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if frame.Payload == nil {
		t.Fatal("Payload is nil")
	}

	// Check values
	if frame.Payload.Type() != cowrie.TypeObject {
		t.Errorf("Expected object, got %v", frame.Payload.Type())
	}

	idVal := frame.Payload.Get("id")
	if idVal == nil || idVal.Int64() != 12345 {
		t.Errorf("id mismatch: got %v", idVal)
	}

	nameVal := frame.Payload.Get("name")
	if nameVal == nil || nameVal.String() != "test" {
		t.Errorf("name mismatch: got %v", nameVal)
	}
}

func TestMasterStreamWithMeta(t *testing.T) {
	data := map[string]any{"content": "test"}
	meta := cowrie.Object(
		cowrie.Member{Key: "source", Value: cowrie.String("test-source")},
		cowrie.Member{Key: "timestamp", Value: cowrie.Int64(1234567890)},
	)

	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	writer := NewMasterWriter(&buf, opts)

	err := writer.WriteWithMeta(data, meta)
	if err != nil {
		t.Fatalf("WriteWithMeta failed: %v", err)
	}

	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Check metadata
	if frame.Meta == nil {
		t.Fatal("Meta is nil")
	}

	sourceVal := frame.Meta.Get("source")
	if sourceVal == nil || sourceVal.String() != "test-source" {
		t.Errorf("meta.source mismatch: got %v", sourceVal)
	}

	if frame.Header.Flags&FlagMasterMeta == 0 {
		t.Error("FlagMasterMeta not set")
	}
}

func TestMasterStreamCompression(t *testing.T) {
	// Create a large payload to trigger compression
	data := make(map[string]any)
	for i := 0; i < 100; i++ {
		data["field"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "value that repeats"
	}

	var buf bytes.Buffer
	opts := MasterWriterOptions{
		Deterministic: true,
		Compression:   cowrie.CompressionZstd,
		EnableCRC:     true,
	}
	writer := NewMasterWriter(&buf, opts)

	err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Verify flags
	if frame.Header.Flags&FlagMasterCompressed == 0 {
		t.Error("Expected compression flag to be set")
	}

	if frame.Header.Flags&FlagMasterCRC == 0 {
		t.Error("Expected CRC flag to be set")
	}

	// Verify payload decoded correctly
	if frame.Payload.Type() != cowrie.TypeObject {
		t.Errorf("Expected object, got %v", frame.Payload.Type())
	}
}

func TestMasterStreamCRCVerification(t *testing.T) {
	data := map[string]any{"test": "data"}

	var buf bytes.Buffer
	opts := MasterWriterOptions{
		EnableCRC: true,
	}
	writer := NewMasterWriter(&buf, opts)

	err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Corrupt the data
	rawData := buf.Bytes()
	if len(rawData) > 20 {
		rawData[20] ^= 0xFF // Flip some bits
	}

	reader := NewMasterReader(rawData, DefaultMasterReaderOptions())
	_, err = reader.Next()

	if err != ErrMasterCRCMismatch {
		t.Errorf("Expected CRC mismatch error, got: %v", err)
	}
}

func TestMasterStreamMultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	writer := NewMasterWriter(&buf, opts)

	frames := []map[string]any{
		{"id": int64(1), "type": "first"},
		{"id": int64(2), "type": "second"},
		{"id": int64(3), "type": "third"},
	}

	for _, f := range frames {
		if err := writer.Write(f); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
	count := 0

	for {
		frame, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		idVal := frame.Payload.Get("id")
		if idVal.Int64() != int64(count+1) {
			t.Errorf("Frame %d: expected id=%d, got %d", count, count+1, idVal.Int64())
		}
		count++
	}

	if count != len(frames) {
		t.Errorf("Expected %d frames, got %d", len(frames), count)
	}
}

func TestMasterStreamTypeID(t *testing.T) {
	data1 := map[string]any{
		"name": "test",
		"age":  int64(30),
	}

	data2 := map[string]any{
		"name": "other",
		"age":  int64(25),
	}

	var buf1, buf2 bytes.Buffer
	opts := DefaultMasterWriterOptions()

	writer1 := NewMasterWriter(&buf1, opts)
	writer2 := NewMasterWriter(&buf2, opts)

	_ = writer1.Write(data1)
	_ = writer2.Write(data2)

	reader1 := NewMasterReader(buf1.Bytes(), DefaultMasterReaderOptions())
	reader2 := NewMasterReader(buf2.Bytes(), DefaultMasterReaderOptions())

	frame1, _ := reader1.Next()
	frame2, _ := reader2.Next()

	// Same schema should have same TypeID
	if frame1.TypeID != frame2.TypeID {
		t.Errorf("Same schema should have same TypeID: %x vs %x", frame1.TypeID, frame2.TypeID)
	}
}

func TestMasterStreamDifferentTypeID(t *testing.T) {
	data1 := map[string]any{
		"name": "test",
	}

	data2 := map[string]any{
		"id": int64(1),
	}

	var buf1, buf2 bytes.Buffer
	opts := DefaultMasterWriterOptions()

	writer1 := NewMasterWriter(&buf1, opts)
	writer2 := NewMasterWriter(&buf2, opts)

	_ = writer1.Write(data1)
	_ = writer2.Write(data2)

	reader1 := NewMasterReader(buf1.Bytes(), DefaultMasterReaderOptions())
	reader2 := NewMasterReader(buf2.Bytes(), DefaultMasterReaderOptions())

	frame1, _ := reader1.Next()
	frame2, _ := reader2.Next()

	// Different schema should have different TypeID
	if frame1.TypeID == frame2.TypeID {
		t.Error("Different schema should have different TypeID")
	}
}

func TestMasterStreamLegacyDetection(t *testing.T) {
	// Create a legacy stream format [u32 len][payload]
	data := map[string]any{"legacy": "data"}
	encoded, err := EncodeBytes(data)
	if err != nil {
		t.Fatalf("EncodeBytes failed: %v", err)
	}

	// Build legacy format
	legacy := make([]byte, 4+len(encoded))
	legacy[0] = byte(len(encoded))
	legacy[1] = byte(len(encoded) >> 8)
	legacy[2] = byte(len(encoded) >> 16)
	legacy[3] = byte(len(encoded) >> 24)
	copy(legacy[4:], encoded)

	opts := DefaultMasterReaderOptions()
	opts.AllowLegacy = true

	reader := NewMasterReader(legacy, opts)
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed for legacy: %v", err)
	}

	if frame.Payload.Get("legacy").String() != "data" {
		t.Error("Failed to read legacy payload")
	}
}

func TestMasterStreamNoCompression(t *testing.T) {
	data := map[string]any{"small": "data"}

	var buf bytes.Buffer
	opts := MasterWriterOptions{
		Compression: cowrie.CompressionNone,
	}
	writer := NewMasterWriter(&buf, opts)

	err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
	frame, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if frame.Header.Flags&FlagMasterCompressed != 0 {
		t.Error("Compression flag should not be set")
	}
}

func TestIsMasterStream(t *testing.T) {
	// Valid master stream
	valid := []byte{'S', 'J', 'S', 'T', 0x02, 0x00}
	if !IsMasterStream(valid) {
		t.Error("IsMasterStream should return true for valid magic")
	}

	// Too short
	short := []byte{'S', 'J'}
	if IsMasterStream(short) {
		t.Error("IsMasterStream should return false for short data")
	}

	// Wrong magic
	wrong := []byte{'S', 'J', 'O', 'N'}
	if IsMasterStream(wrong) {
		t.Error("IsMasterStream should return false for wrong magic")
	}
}

func TestIsCowrieDocument(t *testing.T) {
	// Valid Cowrie document
	valid := []byte{'S', 'J', 0x02, 0x00}
	if !IsCowrieDocument(valid) {
		t.Error("IsCowrieDocument should return true for valid document")
	}

	// Master stream (should be false)
	master := []byte{'S', 'J', 'S', 'T'}
	if IsCowrieDocument(master) {
		t.Error("IsCowrieDocument should return false for master stream")
	}
}

func TestReadAllMasterStream(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	writer := NewMasterWriter(&buf, opts)

	for i := 0; i < 5; i++ {
		err := writer.Write(map[string]any{"index": int64(i)})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	frames, err := ReadAllMasterStream(buf.Bytes(), DefaultMasterReaderOptions())
	if err != nil {
		t.Fatalf("ReadAllMasterStream failed: %v", err)
	}

	if len(frames) != 5 {
		t.Errorf("Expected 5 frames, got %d", len(frames))
	}

	for i, frame := range frames {
		idx := frame.Payload.Get("index").Int64()
		if idx != int64(i) {
			t.Errorf("Frame %d: expected index=%d, got %d", i, i, idx)
		}
	}
}

func TestMasterStreamPosition(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	opts.Compression = cowrie.CompressionNone // No compression for predictable size
	opts.EnableCRC = false
	writer := NewMasterWriter(&buf, opts)

	_ = writer.Write(map[string]any{"a": int64(1)})
	_ = writer.Write(map[string]any{"b": int64(2)})

	reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())

	if reader.Position() != 0 {
		t.Error("Initial position should be 0")
	}

	if reader.Remaining() != len(buf.Bytes()) {
		t.Errorf("Initial remaining should be %d", len(buf.Bytes()))
	}

	_, _ = reader.Next()

	if reader.Position() == 0 {
		t.Error("Position should advance after reading")
	}

	if reader.Remaining() >= len(buf.Bytes()) {
		t.Error("Remaining should decrease after reading")
	}
}

func BenchmarkMasterStreamWrite(b *testing.B) {
	data := map[string]any{
		"id":     int64(12345),
		"name":   "benchmark",
		"values": []float32{1.0, 2.0, 3.0, 4.0},
	}

	opts := DefaultMasterWriterOptions()
	opts.Compression = cowrie.CompressionNone // Benchmark without compression overhead

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := NewMasterWriter(&buf, opts)
		_ = writer.Write(data)
	}
}

func BenchmarkMasterStreamRead(b *testing.B) {
	data := map[string]any{
		"id":     int64(12345),
		"name":   "benchmark",
		"values": []float32{1.0, 2.0, 3.0, 4.0},
	}

	var buf bytes.Buffer
	opts := DefaultMasterWriterOptions()
	opts.Compression = cowrie.CompressionNone
	writer := NewMasterWriter(&buf, opts)
	_ = writer.Write(data)

	encoded := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := NewMasterReader(encoded, DefaultMasterReaderOptions())
		_, _ = reader.Next()
	}
}

func BenchmarkMasterStreamRoundTrip(b *testing.B) {
	data := map[string]any{
		"id":     int64(12345),
		"name":   "benchmark",
		"values": []float32{1.0, 2.0, 3.0, 4.0},
	}

	opts := DefaultMasterWriterOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := NewMasterWriter(&buf, opts)
		_ = writer.Write(data)

		reader := NewMasterReader(buf.Bytes(), DefaultMasterReaderOptions())
		_, _ = reader.Next()
	}
}
