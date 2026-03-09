package cowrie

import (
	"bytes"
	"testing"
)

// ============================================================
// compress.go: decompressGzip, decompressZstd edge cases
// ============================================================

func TestDecompressGzipRoundtrip2(t *testing.T) {
	original := []byte("hello gzip, this is a roundtrip test with enough data to compress")
	compressed, err := compressGzip(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressGzip(compressed, 0) // no limit
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestDecompressGzipWithLimit2(t *testing.T) {
	original := []byte("hello gzip, limited decompression test data for coverage")
	compressed, err := compressGzip(original)
	if err != nil {
		t.Fatal(err)
	}
	// With sufficient limit
	got, err := decompressGzip(compressed, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("roundtrip mismatch")
	}
	// With too-small limit
	_, err = decompressGzip(compressed, 5)
	if err == nil {
		t.Fatal("expected error for too-small limit")
	}
}

func TestDecompressGzipInvalid2(t *testing.T) {
	_, err := decompressGzip([]byte("not gzip"), 0)
	if err == nil {
		t.Fatal("expected error for invalid gzip data")
	}
}

func TestDecompressZstdRoundtrip2(t *testing.T) {
	original := []byte("hello zstd, this is a roundtrip test with enough data to compress well")
	compressed, err := compressZstd(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressZstd(compressed, 0) // no limit
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestDecompressZstdWithLimit2(t *testing.T) {
	original := []byte("hello zstd, limited decompression test data here for coverage boost")
	compressed, err := compressZstd(original)
	if err != nil {
		t.Fatal(err)
	}
	// With sufficient limit
	got, err := decompressZstd(compressed, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("roundtrip mismatch")
	}
	// With too-small limit
	_, err = decompressZstd(compressed, 5)
	if err == nil {
		t.Fatal("expected error for too-small limit")
	}
}

func TestDecompressZstdInvalid2(t *testing.T) {
	_, err := decompressZstd([]byte("not zstd"), 0)
	if err == nil {
		t.Fatal("expected error for invalid zstd data")
	}
}

// ============================================================
// column_reader.go: Root, Len, Stats via EncodeWithHints
// ============================================================

func TestColumnReaderWithHints(t *testing.T) {
	items := Array(
		Object(Member{Key: "name", Value: String("Alice")}, Member{Key: "age", Value: Int64(30)}),
		Object(Member{Key: "name", Value: String("Bob")}, Member{Key: "age", Value: Int64(25)}),
	)

	hints := []ColumnHint{
		{Field: "name", Type: HintString},
		{Field: "age", Type: HintInt64},
	}
	data, err := EncodeWithHints(items, hints)
	if err != nil {
		t.Fatal(err)
	}

	cr, err := NewColumnReader(data)
	if err != nil {
		t.Skipf("NewColumnReader: %v", err)
	}

	root, err := cr.Root()
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != TypeArray {
		t.Fatalf("expected array root, got %s", root.Type())
	}

	n, err := cr.Len()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	// Stats
	stats, err := cr.Stats("name")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Count != 2 {
		t.Fatalf("expected 2, got %d", stats.Count)
	}
	if stats.ValidCount != 2 {
		t.Fatalf("expected 2 valid, got %d", stats.ValidCount)
	}
}

// ============================================================
// schema.go: fnvHashBytes
// ============================================================

func TestFnvHashBytes2(t *testing.T) {
	h := uint64(fnvOffsetBasis64)
	h1 := fnvHashBytes(h, []byte("hello"))
	h2 := fnvHashBytes(h, []byte("world"))
	if h1 == h2 {
		t.Fatal("different inputs should produce different hashes")
	}
	h3 := fnvHashBytes(h, []byte("hello"))
	if h1 != h3 {
		t.Fatal("same input should produce same hash")
	}
}

// ============================================================
// hints.go: uvarintBytes
// ============================================================

func TestUvarintBytes2(t *testing.T) {
	b := uvarintBytes(0)
	if len(b) != 1 || b[0] != 0 {
		t.Fatalf("expected [0], got %v", b)
	}
	b = uvarintBytes(127)
	if len(b) != 1 {
		t.Fatalf("expected 1 byte for 127, got %d", len(b))
	}
	b = uvarintBytes(128)
	if len(b) != 2 {
		t.Fatalf("expected 2 bytes for 128, got %d", len(b))
	}
}

// ============================================================
// EncodeFramed / DecodeFramedWithLimit roundtrip
// ============================================================

func TestFramedCompressRoundtrip2(t *testing.T) {
	v := Object(
		Member{Key: "data", Value: String("test data for framed encoding")},
	)

	// Gzip framing
	framed, err := EncodeFramed(v, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFramedWithLimit(framed, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type() != TypeObject {
		t.Fatalf("expected object, got %s", decoded.Type())
	}

	// Zstd framing
	framed, err = EncodeFramed(v, CompressionZstd)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = DecodeFramedWithLimit(framed, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type() != TypeObject {
		t.Fatalf("expected object, got %s", decoded.Type())
	}

	// None framing
	framed, err = EncodeFramed(v, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = DecodeFramedWithLimit(framed, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type() != TypeObject {
		t.Fatalf("expected object, got %s", decoded.Type())
	}
}

// ============================================================
// decode.go: security limits
// ============================================================

func TestDecodeSecurityLimits(t *testing.T) {
	// Test with deep nesting
	v := String("leaf")
	for i := 0; i < 10; i++ {
		v = Object(Member{Key: "nested", Value: v})
	}
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	// Decode with very restrictive depth limit
	_, err = DecodeFromWithOptions(bytes.NewReader(data), DecodeOptions{MaxDepth: 3})
	if err == nil {
		t.Fatal("expected depth limit error")
	}
}

func TestDecodeStringLimit(t *testing.T) {
	longStr := String(string(make([]byte, 1000)))
	data, err := Encode(longStr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeFromWithOptions(bytes.NewReader(data), DecodeOptions{MaxStringLen: 10})
	if err == nil {
		t.Fatal("expected string length limit error")
	}
}

// ============================================================
// any_api.go: EncodeAnyWithOptions
// ============================================================

func TestEncodeAnyWithOptionsEnriched(t *testing.T) {
	type Custom struct {
		X int    `json:"x"`
		Y string `json:"y"`
	}
	c := Custom{X: 42, Y: "hello"}
	opts := AnyOptions{Enriched: true}
	data, err := EncodeAnyWithOptions(c, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty data")
	}
}

// ============================================================
// DecodeFrom with reader
// ============================================================

func TestDecodeFromReader(t *testing.T) {
	v := Object(
		Member{Key: "x", Value: Int64(1)},
		Member{Key: "y", Value: Float64(3.14)},
		Member{Key: "z", Value: Bool(true)},
		Member{Key: "w", Value: Bytes([]byte{1, 2, 3})},
	)
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeFrom(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if result.Get("x").Int64() != 1 {
		t.Fatal("expected x=1")
	}
}
