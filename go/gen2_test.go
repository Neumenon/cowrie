package cowrie

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestPrimitives(t *testing.T) {
	tests := []struct {
		name  string
		value *Value
		check func(*Value) bool
	}{
		{"null", Null(), func(v *Value) bool { return v.IsNull() }},
		{"bool true", Bool(true), func(v *Value) bool { return v.Type() == TypeBool && v.Bool() == true }},
		{"bool false", Bool(false), func(v *Value) bool { return v.Type() == TypeBool && v.Bool() == false }},
		{"int64 positive", Int64(12345), func(v *Value) bool { return v.Type() == TypeInt64 && v.Int64() == 12345 }},
		{"int64 negative", Int64(-9876), func(v *Value) bool { return v.Type() == TypeInt64 && v.Int64() == -9876 }},
		{"int64 max", Int64(math.MaxInt64), func(v *Value) bool { return v.Type() == TypeInt64 && v.Int64() == math.MaxInt64 }},
		{"int64 min", Int64(math.MinInt64), func(v *Value) bool { return v.Type() == TypeInt64 && v.Int64() == math.MinInt64 }},
		{"uint64", Uint64(math.MaxUint64), func(v *Value) bool { return v.Type() == TypeUint64 && v.Uint64() == math.MaxUint64 }},
		{"float64", Float64(3.14159265359), func(v *Value) bool { return v.Type() == TypeFloat64 && v.Float64() == 3.14159265359 }},
		{"string", String("Hello, Cowrie!"), func(v *Value) bool { return v.Type() == TypeString && v.String() == "Hello, Cowrie!" }},
		{"string empty", String(""), func(v *Value) bool { return v.Type() == TypeString && v.String() == "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Check
			if !tt.check(decoded) {
				t.Errorf("Check failed for %s", tt.name)
			}
		})
	}
}

func TestBytes(t *testing.T) {
	original := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF}
	v := Bytes(original)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeBytes {
		t.Errorf("Expected TypeBytes, got %v", decoded.Type())
	}
	if !bytes.Equal(decoded.Bytes(), original) {
		t.Errorf("Bytes mismatch: got %v, want %v", decoded.Bytes(), original)
	}
}

func TestDatetime64(t *testing.T) {
	now := time.Now().UTC()
	v := Datetime(now)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeDatetime64 {
		t.Errorf("Expected TypeDatetime64, got %v", decoded.Type())
	}

	// Compare nanoseconds
	if decoded.Datetime64() != now.UnixNano() {
		t.Errorf("Datetime mismatch: got %v, want %v", decoded.Datetime64(), now.UnixNano())
	}
}

func TestUUID128(t *testing.T) {
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	v := UUID128(uuid)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeUUID128 {
		t.Errorf("Expected TypeUUID128, got %v", decoded.Type())
	}
	if decoded.UUID128() != uuid {
		t.Errorf("UUID mismatch: got %v, want %v", decoded.UUID128(), uuid)
	}
}

func TestDecimal128(t *testing.T) {
	// 123.45 = 12345 * 10^-2
	var coef [16]byte
	coef[14] = 0x30 // 12345 = 0x3039
	coef[15] = 0x39
	v := NewDecimal128(2, coef)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeDecimal128 {
		t.Errorf("Expected TypeDecimal128, got %v", decoded.Type())
	}

	d := decoded.Decimal128()
	if d.Scale != 2 {
		t.Errorf("Scale mismatch: got %v, want 2", d.Scale)
	}
	if d.Coef != coef {
		t.Errorf("Coef mismatch")
	}
}

func TestBigInt(t *testing.T) {
	// 2^64 in bytes
	bigint := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	v := BigInt(bigint)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeBigInt {
		t.Errorf("Expected TypeBigInt, got %v", decoded.Type())
	}
	if !bytes.Equal(decoded.BigInt(), bigint) {
		t.Errorf("BigInt mismatch")
	}
}

func TestArray(t *testing.T) {
	v := Array(
		Int64(1),
		Int64(2),
		Int64(3),
		String("four"),
		Bool(true),
	)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeArray {
		t.Errorf("Expected TypeArray, got %v", decoded.Type())
	}
	if decoded.Len() != 5 {
		t.Errorf("Array length mismatch: got %d, want 5", decoded.Len())
	}
	if decoded.Index(0).Int64() != 1 {
		t.Errorf("First element mismatch")
	}
	if decoded.Index(3).String() != "four" {
		t.Errorf("Fourth element mismatch")
	}
}

func TestObject(t *testing.T) {
	v := Object(
		Member{Key: "name", Value: String("Alice")},
		Member{Key: "age", Value: Int64(30)},
		Member{Key: "active", Value: Bool(true)},
	)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type() != TypeObject {
		t.Errorf("Expected TypeObject, got %v", decoded.Type())
	}
	if decoded.Len() != 3 {
		t.Errorf("Object length mismatch: got %d, want 3", decoded.Len())
	}
	if decoded.Get("name").String() != "Alice" {
		t.Errorf("Name mismatch")
	}
	if decoded.Get("age").Int64() != 30 {
		t.Errorf("Age mismatch")
	}
}

func TestNested(t *testing.T) {
	v := Object(
		Member{Key: "user", Value: Object(
			Member{Key: "name", Value: String("Bob")},
			Member{Key: "tags", Value: Array(
				String("admin"),
				String("developer"),
			)},
		)},
	)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	user := decoded.Get("user")
	if user == nil || user.Type() != TypeObject {
		t.Fatalf("User object not found")
	}
	if user.Get("name").String() != "Bob" {
		t.Errorf("User name mismatch")
	}

	tags := user.Get("tags")
	if tags == nil || tags.Type() != TypeArray {
		t.Fatalf("Tags array not found")
	}
	if tags.Len() != 2 {
		t.Errorf("Tags length mismatch")
	}
	if tags.Index(0).String() != "admin" {
		t.Errorf("First tag mismatch")
	}
}

func TestCompression(t *testing.T) {
	// Create a large object to ensure compression kicks in
	members := make([]Member, 100)
	for i := 0; i < 100; i++ {
		members[i] = Member{
			Key:   "field_" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Value: String("This is a test value that should compress well with repetition"),
		}
	}
	v := Object(members...)

	// Test zstd compression
	t.Run("zstd", func(t *testing.T) {
		data, err := EncodeFramed(v, CompressionZstd)
		if err != nil {
			t.Fatalf("EncodeFramed failed: %v", err)
		}

		raw, _ := Encode(v)
		t.Logf("Raw size: %d, Compressed size: %d, Ratio: %.2f%%", len(raw), len(data), float64(len(data))*100/float64(len(raw)))

		decoded, err := DecodeFramed(data)
		if err != nil {
			t.Fatalf("DecodeFramed failed: %v", err)
		}

		if decoded.Len() != 100 {
			t.Errorf("Object length mismatch after decompression")
		}
	})

	// Test gzip compression
	t.Run("gzip", func(t *testing.T) {
		data, err := EncodeFramed(v, CompressionGzip)
		if err != nil {
			t.Fatalf("EncodeFramed failed: %v", err)
		}

		raw, _ := Encode(v)
		t.Logf("Raw size: %d, Compressed size: %d, Ratio: %.2f%%", len(raw), len(data), float64(len(data))*100/float64(len(raw)))

		decoded, err := DecodeFramed(data)
		if err != nil {
			t.Fatalf("DecodeFramed failed: %v", err)
		}

		if decoded.Len() != 100 {
			t.Errorf("Object length mismatch after decompression")
		}
	})
}

func TestJSONBridge(t *testing.T) {
	jsonData := `{
		"name": "Alice",
		"age": 30,
		"active": true,
		"balance": 1234.56,
		"tags": ["admin", "user"],
		"created_at": "2025-11-28T12:34:56Z",
		"id": "550e8400-e29b-41d4-a716-446655440000"
	}`

	// Parse JSON to Cowrie with enrichment (type inference)
	v, err := FromJSONEnriched([]byte(jsonData))
	if err != nil {
		t.Fatalf("FromJSONEnriched failed: %v", err)
	}

	// Check type inference
	if v.Get("name").Type() != TypeString {
		t.Errorf("Expected name to be string")
	}
	if v.Get("age").Type() != TypeInt64 {
		t.Errorf("Expected age to be int64, got %v", v.Get("age").Type())
	}
	if v.Get("created_at").Type() != TypeDatetime64 {
		t.Errorf("Expected created_at to be datetime64, got %v", v.Get("created_at").Type())
	}
	if v.Get("id").Type() != TypeUUID128 {
		t.Errorf("Expected id to be uuid128, got %v", v.Get("id").Type())
	}

	// Convert back to JSON
	out, err := ToJSONIndent(v, "  ")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	t.Logf("Round-tripped JSON:\n%s", string(out))

	// Verify it's valid JSON
	var check map[string]any
	if err := json.Unmarshal(out, &check); err != nil {
		t.Fatalf("Output is not valid JSON: %v", err)
	}
}

func TestSizeComparison(t *testing.T) {
	// Create a typical API response
	v := Object(
		Member{Key: "user_id", Value: Int64(12345)},
		Member{Key: "user_name", Value: String("alice_wonder")},
		Member{Key: "user_email", Value: String("alice@example.com")},
		Member{Key: "created_at", Value: Datetime(time.Now())},
		Member{Key: "is_active", Value: Bool(true)},
		Member{Key: "balance", Value: Float64(1234.56)},
		Member{Key: "tags", Value: Array(
			String("premium"),
			String("verified"),
			String("developer"),
		)},
	)

	// Cowrie size
	cowrieData, _ := Encode(v)

	// JSON equivalent
	jsonData, _ := ToJSON(v)

	t.Logf("Cowrie size: %d bytes", len(cowrieData))
	t.Logf("JSON size:  %d bytes", len(jsonData))
	t.Logf("Savings:    %.1f%%", (1-float64(len(cowrieData))/float64(len(jsonData)))*100)
}

func BenchmarkEncode(b *testing.B) {
	v := Object(
		Member{Key: "user_id", Value: Int64(12345)},
		Member{Key: "user_name", Value: String("alice_wonder")},
		Member{Key: "user_email", Value: String("alice@example.com")},
		Member{Key: "is_active", Value: Bool(true)},
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(v)
	}
}

func BenchmarkDecode(b *testing.B) {
	v := Object(
		Member{Key: "user_id", Value: Int64(12345)},
		Member{Key: "user_name", Value: String("alice_wonder")},
		Member{Key: "user_email", Value: String("alice@example.com")},
		Member{Key: "is_active", Value: Bool(true)},
	)
	data, _ := Encode(v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	v := map[string]any{
		"user_id":    12345,
		"user_name":  "alice_wonder",
		"user_email": "alice@example.com",
		"is_active":  true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(v)
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	data := []byte(`{"user_id":12345,"user_name":"alice_wonder","user_email":"alice@example.com","is_active":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v map[string]any
		json.Unmarshal(data, &v)
	}
}

// TestTagExtUnknownKeep tests that unknown extensions are preserved by default.
func TestTagExtUnknownKeep(t *testing.T) {
	// Manually craft a TagExt payload
	// Wire format: Magic(2) | Version(1) | Flags(1) | DictLen(uvarint) | TagExt | ExtType(uvarint) | Len(uvarint) | Payload
	payload := []byte{
		'S', 'J', // Magic
		0x02,       // Version
		0x00,       // Flags
		0x00,       // DictLen = 0
		TagExt,     // TagExt = 0x0E
		0x99, 0x01, // ExtType = 153 (uvarint: 0x99 0x01 = 153)
		0x05,                    // Len = 5
		'h', 'e', 'l', 'l', 'o', // Payload
	}

	// Decode with default options (UnknownExtKeep)
	v, err := Decode(payload)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if v.Type() != TypeUnknownExt {
		t.Fatalf("Expected TypeUnknownExt, got %v", v.Type())
	}

	ext := v.UnknownExt()
	if ext.ExtType != 153 {
		t.Errorf("ExtType: got %d, want 153", ext.ExtType)
	}
	if !bytes.Equal(ext.Payload, []byte("hello")) {
		t.Errorf("Payload: got %q, want %q", ext.Payload, "hello")
	}
}

// TestTagExtUnknownSkipAsNull tests that unknown extensions can be skipped as Null.
func TestTagExtUnknownSkipAsNull(t *testing.T) {
	payload := []byte{
		'S', 'J', // Magic
		0x02,          // Version
		0x00,          // Flags
		0x00,          // DictLen = 0
		TagExt,        // TagExt
		0x42,          // ExtType = 66
		0x03,          // Len = 3
		'a', 'b', 'c', // Payload
	}

	opts := DecodeOptions{
		OnUnknownExt: UnknownExtSkipAsNull,
	}
	v, err := DecodeWithOptions(payload, opts)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if v.Type() != TypeNull {
		t.Fatalf("Expected TypeNull, got %v", v.Type())
	}
}

// TestTagExtUnknownError tests that unknown extensions can cause an error in strict mode.
func TestTagExtUnknownError(t *testing.T) {
	payload := []byte{
		'S', 'J', // Magic
		0x02,     // Version
		0x00,     // Flags
		0x00,     // DictLen = 0
		TagExt,   // TagExt
		0x01,     // ExtType = 1
		0x02,     // Len = 2
		'x', 'y', // Payload
	}

	opts := DecodeOptions{
		OnUnknownExt: UnknownExtError,
	}
	_, err := DecodeWithOptions(payload, opts)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if err != ErrUnknownExt {
		t.Errorf("Expected ErrUnknownExt, got %v", err)
	}
}

// TestTagExtPayloadTooLarge tests that oversized extension payloads are rejected.
func TestTagExtPayloadTooLarge(t *testing.T) {
	// Craft a payload claiming to be very large
	payload := []byte{
		'S', 'J', // Magic
		0x02,                         // Version
		0x00,                         // Flags
		0x00,                         // DictLen = 0
		TagExt,                       // TagExt
		0x01,                         // ExtType = 1
		0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // Len = MaxUint32 (huge, will exceed limit)
	}

	opts := DecodeOptions{
		MaxExtLen: 1024, // 1KB limit
	}
	_, err := DecodeWithOptions(payload, opts)
	if err == nil {
		t.Fatal("Expected error for oversized payload")
	}
	// Could be ErrMalformedLength (exceeds remaining) or ErrExtTooLarge
	// depending on which check fires first
}

// TestUnknownExtRoundTrip tests that unknown extensions can be encoded back after decoding.
func TestUnknownExtRoundTrip(t *testing.T) {
	// Craft a TagExt payload
	original := []byte{
		'S', 'J', // Magic
		0x02,       // Version
		0x00,       // Flags
		0x00,       // DictLen = 0
		TagExt,     // TagExt = 0x0E
		0x99, 0x01, // ExtType = 153 (uvarint)
		0x05,                    // Len = 5
		'h', 'e', 'l', 'l', 'o', // Payload
	}

	// Decode
	v, err := Decode(original)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if v.Type() != TypeUnknownExt {
		t.Fatalf("Expected TypeUnknownExt, got %v", v.Type())
	}

	// Re-encode
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode again
	v2, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Second decode failed: %v", err)
	}

	if v2.Type() != TypeUnknownExt {
		t.Fatalf("Expected TypeUnknownExt after round-trip, got %v", v2.Type())
	}

	ext := v2.UnknownExt()
	if ext.ExtType != 153 {
		t.Errorf("ExtType: got %d, want 153", ext.ExtType)
	}
	if !bytes.Equal(ext.Payload, []byte("hello")) {
		t.Errorf("Payload: got %q, want %q", ext.Payload, "hello")
	}
}

// TestTensorDataTooLarge tests that oversized tensor data is rejected.
func TestTensorDataTooLarge(t *testing.T) {
	// Create a tensor claiming huge data
	payload := []byte{
		'S', 'J', // Magic
		0x02,                         // Version
		0x00,                         // Flags
		0x00,                         // DictLen = 0
		TagTensor,                    // Tensor tag
		0x01,                         // DType = Float32
		0x01,                         // Rank = 1
		0x01,                         // Dim[0] = 1
		0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // DataLen = huge
	}

	opts := DecodeOptions{
		MaxBytesLen: 1024, // 1KB limit
	}
	_, err := DecodeWithOptions(payload, opts)
	if err == nil {
		t.Fatal("Expected error for oversized tensor data")
	}
	// Should be ErrMalformedLength (exceeds remaining) or ErrBytesTooLarge
}

// TestImageDataTooLarge tests that oversized image data is rejected.
func TestImageDataTooLarge(t *testing.T) {
	payload := []byte{
		'S', 'J', // Magic
		0x02,       // Version
		0x00,       // Flags
		0x00,       // DictLen = 0
		TagImage,   // Image tag
		0x01,       // Format = JPEG
		0x80, 0x02, // Width = 640 (little-endian)
		0xE0, 0x01, // Height = 480 (little-endian)
		0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // DataLen = huge
	}

	opts := DecodeOptions{
		MaxBytesLen: 1024, // 1KB limit
	}
	_, err := DecodeWithOptions(payload, opts)
	if err == nil {
		t.Fatal("Expected error for oversized image data")
	}
}

// TestDictTooLarge tests that oversized dictionary is rejected before allocation.
func TestDictTooLarge(t *testing.T) {
	payload := []byte{
		'S', 'J', // Magic
		0x02,                         // Version
		0x00,                         // Flags
		0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // DictLen = huge (will exceed limit before alloc)
	}

	opts := DecodeOptions{
		MaxDictLen: 100, // Small limit
	}
	_, err := DecodeWithOptions(payload, opts)
	if err == nil {
		t.Fatal("Expected error for oversized dictionary")
	}
	if err != ErrDictTooLarge && err != ErrMalformedLength {
		t.Logf("Got error: %v (expected ErrDictTooLarge or ErrMalformedLength)", err)
	}
}

// TestInvalidVarint tests that overflow varints are detected.
func TestInvalidVarint(t *testing.T) {
	// A varint that would overflow uint64 (more than 10 bytes with high bits set)
	payload := []byte{
		'S', 'J', // Magic
		0x02, // Version
		0x00, // Flags
		// DictLen as overflowing varint (all continuation bits, too many bytes)
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
	}

	_, err := Decode(payload)
	if err == nil {
		t.Fatal("Expected error for invalid varint")
	}
	if err != ErrInvalidVarint {
		t.Logf("Got error: %v (expected ErrInvalidVarint)", err)
	}
}

// TestEncoderRejectsInvalidType tests that the encoder returns an error for unknown types.
func TestEncoderRejectsInvalidType(t *testing.T) {
	// Create a value with an invalid type (this shouldn't happen in normal use)
	v := &Value{typ: Type(255)} // Invalid type

	_, err := Encode(v)
	if err == nil {
		t.Fatal("Expected error for invalid type")
	}
	t.Logf("Got expected error: %v", err)
}

// TestExtensionTypeJSONProjection tests that extension types have proper JSON representations.
func TestExtensionTypeJSONProjection(t *testing.T) {
	t.Run("Tensor", func(t *testing.T) {
		v := Tensor(DTypeFloat32, []uint64{2, 3}, []byte{0, 0, 128, 63, 0, 0, 0, 64}) // 1.0, 2.0

		jsonBytes, err := ToJSON(v)
		if err != nil {
			t.Fatalf("ToJSON failed: %v", err)
		}

		var m map[string]any
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			t.Fatalf("JSON unmarshal failed: %v", err)
		}

		if m["_type"] != "tensor" {
			t.Errorf("Expected _type=tensor, got %v", m["_type"])
		}
		if m["dtype"] != "float32" {
			t.Errorf("Expected dtype=float32, got %v", m["dtype"])
		}
	})

	t.Run("Image", func(t *testing.T) {
		v := Image(ImageFormatPNG, 640, 480, []byte{0x89, 'P', 'N', 'G'})

		jsonBytes, err := ToJSON(v)
		if err != nil {
			t.Fatalf("ToJSON failed: %v", err)
		}

		var m map[string]any
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			t.Fatalf("JSON unmarshal failed: %v", err)
		}

		if m["_type"] != "image" {
			t.Errorf("Expected _type=image, got %v", m["_type"])
		}
		if m["format"] != "png" {
			t.Errorf("Expected format=png, got %v", m["format"])
		}
		if int(m["width"].(float64)) != 640 {
			t.Errorf("Expected width=640, got %v", m["width"])
		}
	})

	t.Run("Audio", func(t *testing.T) {
		v := Audio(AudioEncodingPCMInt16, 44100, 2, []byte{0, 0, 1, 0})

		jsonBytes, err := ToJSON(v)
		if err != nil {
			t.Fatalf("ToJSON failed: %v", err)
		}

		var m map[string]any
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			t.Fatalf("JSON unmarshal failed: %v", err)
		}

		if m["_type"] != "audio" {
			t.Errorf("Expected _type=audio, got %v", m["_type"])
		}
		if m["encoding"] != "pcm_int16" {
			t.Errorf("Expected encoding=pcm_int16, got %v", m["encoding"])
		}
		if int(m["rate"].(float64)) != 44100 {
			t.Errorf("Expected rate=44100, got %v", m["rate"])
		}
	})

	t.Run("UnknownExt", func(t *testing.T) {
		v := UnknownExtension(42, []byte("test payload"))

		jsonBytes, err := ToJSON(v)
		if err != nil {
			t.Fatalf("ToJSON failed: %v", err)
		}

		var m map[string]any
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			t.Fatalf("JSON unmarshal failed: %v", err)
		}

		if m["_type"] != "unknown_ext" {
			t.Errorf("Expected _type=unknown_ext, got %v", m["_type"])
		}
		if int(m["ext_type"].(float64)) != 42 {
			t.Errorf("Expected ext_type=42, got %v", m["ext_type"])
		}
	})
}

// ============================================================
// DecodeFromLimited Tests
// ============================================================

func TestDecodeFromLimited(t *testing.T) {
	// Create a small test value
	obj := Object(
		Member{Key: "name", Value: String("test")},
		Member{Key: "value", Value: Int64(42)},
	)
	data, err := Encode(obj)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	t.Run("within limit", func(t *testing.T) {
		r := bytes.NewReader(data)
		val, err := DecodeFromLimited(r, int64(len(data)+100))
		if err != nil {
			t.Fatalf("DecodeFromLimited failed: %v", err)
		}
		if val.Get("name").String() != "test" {
			t.Errorf("unexpected name: %v", val.Get("name").String())
		}
	})

	t.Run("exact limit", func(t *testing.T) {
		r := bytes.NewReader(data)
		val, err := DecodeFromLimited(r, int64(len(data)))
		if err != nil {
			t.Fatalf("DecodeFromLimited failed at exact limit: %v", err)
		}
		if val.Get("value").Int64() != 42 {
			t.Errorf("unexpected value: %v", val.Get("value").Int64())
		}
	})

	t.Run("exceeds limit", func(t *testing.T) {
		r := bytes.NewReader(data)
		_, err := DecodeFromLimited(r, int64(len(data)-1))
		if err == nil {
			t.Error("expected error for input exceeding limit")
		}
		if err != ErrInputTooLarge {
			t.Errorf("expected ErrInputTooLarge, got: %v", err)
		}
	})

	t.Run("zero limit", func(t *testing.T) {
		r := bytes.NewReader(data)
		_, err := DecodeFromLimited(r, 0)
		if err == nil {
			t.Error("expected error for zero limit")
		}
	})
}

func TestDecodeFromWithOptions(t *testing.T) {
	// Create a deeply nested value
	inner := String("deep")
	for i := 0; i < 10; i++ {
		inner = Object(Member{Key: "nested", Value: inner})
	}
	data, err := Encode(inner)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	t.Run("with default options", func(t *testing.T) {
		r := bytes.NewReader(data)
		_, err := DecodeFromWithOptions(r, DefaultDecodeOptions())
		if err != nil {
			t.Fatalf("DecodeFromWithOptions failed: %v", err)
		}
	})

	t.Run("with low depth limit", func(t *testing.T) {
		r := bytes.NewReader(data)
		opts := DecodeOptions{MaxDepth: 5}
		_, err := DecodeFromWithOptions(r, opts)
		if err == nil {
			t.Error("expected error for depth exceeded")
		}
	})
}

func TestJSONStrictRoundTrip(t *testing.T) {
	// These inputs should round-trip exactly with strict FromJSON
	tests := []string{
		`{"created_at":"2024-01-15T12:00:00Z"}`,
		`{"created_at":"2024-01-15T12:00:00.123456789Z"}`,
		`{"id":"550E8400-E29B-41D4-A716-446655440000"}`,
		`{"id":"550e8400-e29b-41d4-a716-446655440000"}`,
		`{"data":"SGVsbG8gV29ybGQ="}`,
		`{"name":"just a string"}`,
		`{"ts":"2024-01-15T12:00:00+05:30"}`,
		`{"num":42}`,
		`{"num":3.14159}`,
		`{"nested":{"a":1,"b":"two"}}`,
		`{"arr":[1,2,3]}`,
		`{"mixed":[1,"two",true,null]}`,
	}

	for _, jsonIn := range tests {
		t.Run(jsonIn, func(t *testing.T) {
			val, err := FromJSON([]byte(jsonIn))
			if err != nil {
				t.Fatalf("FromJSON failed: %v", err)
			}
			jsonOut, err := ToJSON(val)
			if err != nil {
				t.Fatalf("ToJSON failed: %v", err)
			}

			// Normalize both for comparison (JSON key order may vary)
			var in, out any
			json.Unmarshal([]byte(jsonIn), &in)
			json.Unmarshal(jsonOut, &out)

			inNorm, _ := json.Marshal(in)
			outNorm, _ := json.Marshal(out)

			if string(inNorm) != string(outNorm) {
				t.Errorf("Round-trip mismatch:\n  IN:  %s\n  OUT: %s", jsonIn, string(jsonOut))
			}
		})
	}
}

func TestFromJSONEnrichedVsStrict(t *testing.T) {
	jsonData := `{"created_at":"2024-01-15T12:00:00Z","id":"550e8400-e29b-41d4-a716-446655440000"}`

	// Strict: strings stay strings
	strict, _ := FromJSON([]byte(jsonData))
	if strict.Get("created_at").Type() != TypeString {
		t.Errorf("Strict: expected created_at to be string, got %v", strict.Get("created_at").Type())
	}
	if strict.Get("id").Type() != TypeString {
		t.Errorf("Strict: expected id to be string, got %v", strict.Get("id").Type())
	}

	// Enriched: strings get inferred types
	enriched, _ := FromJSONEnriched([]byte(jsonData))
	if enriched.Get("created_at").Type() != TypeDatetime64 {
		t.Errorf("Enriched: expected created_at to be datetime64, got %v", enriched.Get("created_at").Type())
	}
	if enriched.Get("id").Type() != TypeUUID128 {
		t.Errorf("Enriched: expected id to be uuid128, got %v", enriched.Get("id").Type())
	}
}

// ============================================================
// v2.1 Graph Type Tests
// ============================================================

func TestNodeRoundTrip(t *testing.T) {
	node := Node("person_42", []string{"Person", "Employee"}, map[string]any{
		"name":   "Alice",
		"age":    int64(30),
		"salary": float64(50000.0),
	})

	// Encode
	encoded, err := Encode(node)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify type
	if decoded.Type() != TypeNode {
		t.Fatalf("Expected TypeNode, got %v", decoded.Type())
	}

	// Verify data
	nodeData := decoded.Node()
	if nodeData.ID != "person_42" {
		t.Errorf("ID: got %q, want %q", nodeData.ID, "person_42")
	}
	if len(nodeData.Labels) != 2 || nodeData.Labels[0] != "Person" || nodeData.Labels[1] != "Employee" {
		t.Errorf("Labels: got %v, want [Person Employee]", nodeData.Labels)
	}
	if nodeData.Props["name"] != "Alice" {
		t.Errorf("Props[name]: got %v, want Alice", nodeData.Props["name"])
	}
	if nodeData.Props["age"] != int64(30) {
		t.Errorf("Props[age]: got %v, want 30", nodeData.Props["age"])
	}
}

func TestEdgeRoundTrip(t *testing.T) {
	edge := Edge("person_42", "company_1", "WORKS_AT", map[string]any{
		"since": int64(2020),
		"role":  "Engineer",
	})

	// Encode
	encoded, err := Encode(edge)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify type
	if decoded.Type() != TypeEdge {
		t.Fatalf("Expected TypeEdge, got %v", decoded.Type())
	}

	// Verify data
	edgeData := decoded.Edge()
	if edgeData.From != "person_42" {
		t.Errorf("From: got %q, want %q", edgeData.From, "person_42")
	}
	if edgeData.To != "company_1" {
		t.Errorf("To: got %q, want %q", edgeData.To, "company_1")
	}
	if edgeData.Type != "WORKS_AT" {
		t.Errorf("Type: got %q, want %q", edgeData.Type, "WORKS_AT")
	}
	if edgeData.Props["since"] != int64(2020) {
		t.Errorf("Props[since]: got %v, want 2020", edgeData.Props["since"])
	}
	if edgeData.Props["role"] != "Engineer" {
		t.Errorf("Props[role]: got %v, want Engineer", edgeData.Props["role"])
	}
}

func TestNodeBatchRoundTrip(t *testing.T) {
	nodes := []NodeData{
		{ID: "n1", Labels: []string{"A"}, Props: map[string]any{"x": float64(0.1)}},
		{ID: "n2", Labels: []string{"B"}, Props: map[string]any{"x": float64(0.2)}},
		{ID: "n3", Labels: []string{"A", "B"}, Props: map[string]any{"x": float64(0.3)}},
	}
	batch := NodeBatch(nodes)

	// Encode
	encoded, err := Encode(batch)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify type
	if decoded.Type() != TypeNodeBatch {
		t.Fatalf("Expected TypeNodeBatch, got %v", decoded.Type())
	}

	// Verify data
	batchData := decoded.NodeBatch()
	if len(batchData.Nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(batchData.Nodes))
	}
	if batchData.Nodes[0].ID != "n1" {
		t.Errorf("Nodes[0].ID: got %q, want %q", batchData.Nodes[0].ID, "n1")
	}
	if batchData.Nodes[2].Labels[1] != "B" {
		t.Errorf("Nodes[2].Labels[1]: got %q, want %q", batchData.Nodes[2].Labels[1], "B")
	}
}

func TestEdgeBatchRoundTrip(t *testing.T) {
	edges := []EdgeData{
		{From: "n1", To: "n2", Type: "KNOWS", Props: map[string]any{"weight": float64(0.5)}},
		{From: "n2", To: "n3", Type: "FOLLOWS", Props: map[string]any{"weight": float64(0.8)}},
	}
	batch := EdgeBatch(edges)

	// Encode
	encoded, err := Encode(batch)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify type
	if decoded.Type() != TypeEdgeBatch {
		t.Fatalf("Expected TypeEdgeBatch, got %v", decoded.Type())
	}

	// Verify data
	batchData := decoded.EdgeBatch()
	if len(batchData.Edges) != 2 {
		t.Fatalf("Expected 2 edges, got %d", len(batchData.Edges))
	}
	if batchData.Edges[0].Type != "KNOWS" {
		t.Errorf("Edges[0].Type: got %q, want %q", batchData.Edges[0].Type, "KNOWS")
	}
	if batchData.Edges[1].Props["weight"] != float64(0.8) {
		t.Errorf("Edges[1].Props[weight]: got %v, want 0.8", batchData.Edges[1].Props["weight"])
	}
}

func TestGraphShardRoundTrip(t *testing.T) {
	nodes := []NodeData{
		{ID: "1", Labels: []string{"Node"}, Props: map[string]any{"x": float64(0.1)}},
		{ID: "2", Labels: []string{"Node"}, Props: map[string]any{"x": float64(0.2)}},
	}
	edges := []EdgeData{
		{From: "1", To: "2", Type: "EDGE", Props: map[string]any{"weight": float64(0.85)}},
	}
	metadata := map[string]any{
		"version":     int64(1),
		"partitionId": int64(42),
	}
	shard := GraphShard(nodes, edges, metadata)

	// Encode
	encoded, err := Encode(shard)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify type
	if decoded.Type() != TypeGraphShard {
		t.Fatalf("Expected TypeGraphShard, got %v", decoded.Type())
	}

	// Verify data
	shardData := decoded.GraphShard()
	if len(shardData.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(shardData.Nodes))
	}
	if len(shardData.Edges) != 1 {
		t.Fatalf("Expected 1 edge, got %d", len(shardData.Edges))
	}
	if shardData.Metadata["version"] != int64(1) {
		t.Errorf("Metadata[version]: got %v, want 1", shardData.Metadata["version"])
	}
	if shardData.Metadata["partitionId"] != int64(42) {
		t.Errorf("Metadata[partitionId]: got %v, want 42", shardData.Metadata["partitionId"])
	}
}

func TestGraphDictionaryCodingSavings(t *testing.T) {
	// Create 100 nodes with repeated property keys
	nodes := make([]NodeData, 100)
	for i := 0; i < 100; i++ {
		nodes[i] = NodeData{
			ID:     fmt.Sprintf("node_%d", i),
			Labels: []string{"Person"},
			Props: map[string]any{
				"name":   fmt.Sprintf("User%d", i),
				"age":    int64(20 + i%50),
				"active": true,
			},
		}
	}
	batch := NodeBatch(nodes)

	// Encode
	encoded, err := Encode(batch)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Log the size (dictionary coding should provide significant savings)
	t.Logf("NodeBatch with 100 nodes: %d bytes", len(encoded))

	// Decode and verify
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	batchData := decoded.NodeBatch()
	if len(batchData.Nodes) != 100 {
		t.Fatalf("Expected 100 nodes, got %d", len(batchData.Nodes))
	}
}

func TestGraphEmptyCollections(t *testing.T) {
	// Test empty node
	node := Node("empty", nil, nil)
	encoded, err := Encode(node)
	if err != nil {
		t.Fatalf("Encode empty node failed: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty node failed: %v", err)
	}
	if decoded.Type() != TypeNode {
		t.Errorf("Expected TypeNode, got %v", decoded.Type())
	}
	nodeData := decoded.Node()
	if nodeData.ID != "empty" {
		t.Errorf("ID: got %q, want %q", nodeData.ID, "empty")
	}

	// Test empty batch
	batch := NodeBatch(nil)
	encoded, err = Encode(batch)
	if err != nil {
		t.Fatalf("Encode empty batch failed: %v", err)
	}
	decoded, err = Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty batch failed: %v", err)
	}
	if decoded.Type() != TypeNodeBatch {
		t.Errorf("Expected TypeNodeBatch, got %v", decoded.Type())
	}

	// Test empty shard
	shard := GraphShard(nil, nil, nil)
	encoded, err = Encode(shard)
	if err != nil {
		t.Fatalf("Encode empty shard failed: %v", err)
	}
	decoded, err = Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty shard failed: %v", err)
	}
	if decoded.Type() != TypeGraphShard {
		t.Errorf("Expected TypeGraphShard, got %v", decoded.Type())
	}
}
