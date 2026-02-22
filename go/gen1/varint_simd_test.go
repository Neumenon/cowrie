package gen1

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestGroupVByteEncode_SingleByte(t *testing.T) {
	// Values that fit in 1 byte each
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 1, 2, 3, 4)

	if n != 5 { // 1 control + 4 data bytes
		t.Errorf("expected 5 bytes, got %d", n)
	}

	// Control byte should be 0x00 (all 1-byte values)
	if dst[0] != 0x00 {
		t.Errorf("expected control byte 0x00, got 0x%02x", dst[0])
	}

	// Decode and verify
	v0, v1, v2, v3, consumed := GroupVByteDecode(dst[:n])
	if consumed != n {
		t.Errorf("decode consumed %d bytes, expected %d", consumed, n)
	}
	if v0 != 1 || v1 != 2 || v2 != 3 || v3 != 4 {
		t.Errorf("decode mismatch: got (%d, %d, %d, %d), expected (1, 2, 3, 4)", v0, v1, v2, v3)
	}
}

func TestGroupVByteEncode_TwoBytes(t *testing.T) {
	// Values that need 2 bytes each
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 256, 512, 1024, 2048)

	if n != 9 { // 1 control + 8 data bytes
		t.Errorf("expected 9 bytes, got %d", n)
	}

	// Control byte: 01 01 01 01 = 0x55
	if dst[0] != 0x55 {
		t.Errorf("expected control byte 0x55, got 0x%02x", dst[0])
	}

	v0, v1, v2, v3, _ := GroupVByteDecode(dst[:n])
	if v0 != 256 || v1 != 512 || v2 != 1024 || v3 != 2048 {
		t.Errorf("decode mismatch: got (%d, %d, %d, %d)", v0, v1, v2, v3)
	}
}

func TestGroupVByteEncode_ThreeBytes(t *testing.T) {
	// Values that need 3 bytes
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 0x10000, 0x20000, 0x30000, 0x40000)

	if n != 13 { // 1 control + 12 data bytes
		t.Errorf("expected 13 bytes, got %d", n)
	}

	// Control byte: 10 10 10 10 = 0xAA
	if dst[0] != 0xAA {
		t.Errorf("expected control byte 0xAA, got 0x%02x", dst[0])
	}

	v0, v1, v2, v3, _ := GroupVByteDecode(dst[:n])
	if v0 != 0x10000 || v1 != 0x20000 || v2 != 0x30000 || v3 != 0x40000 {
		t.Errorf("decode mismatch")
	}
}

func TestGroupVByteEncode_FourBytes(t *testing.T) {
	// Values that need 4 bytes
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 0x1000000, 0x2000000, 0x3000000, 0xFFFFFFFF)

	if n != 17 { // 1 control + 16 data bytes
		t.Errorf("expected 17 bytes, got %d", n)
	}

	// Control byte: 11 11 11 11 = 0xFF
	if dst[0] != 0xFF {
		t.Errorf("expected control byte 0xFF, got 0x%02x", dst[0])
	}

	v0, v1, v2, v3, _ := GroupVByteDecode(dst[:n])
	if v0 != 0x1000000 || v1 != 0x2000000 || v2 != 0x3000000 || v3 != 0xFFFFFFFF {
		t.Errorf("decode mismatch: got (%d, %d, %d, %d)", v0, v1, v2, v3)
	}
}

func TestGroupVByteEncode_Mixed(t *testing.T) {
	// Mixed sizes
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 5, 300, 70000, 0x12345678)

	// Expected sizes: 1, 2, 3, 4 bytes
	// Control: 00 01 10 11 = 0b11100100 = 0xE4
	if dst[0] != 0xE4 {
		t.Errorf("expected control byte 0xE4, got 0x%02x", dst[0])
	}

	v0, v1, v2, v3, consumed := GroupVByteDecode(dst[:n])
	if consumed != n {
		t.Errorf("consumed mismatch: %d vs %d", consumed, n)
	}
	if v0 != 5 || v1 != 300 || v2 != 70000 || v3 != 0x12345678 {
		t.Errorf("decode mismatch: got (%d, %d, %d, %d)", v0, v1, v2, v3)
	}
}

func TestZigzagEncode32(t *testing.T) {
	tests := []struct {
		input    int32
		expected uint32
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-128, 255},
		{127, 254},
		{-2147483648, 4294967295},
		{2147483647, 4294967294},
	}

	for _, tc := range tests {
		result := zigzagEncode32(tc.input)
		if result != tc.expected {
			t.Errorf("zigzagEncode32(%d) = %d, expected %d", tc.input, result, tc.expected)
		}

		// Verify round-trip
		decoded := zigzagDecode32(result)
		if decoded != tc.input {
			t.Errorf("zigzagDecode32(%d) = %d, expected %d", result, decoded, tc.input)
		}
	}
}

func TestEncodeInt32ArrayGroupVB_Empty(t *testing.T) {
	result := EncodeInt32ArrayGroupVB([]int32{})

	if len(result) != 2 {
		t.Errorf("expected 2 bytes for empty array, got %d", len(result))
	}
	if result[0] != tagArrayInt32GroupVB {
		t.Errorf("wrong tag: %02x", result[0])
	}
	if result[1] != 0 {
		t.Errorf("expected count 0, got %d", result[1])
	}

	// Decode
	decoded, _, err := DecodeInt32ArrayGroupVB(result[1:])
	if err != nil {
		t.Errorf("decode error: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty slice, got %v", decoded)
	}
}

func TestEncodeInt32ArrayGroupVB_SmallArray(t *testing.T) {
	input := []int32{1, -2, 3, -4, 5, -6, 7}

	encoded := EncodeInt32ArrayGroupVB(input)

	// Decode (skip tag byte)
	decoded, _, err := DecodeInt32ArrayGroupVB(encoded[1:])
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded) != len(input) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(input))
	}

	for i := range input {
		if decoded[i] != input[i] {
			t.Errorf("mismatch at %d: got %d, expected %d", i, decoded[i], input[i])
		}
	}
}

func TestEncodeInt32ArrayGroupVB_LargeValues(t *testing.T) {
	input := []int32{
		-2147483648, // min int32
		2147483647,  // max int32
		0,
		-1,
		1,
		1000000,
		-1000000,
		12345678,
	}

	encoded := EncodeInt32ArrayGroupVB(input)
	decoded, _, err := DecodeInt32ArrayGroupVB(encoded[1:])
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i := range input {
		if decoded[i] != input[i] {
			t.Errorf("mismatch at %d: got %d, expected %d", i, decoded[i], input[i])
		}
	}
}

func TestEncodeUint32ArrayGroupVB(t *testing.T) {
	input := []uint32{0, 1, 255, 256, 65535, 65536, 0xFFFFFF, 0xFFFFFFFF}

	encoded := EncodeUint32ArrayGroupVB(input)
	decoded, _, err := DecodeUint32ArrayGroupVB(encoded[1:])
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded) != len(input) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(input))
	}

	for i := range input {
		if decoded[i] != input[i] {
			t.Errorf("mismatch at %d: got %d, expected %d", i, decoded[i], input[i])
		}
	}
}

func TestGroupVByte_RoundTrip_Random(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		// Generate random array of random length
		length := rng.Intn(100) + 1
		input := make([]int32, length)
		for i := range input {
			input[i] = rng.Int31()
			if rng.Intn(2) == 0 {
				input[i] = -input[i]
			}
		}

		encoded := EncodeInt32ArrayGroupVB(input)
		decoded, _, err := DecodeInt32ArrayGroupVB(encoded[1:])
		if err != nil {
			t.Fatalf("trial %d: decode error: %v", trial, err)
		}

		if len(decoded) != len(input) {
			t.Fatalf("trial %d: length mismatch: %d vs %d", trial, len(decoded), len(input))
		}

		for i := range input {
			if decoded[i] != input[i] {
				t.Errorf("trial %d: mismatch at %d: got %d, expected %d", trial, i, decoded[i], input[i])
				break
			}
		}
	}
}

func TestByteCountTable(t *testing.T) {
	// Verify the precomputed byte count table
	for ctrl := 0; ctrl < 256; ctrl++ {
		expected := byte(0)
		for i := 0; i < 4; i++ {
			code := (ctrl >> (i * 2)) & 0x03
			expected += bytesPerSlot[code]
		}
		if byteCountTable[ctrl] != expected {
			t.Errorf("byteCountTable[%d] = %d, expected %d", ctrl, byteCountTable[ctrl], expected)
		}
	}
}

// Benchmarks

func BenchmarkGroupVByteEncode(b *testing.B) {
	dst := make([]byte, 17)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GroupVByteEncode(dst, 123, 45678, 901234, 56789012)
	}
}

func BenchmarkGroupVByteDecode(b *testing.B) {
	dst := make([]byte, 17)
	n := GroupVByteEncode(dst, 123, 45678, 901234, 56789012)
	src := dst[:n]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GroupVByteDecode(src)
	}
}

func BenchmarkEncodeInt32ArrayGroupVB_100(b *testing.B) {
	input := make([]int32, 100)
	for i := range input {
		input[i] = int32(i * 1000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeInt32ArrayGroupVB(input)
	}
}

func BenchmarkDecodeInt32ArrayGroupVB_100(b *testing.B) {
	input := make([]int32, 100)
	for i := range input {
		input[i] = int32(i * 1000)
	}
	encoded := EncodeInt32ArrayGroupVB(input)
	data := encoded[1:] // Skip tag
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeInt32ArrayGroupVB(data)
	}
}

func BenchmarkEncodeInt32ArrayGroupVB_1000(b *testing.B) {
	input := make([]int32, 1000)
	for i := range input {
		input[i] = int32(i * 1000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeInt32ArrayGroupVB(input)
	}
}

// Compare with standard varint encoding
func BenchmarkStandardVarint_Encode_100(b *testing.B) {
	input := make([]int32, 100)
	for i := range input {
		input[i] = int32(i * 1000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 500)
		for _, v := range input {
			buf = appendVarint(buf, int64(v))
		}
		_ = buf
	}
}

func TestGroupVByte_CompressionRatio(t *testing.T) {
	// Test compression ratio for different value distributions

	// Small values (1 byte each)
	smallValues := make([]int32, 100)
	for i := range smallValues {
		smallValues[i] = int32(i % 128)
	}
	smallEncoded := EncodeInt32ArrayGroupVB(smallValues)
	smallRaw := len(smallValues) * 4
	t.Logf("Small values: raw=%d, encoded=%d, ratio=%.2f%%",
		smallRaw, len(smallEncoded), float64(len(smallEncoded))/float64(smallRaw)*100)

	// Large values (4 bytes each)
	largeValues := make([]int32, 100)
	for i := range largeValues {
		largeValues[i] = int32(1000000 + i*10000)
	}
	largeEncoded := EncodeInt32ArrayGroupVB(largeValues)
	largeRaw := len(largeValues) * 4
	t.Logf("Large values: raw=%d, encoded=%d, ratio=%.2f%%",
		largeRaw, len(largeEncoded), float64(len(largeEncoded))/float64(largeRaw)*100)

	// Mixed values
	mixedValues := make([]int32, 100)
	for i := range mixedValues {
		switch i % 4 {
		case 0:
			mixedValues[i] = int32(i % 128)
		case 1:
			mixedValues[i] = int32(i * 100)
		case 2:
			mixedValues[i] = int32(i * 10000)
		case 3:
			mixedValues[i] = int32(i * 1000000)
		}
	}
	mixedEncoded := EncodeInt32ArrayGroupVB(mixedValues)
	mixedRaw := len(mixedValues) * 4
	t.Logf("Mixed values: raw=%d, encoded=%d, ratio=%.2f%%",
		mixedRaw, len(mixedEncoded), float64(len(mixedEncoded))/float64(mixedRaw)*100)
}

func TestGroupVByteDecode_ShortInput(t *testing.T) {
	// Test with input too short
	short := []byte{0x00, 1, 2, 3} // Missing 4th value
	v0, v1, v2, v3, n := GroupVByteDecode(short)
	if n != 0 {
		t.Errorf("expected 0 consumed for short input, got %d (values: %d, %d, %d, %d)", n, v0, v1, v2, v3)
	}
}

func TestEncodeDecodeInt32_Consistency(t *testing.T) {
	// Verify that encoding followed by decoding produces identical results
	testCases := [][]int32{
		{0},
		{1, 2, 3, 4},
		{-1, -2, -3, -4},
		{127, 128, 255, 256},
		{-128, -129, -255, -256},
		{32767, 32768, 65535, 65536},
	}

	for _, tc := range testCases {
		encoded := EncodeInt32ArrayGroupVB(tc)
		decoded, consumed, err := DecodeInt32ArrayGroupVB(encoded[1:])
		if err != nil {
			t.Errorf("decode error for %v: %v", tc, err)
			continue
		}
		if consumed == 0 && len(tc) > 0 {
			t.Errorf("no bytes consumed for %v", tc)
			continue
		}
		if !int32SlicesEqual(tc, decoded) {
			t.Errorf("mismatch: input=%v, decoded=%v", tc, decoded)
		}
	}
}

func int32SlicesEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Helper to ensure tag constant is used properly
func TestTagConstant(t *testing.T) {
	if tagArrayInt32GroupVB != 0x0F {
		t.Errorf("tag constant changed: expected 0x0F, got 0x%02x", tagArrayInt32GroupVB)
	}
}

// Test that the encoder produces deterministic output
func TestDeterministicEncoding(t *testing.T) {
	input := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	encoded1 := EncodeInt32ArrayGroupVB(input)
	encoded2 := EncodeInt32ArrayGroupVB(input)

	if !bytes.Equal(encoded1, encoded2) {
		t.Error("encoding is not deterministic")
	}
}
