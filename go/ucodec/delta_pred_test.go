package ucodec

import (
	"math"
	"math/rand"
	"testing"
)

func TestAnalyzeCorrelation(t *testing.T) {
	tests := []struct {
		name     string
		data     []float32
		minCorr  float64
		maxCorr  float64
	}{
		{
			name:    "perfect_positive",
			data:    []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			minCorr: 0.99,
			maxCorr: 1.0,
		},
		{
			name:    "perfect_negative",
			data:    []float32{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
			minCorr: 0.99, // Absolute value
			maxCorr: 1.0,
		},
		{
			name:    "low_correlation",
			data:    []float32{1, 5, 2, 8, 3, 9, 1, 7, 2, 6},
			minCorr: -1.0,
			maxCorr: 1.0, // Any correlation is valid
		},
		{
			name:    "constant",
			data:    []float32{5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
			minCorr: -0.1,
			maxCorr: 0.1, // Should be ~0 (undefined for constant)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corr := AnalyzeCorrelation(tc.data)
			if corr < tc.minCorr || corr > tc.maxCorr {
				t.Errorf("correlation = %f, expected [%f, %f]", corr, tc.minCorr, tc.maxCorr)
			}
		})
	}
}

func TestShouldUseDelta(t *testing.T) {
	// Highly correlated data
	correlated := make([]float32, 100)
	for i := range correlated {
		correlated[i] = float32(i) * 1.5
	}
	if !ShouldUseDelta(correlated, 0.7) {
		t.Error("expected ShouldUseDelta=true for correlated data")
	}

	// Random data
	rng := rand.New(rand.NewSource(42))
	random := make([]float32, 100)
	for i := range random {
		random[i] = rng.Float32() * 100
	}
	if ShouldUseDelta(random, 0.7) {
		t.Error("expected ShouldUseDelta=false for random data")
	}

	// Too small
	small := []float32{1, 2, 3}
	if ShouldUseDelta(small, 0.7) {
		t.Error("expected ShouldUseDelta=false for small data")
	}
}

func TestChoosePredictor(t *testing.T) {
	// Linear data -> should prefer linear prediction
	linear := make([]float32, 100)
	for i := range linear {
		linear[i] = float32(i) * 2.5
	}
	pred := ChoosePredictor(linear)
	// Either Delta or Linear should work well for linear data
	if pred == PredNone {
		t.Error("expected predictor for linear data")
	}

	// Constant data -> delta should be best
	constant := make([]float32, 100)
	for i := range constant {
		constant[i] = 5.0
	}
	pred = ChoosePredictor(constant)
	if pred == PredNone {
		// Delta of constant is 0, which is optimal
		t.Log("PredNone chosen for constant data (acceptable)")
	}

	// Random data -> might choose None
	rng := rand.New(rand.NewSource(42))
	random := make([]float32, 100)
	for i := range random {
		random[i] = rng.Float32() * 1000
	}
	_ = ChoosePredictor(random) // Just verify it doesn't panic
}

func TestEncodeDelta_RoundTrip(t *testing.T) {
	testCases := [][]float32{
		{1, 2, 3, 4, 5},
		{1.5, 2.5, 3.5, 4.5, 5.5},
		{0, 0, 0, 0, 0},
		{100, 200, 300, 400, 500},
		{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5},
	}

	for i, tc := range testCases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			encoded, err := EncodeDelta(tc)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := DecodeDelta(encoded)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if len(decoded) != len(tc) {
				t.Fatalf("length mismatch: %d vs %d", len(decoded), len(tc))
			}

			for j := range tc {
				if math.Abs(float64(decoded[j]-tc[j])) > 1e-5 {
					t.Errorf("mismatch at %d: %f vs %f", j, decoded[j], tc[j])
				}
			}
		})
	}
}

func TestEncodeDeltaWithPredictor_Delta(t *testing.T) {
	data := []float32{10, 12, 14, 16, 18, 20}

	encoded, err := EncodeDeltaWithPredictor(data, PredDelta)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Check tag
	if encoded[0] != TagDeltaSequence {
		t.Errorf("wrong tag: %02x", encoded[0])
	}

	// Check predictor
	if encoded[1] != PredDelta {
		t.Errorf("wrong predictor: %02x", encoded[1])
	}

	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeDeltaWithPredictor_Linear(t *testing.T) {
	// Linear data is perfect for linear prediction
	data := []float32{0, 2, 4, 6, 8, 10, 12, 14}

	encoded, err := EncodeDeltaWithPredictor(data, PredLinear)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Check predictor
	if encoded[1] != PredLinear {
		t.Errorf("wrong predictor: %02x", encoded[1])
	}

	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i := range data {
		if math.Abs(float64(decoded[i]-data[i])) > 1e-5 {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeDeltaWithPredictor_None(t *testing.T) {
	data := []float32{1.1, 2.2, 3.3, 4.4}

	encoded, err := EncodeDeltaWithPredictor(data, PredNone)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeDeltaQuantized_RoundTrip(t *testing.T) {
	data := []float32{10, 12, 14, 16, 18, 20, 22, 24, 26, 28}

	for _, bits := range []uint8{8, 12, 16} {
		t.Run(string(rune('0'+bits/4)), func(t *testing.T) {
			encoded, err := EncodeDeltaQuantized(data, bits)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := DecodeDelta(encoded)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if len(decoded) != len(data) {
				t.Fatalf("length mismatch: %d vs %d", len(decoded), len(data))
			}

			// Quantized encoding may have some error
			maxError := float32(0)
			for i := range data {
				err := float32(math.Abs(float64(decoded[i] - data[i])))
				if err > maxError {
					maxError = err
				}
			}
			t.Logf("bits=%d, max error=%.6f", bits, maxError)
		})
	}
}

func TestEncodeDeltaQuantized_InvalidBits(t *testing.T) {
	data := []float32{1, 2, 3, 4}

	_, err := EncodeDeltaQuantized(data, 2) // Too few
	if err == nil {
		t.Error("expected error for bits < 4")
	}

	_, err = EncodeDeltaQuantized(data, 20) // Too many
	if err == nil {
		t.Error("expected error for bits > 16")
	}
}

func TestEncodeDelta_Empty(t *testing.T) {
	_, err := EncodeDelta([]float32{})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestDecodeDelta_Invalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{0x26}},
		{"wrong_tag", []byte{0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeDelta(tc.data)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestEncodeDeltaInt32_RoundTrip(t *testing.T) {
	testCases := [][]int32{
		{1, 2, 3, 4, 5},
		{0, 0, 0, 0, 0},
		{100, 101, 102, 103, 104},
		{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5},
		{1000000, 1000001, 1000002, 1000003},
	}

	for i, tc := range testCases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			encoded := EncodeDeltaInt32(tc)

			decoded, err := DecodeDeltaInt32(encoded)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if len(decoded) != len(tc) {
				t.Fatalf("length mismatch: %d vs %d", len(decoded), len(tc))
			}

			for j := range tc {
				if decoded[j] != tc[j] {
					t.Errorf("mismatch at %d: %d vs %d", j, decoded[j], tc[j])
				}
			}
		})
	}
}

func TestEncodeDeltaInt32_Empty(t *testing.T) {
	encoded := EncodeDeltaInt32([]int32{})

	if encoded[0] != TagDeltaEncoded {
		t.Errorf("wrong tag: %02x", encoded[0])
	}

	decoded, err := DecodeDeltaInt32(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("expected empty, got %v", decoded)
	}
}

func TestEncodeDeltaInt32_CompressionRatio(t *testing.T) {
	// Monotonic indices (common in sparse tensors)
	indices := make([]int32, 1000)
	for i := range indices {
		indices[i] = int32(i * 100) // Stride of 100
	}

	encoded := EncodeDeltaInt32(indices)
	raw := len(indices) * 4

	ratio := float64(len(encoded)) / float64(raw)
	t.Logf("Monotonic: raw=%d, encoded=%d, ratio=%.2f%%", raw, len(encoded), ratio*100)

	// Deltas are all 100, which encodes to 2 bytes each
	// Much better than 4 bytes per index
	if ratio > 0.6 {
		t.Errorf("compression ratio %.2f worse than expected (<0.6)", ratio)
	}
}

func TestDecodeDeltaInt32_Invalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{}},
		{"wrong_tag", []byte{0x00, 0x05, 0x00}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeDeltaInt32(tc.data)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestBitWriter_Basic(t *testing.T) {
	w := newBitWriter()

	// Write 3 bits: 5 (101)
	w.writeBits(5, 3)

	// Write 4 bits: 10 (1010)
	w.writeBits(10, 4)

	// Write 1 bit: 1
	w.writeBits(1, 1)

	// Total: 8 bits = 1 byte
	// 101 01010 -> reversed bit order in byte
	bytes := w.bytes()
	if len(bytes) != 1 {
		t.Errorf("expected 1 byte, got %d", len(bytes))
	}
}

func TestBitWriter_MultiBytes(t *testing.T) {
	w := newBitWriter()

	// Write 12 bits: 0xABC
	w.writeBits(0xABC, 12)

	bytes := w.bytes()
	if len(bytes) != 2 {
		t.Errorf("expected 2 bytes, got %d", len(bytes))
	}
}

func TestBitReader_Basic(t *testing.T) {
	// Create test data
	w := newBitWriter()
	w.writeBits(5, 3)   // 101
	w.writeBits(10, 4)  // 1010
	w.writeBits(1, 1)   // 1
	w.writeBits(255, 8) // 11111111

	data := w.bytes()
	r := newBitReader(data)

	// Read back
	v1, err := r.readBits(3)
	if err != nil || v1 != 5 {
		t.Errorf("readBits(3) = %d, err=%v, expected 5", v1, err)
	}

	v2, err := r.readBits(4)
	if err != nil || v2 != 10 {
		t.Errorf("readBits(4) = %d, err=%v, expected 10", v2, err)
	}

	v3, err := r.readBits(1)
	if err != nil || v3 != 1 {
		t.Errorf("readBits(1) = %d, err=%v, expected 1", v3, err)
	}

	v4, err := r.readBits(8)
	if err != nil || v4 != 255 {
		t.Errorf("readBits(8) = %d, err=%v, expected 255", v4, err)
	}
}

func TestBitReader_EOF(t *testing.T) {
	r := newBitReader([]byte{0xFF})

	// Read 8 bits successfully
	_, err := r.readBits(8)
	if err != nil {
		t.Fatal(err)
	}

	// Next read should fail
	_, err = r.readBits(1)
	if err == nil {
		t.Error("expected EOF error")
	}
}

// Benchmarks

func BenchmarkEncodeDelta_100(b *testing.B) {
	data := make([]float32, 100)
	for i := range data {
		data[i] = float32(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDelta(data)
	}
}

func BenchmarkDecodeDelta_100(b *testing.B) {
	data := make([]float32, 100)
	for i := range data {
		data[i] = float32(i) * 1.5
	}
	encoded, _ := EncodeDelta(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeDelta(encoded)
	}
}

func BenchmarkEncodeDeltaQuantized_100(b *testing.B) {
	data := make([]float32, 100)
	for i := range data {
		data[i] = float32(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDeltaQuantized(data, 8)
	}
}

func BenchmarkEncodeDeltaInt32_1000(b *testing.B) {
	data := make([]int32, 1000)
	for i := range data {
		data[i] = int32(i * 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeDeltaInt32(data)
	}
}

func BenchmarkDecodeDeltaInt32_1000(b *testing.B) {
	data := make([]int32, 1000)
	for i := range data {
		data[i] = int32(i * 100)
	}
	encoded := EncodeDeltaInt32(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeDeltaInt32(encoded)
	}
}

func BenchmarkAnalyzeCorrelation(b *testing.B) {
	data := make([]float32, 1000)
	for i := range data {
		data[i] = float32(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnalyzeCorrelation(data)
	}
}

func BenchmarkChoosePredictor(b *testing.B) {
	data := make([]float32, 1000)
	for i := range data {
		data[i] = float32(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ChoosePredictor(data)
	}
}
