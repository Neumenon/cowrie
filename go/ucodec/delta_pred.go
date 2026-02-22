// Package ucodec - Predictive Delta Encoding for correlated sequences.
//
// This module implements delta encoding with prediction for sequences where
// consecutive values are correlated (embeddings, time series, coordinates).
//
// Techniques:
//   - Simple delta: x[i] - x[i-1]
//   - Linear prediction: x[i] - (2*x[i-1] - x[i-2])
//   - Adaptive: choose predictor per-segment based on variance
//
// For embedding dimensions that are highly correlated, this can achieve
// 30-50% additional compression on top of quantization.
package ucodec

import (
	"encoding/binary"
	"errors"
	"math"
)

// Predictor type constants
const (
	PredNone     byte = 0x00 // No prediction (raw values)
	PredDelta    byte = 0x01 // Simple delta: x[i] - x[i-1]
	PredLinear   byte = 0x02 // Linear prediction: x[i] - 2*x[i-1] + x[i-2]
	PredAdaptive byte = 0x03 // Adaptive per-segment
)

// TagDeltaSequence is the type tag for delta-encoded sequences.
const TagDeltaSequence byte = 0x26

// DeltaHeader contains metadata for delta-encoded sequences.
type DeltaHeader struct {
	Predictor  byte    // Prediction mode
	NumValues  uint32  // Number of values
	BaseValue  float32 // First value (anchor)
	BaseValue2 float32 // Second value (for linear prediction)
	Quantized  bool    // Whether residuals are quantized
	QuantBits  uint8   // Quantization bits (if quantized)
	Scale      float32 // Quantization scale
}

// ============================================================
// Analysis Functions
// ============================================================

// AnalyzeCorrelation computes the correlation coefficient between
// consecutive elements in a sequence. Returns a value between -1 and 1.
func AnalyzeCorrelation(data []float32) float64 {
	if len(data) < 3 {
		return 0
	}

	// Compute correlation between x[i] and x[i-1]
	n := len(data) - 1
	var sumX, sumY, sumXY, sumX2, sumY2 float64

	for i := 1; i < len(data); i++ {
		x := float64(data[i-1])
		y := float64(data[i])
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
		sumY2 += y * y
	}

	nf := float64(n)
	numerator := nf*sumXY - sumX*sumY
	denominator := math.Sqrt((nf*sumX2 - sumX*sumX) * (nf*sumY2 - sumY*sumY))

	if denominator == 0 {
		return 0
	}

	return numerator / denominator
}

// ShouldUseDelta determines if delta encoding is beneficial.
// Returns true if correlation > threshold (default 0.7).
func ShouldUseDelta(data []float32, threshold float64) bool {
	if len(data) < 8 {
		return false
	}
	corr := AnalyzeCorrelation(data)
	return math.Abs(corr) > threshold
}

// ChoosePredictor analyzes data and returns the best predictor.
func ChoosePredictor(data []float32) byte {
	if len(data) < 3 {
		return PredNone
	}

	// Compute variance of residuals for each predictor
	varDelta := computeResidualVariance(data, PredDelta)
	varLinear := computeResidualVariance(data, PredLinear)
	varNone := computeVariance(data)

	// Choose predictor with lowest residual variance
	if varDelta < varNone && varDelta <= varLinear {
		return PredDelta
	}
	if varLinear < varNone && varLinear < varDelta {
		return PredLinear
	}
	return PredNone
}

func computeVariance(data []float32) float64 {
	if len(data) == 0 {
		return 0
	}

	var sum, sumSq float64
	for _, v := range data {
		sum += float64(v)
		sumSq += float64(v) * float64(v)
	}
	n := float64(len(data))
	mean := sum / n
	return sumSq/n - mean*mean
}

func computeResidualVariance(data []float32, pred byte) float64 {
	residuals := computeResiduals(data, pred)
	return computeVariance(residuals)
}

func computeResiduals(data []float32, pred byte) []float32 {
	if len(data) == 0 {
		return nil
	}

	residuals := make([]float32, len(data))
	residuals[0] = data[0] // First value is always stored as-is

	switch pred {
	case PredDelta:
		for i := 1; i < len(data); i++ {
			residuals[i] = data[i] - data[i-1]
		}
	case PredLinear:
		if len(data) > 1 {
			residuals[1] = data[1] - data[0]
		}
		for i := 2; i < len(data); i++ {
			predicted := 2*data[i-1] - data[i-2]
			residuals[i] = data[i] - predicted
		}
	default:
		copy(residuals, data)
	}

	return residuals
}

// ============================================================
// Encoding Functions
// ============================================================

// EncodeDelta encodes a float32 sequence using delta prediction.
// Automatically chooses the best predictor.
func EncodeDelta(data []float32) ([]byte, error) {
	return EncodeDeltaWithPredictor(data, ChoosePredictor(data))
}

// EncodeDeltaWithPredictor encodes using a specific predictor.
func EncodeDeltaWithPredictor(data []float32, pred byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	residuals := computeResiduals(data, pred)

	// Estimate buffer size
	buf := make([]byte, 0, 16+len(residuals)*4)

	// Write header
	buf = append(buf, TagDeltaSequence)
	buf = append(buf, pred)

	// Number of values (uvarint)
	buf = appendUvarint(buf, uint64(len(data)))

	// Base values
	buf = appendFloat32(buf, data[0])
	if pred == PredLinear && len(data) > 1 {
		buf = appendFloat32(buf, data[1])
	}

	// Write residuals
	start := 1
	if pred == PredLinear {
		start = 2
	}

	for i := start; i < len(residuals); i++ {
		buf = appendFloat32(buf, residuals[i])
	}

	return buf, nil
}

// EncodeDeltaQuantized encodes with quantized residuals for extra compression.
func EncodeDeltaQuantized(data []float32, bits uint8) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}
	if bits < 4 || bits > 16 {
		return nil, errors.New("bits must be between 4 and 16")
	}

	pred := ChoosePredictor(data)
	residuals := computeResiduals(data, pred)

	// Find min/max for quantization (excluding base values)
	start := 1
	if pred == PredLinear {
		start = 2
	}

	var minRes, maxRes float32
	first := true
	for i := start; i < len(residuals); i++ {
		if first || residuals[i] < minRes {
			minRes = residuals[i]
		}
		if first || residuals[i] > maxRes {
			maxRes = residuals[i]
		}
		first = false
	}

	// Compute scale
	levels := float32(uint32(1)<<bits - 1)
	scale := (maxRes - minRes) / levels
	if scale == 0 {
		scale = 1
	}

	// Estimate buffer size
	bitsNeeded := (len(residuals) - start) * int(bits)
	bytesNeeded := (bitsNeeded + 7) / 8
	buf := make([]byte, 0, 32+bytesNeeded)

	// Write header
	buf = append(buf, TagDeltaSequence)
	buf = append(buf, pred|0x80) // High bit indicates quantized

	// Number of values
	buf = appendUvarint(buf, uint64(len(data)))

	// Base values
	buf = appendFloat32(buf, data[0])
	if pred == PredLinear && len(data) > 1 {
		buf = appendFloat32(buf, data[1])
	}

	// Quantization params
	buf = append(buf, bits)
	buf = appendFloat32(buf, minRes)
	buf = appendFloat32(buf, scale)

	// Write quantized residuals (bit-packed)
	bitBuf := newBitWriter()
	for i := start; i < len(residuals); i++ {
		quantized := uint32((residuals[i] - minRes) / scale)
		if quantized > uint32(levels) {
			quantized = uint32(levels)
		}
		bitBuf.writeBits(quantized, int(bits))
	}
	buf = append(buf, bitBuf.bytes()...)

	return buf, nil
}

// ============================================================
// Decoding Functions
// ============================================================

// DecodeDelta decodes a delta-encoded sequence.
func DecodeDelta(data []byte) ([]float32, error) {
	if len(data) < 6 {
		return nil, errors.New("data too short")
	}

	off := 0

	// Check tag
	if data[off] != TagDeltaSequence {
		return nil, errors.New("not a delta sequence")
	}
	off++

	// Predictor and quantized flag
	predByte := data[off]
	off++
	quantized := predByte&0x80 != 0
	pred := predByte & 0x7F

	// Number of values
	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, err
	}
	off += n

	if count == 0 {
		return []float32{}, nil
	}

	result := make([]float32, count)

	// Read base values
	base1, n, err := readFloat32(data, off)
	if err != nil {
		return nil, err
	}
	result[0] = base1
	off += n

	if pred == PredLinear && count > 1 {
		base2, n, err := readFloat32(data, off)
		if err != nil {
			return nil, err
		}
		result[1] = base2
		off += n
	}

	start := 1
	if pred == PredLinear {
		start = 2
	}

	if quantized {
		// Read quantization params
		if off >= len(data) {
			return nil, errors.New("missing quantization params")
		}
		bits := data[off]
		off++

		minRes, n, err := readFloat32(data, off)
		if err != nil {
			return nil, err
		}
		off += n

		scale, n, err := readFloat32(data, off)
		if err != nil {
			return nil, err
		}
		off += n

		// Read bit-packed residuals
		bitReader := newBitReader(data[off:])
		for i := start; i < int(count); i++ {
			q, err := bitReader.readBits(int(bits))
			if err != nil {
				return nil, err
			}
			residual := minRes + float32(q)*scale

			// Reconstruct value
			switch pred {
			case PredDelta:
				result[i] = result[i-1] + residual
			case PredLinear:
				predicted := 2*result[i-1] - result[i-2]
				result[i] = predicted + residual
			default:
				result[i] = residual
			}
		}
	} else {
		// Read raw float32 residuals
		for i := start; i < int(count); i++ {
			residual, n, err := readFloat32(data, off)
			if err != nil {
				return nil, err
			}
			off += n

			// Reconstruct value
			switch pred {
			case PredDelta:
				result[i] = result[i-1] + residual
			case PredLinear:
				predicted := 2*result[i-1] - result[i-2]
				result[i] = predicted + residual
			default:
				result[i] = residual
			}
		}
	}

	return result, nil
}

// ============================================================
// Bit-packing helpers
// ============================================================

type bitWriter struct {
	buf     []byte
	bitPos  int
	current byte
}

func newBitWriter() *bitWriter {
	return &bitWriter{
		buf:    make([]byte, 0, 64),
		bitPos: 0,
	}
}

func (w *bitWriter) writeBits(value uint32, bits int) {
	for bits > 0 {
		// How many bits can we write to current byte
		bitsAvail := 8 - w.bitPos
		bitsToWrite := bits
		if bitsToWrite > bitsAvail {
			bitsToWrite = bitsAvail
		}

		// Extract bits to write (from low end of value)
		mask := uint32((1 << bitsToWrite) - 1)
		bitsVal := byte(value & mask)

		// Write to current byte
		w.current |= bitsVal << w.bitPos
		w.bitPos += bitsToWrite
		value >>= bitsToWrite
		bits -= bitsToWrite

		// Flush if byte is full
		if w.bitPos == 8 {
			w.buf = append(w.buf, w.current)
			w.current = 0
			w.bitPos = 0
		}
	}
}

func (w *bitWriter) bytes() []byte {
	// Flush any remaining bits
	if w.bitPos > 0 {
		w.buf = append(w.buf, w.current)
	}
	return w.buf
}

type bitReader struct {
	data   []byte
	pos    int
	bitPos int
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data, pos: 0, bitPos: 0}
}

func (r *bitReader) readBits(bits int) (uint32, error) {
	var result uint32
	shift := 0

	for bits > 0 {
		if r.pos >= len(r.data) {
			return 0, errors.New("unexpected end of data")
		}

		// How many bits available in current byte
		bitsAvail := 8 - r.bitPos
		bitsToRead := bits
		if bitsToRead > bitsAvail {
			bitsToRead = bitsAvail
		}

		// Extract bits from current byte
		mask := byte((1 << bitsToRead) - 1)
		bitsVal := (r.data[r.pos] >> r.bitPos) & mask

		result |= uint32(bitsVal) << shift
		shift += bitsToRead
		r.bitPos += bitsToRead
		bits -= bitsToRead

		// Advance to next byte if needed
		if r.bitPos == 8 {
			r.pos++
			r.bitPos = 0
		}
	}

	return result, nil
}

// ============================================================
// Int32 Delta Encoding (for indices, offsets)
// ============================================================

// EncodeDeltaInt32 encodes an int32 sequence using delta encoding.
// Optimal for monotonic or slowly-changing sequences.
func EncodeDeltaInt32(data []int32) []byte {
	if len(data) == 0 {
		return []byte{TagDeltaEncoded, 0}
	}

	buf := make([]byte, 0, 8+len(data)*2) // Optimistic estimate

	buf = append(buf, TagDeltaEncoded)
	buf = appendUvarint(buf, uint64(len(data)))

	// Write first value (zigzag for signed)
	buf = appendVarint(buf, int64(data[0]))

	// Write deltas (usually small, so varint is efficient)
	for i := 1; i < len(data); i++ {
		delta := int64(data[i]) - int64(data[i-1])
		buf = appendVarint(buf, delta)
	}

	return buf
}

// DecodeDeltaInt32 decodes a delta-encoded int32 sequence.
func DecodeDeltaInt32(data []byte) ([]int32, error) {
	if len(data) < 2 {
		return nil, errors.New("data too short")
	}

	off := 0

	if data[off] != TagDeltaEncoded {
		return nil, errors.New("not a delta-encoded sequence")
	}
	off++

	count, n, err := readUvarint(data, off)
	if err != nil {
		return nil, err
	}
	off += n

	if count == 0 {
		return []int32{}, nil
	}

	result := make([]int32, count)

	// Read first value
	first, n, err := readVarint(data, off)
	if err != nil {
		return nil, err
	}
	result[0] = int32(first)
	off += n

	// Read deltas
	for i := 1; i < int(count); i++ {
		delta, n, err := readVarint(data, off)
		if err != nil {
			return nil, err
		}
		result[i] = result[i-1] + int32(delta)
		off += n
	}

	return result, nil
}

// Helper: append signed varint
func appendVarint(buf []byte, x int64) []byte {
	var tmp [10]byte
	n := binary.PutVarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

// Helper: read signed varint
func readVarint(data []byte, off int) (int64, int, error) {
	if off >= len(data) {
		return 0, 0, errors.New("unexpected EOF")
	}
	v, n := binary.Varint(data[off:])
	if n <= 0 {
		return 0, 0, errors.New("invalid varint")
	}
	return v, n, nil
}
