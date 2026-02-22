// Package ucodec provides unified codec utilities for tensors and quantization.
//
// This package implements sparse tensor encoding using COO (Coordinate) and
// RLE (Run-Length Encoding) hybrid formats for efficient storage of
// sparse data common in quantized ML models.
package ucodec

import (
	"encoding/binary"
	"errors"
	"math"
)

// Sparse tensor type tags
const (
	TagTensorSparse byte = 0x24 // Sparse tensor with COO/RLE encoding
	TagDeltaEncoded byte = 0x25 // Delta-encoded array
)

// Sparse format flags
const (
	FormatCOO    byte = 0x01 // Coordinate list format
	FormatCSR    byte = 0x02 // Compressed sparse row
	FormatRLE    byte = 0x04 // Run-length encoded zeros
	FormatHybrid byte = 0x08 // COO for outliers + RLE for runs
)

// SparsityThreshold is the minimum sparsity ratio to use sparse encoding.
// Sparse encoding wins when >40% zeros.
const SparsityThreshold = 0.4

// SparseTensorHeader contains metadata for sparse tensors.
type SparseTensorHeader struct {
	DType     DType    // Data type
	NDim      uint8    // Number of dimensions
	Dims      []uint64 // Dimension sizes
	NNZ       uint64   // Number of non-zeros
	Format    byte     // COO, CSR, RLE, or Hybrid
	FillValue float32  // Default value (usually 0)
}

// DType represents data types for tensor elements.
type DType uint8

const (
	DTypeFloat32 DType = iota
	DTypeFloat64
	DTypeInt8
	DTypeInt16
	DTypeInt32
	DTypeInt64
	DTypeUint8
	DTypeUint16
	DTypeUint32
	DTypeUint64
)

// DTypeSize returns the byte size of a data type.
func (d DType) Size() int {
	switch d {
	case DTypeFloat32, DTypeInt32, DTypeUint32:
		return 4
	case DTypeFloat64, DTypeInt64, DTypeUint64:
		return 8
	case DTypeInt8, DTypeUint8:
		return 1
	case DTypeInt16, DTypeUint16:
		return 2
	default:
		return 4
	}
}

// RLESegment represents a run in RLE encoding.
type RLESegment struct {
	IsZero bool   // True if this is a run of zeros
	Count  uint32 // Number of elements in this run
	Values []byte // Non-zero values (only if !IsZero)
}

// ShouldEncodeSparse determines if sparse encoding is beneficial.
// Returns true if sparsity ratio exceeds threshold.
func ShouldEncodeSparse(data []float32) bool {
	if len(data) < 16 { // Too small to benefit
		return false
	}
	zeros := 0
	for _, v := range data {
		if v == 0 {
			zeros++
		}
	}
	return float64(zeros)/float64(len(data)) > SparsityThreshold
}

// ShouldEncodeSparseFloat64 checks sparsity for float64 data.
func ShouldEncodeSparseFloat64(data []float64) bool {
	if len(data) < 16 {
		return false
	}
	zeros := 0
	for _, v := range data {
		if v == 0 {
			zeros++
		}
	}
	return float64(zeros)/float64(len(data)) > SparsityThreshold
}

// ShouldEncodeSparseInt8 checks sparsity for int8 data (quantized weights).
func ShouldEncodeSparseInt8(data []int8) bool {
	if len(data) < 16 {
		return false
	}
	zeros := 0
	for _, v := range data {
		if v == 0 {
			zeros++
		}
	}
	return float64(zeros)/float64(len(data)) > SparsityThreshold
}

// EncodeCOO encodes sparse float32 data in COO format.
// Returns indices and values arrays.
func EncodeCOO(data []float32) (indices []uint32, values []float32) {
	for i, v := range data {
		if v != 0 {
			indices = append(indices, uint32(i))
			values = append(values, v)
		}
	}
	return
}

// DecodeCOO expands COO format back to dense.
func DecodeCOO(indices []uint32, values []float32, size int) []float32 {
	result := make([]float32, size)
	for i, idx := range indices {
		if int(idx) < size {
			result[idx] = values[i]
		}
	}
	return result
}

// EncodeRLE encodes int8 data using run-length encoding for zeros.
// Optimized for quantized weights with many zero values.
func EncodeRLE(data []int8) []RLESegment {
	if len(data) == 0 {
		return nil
	}

	var segments []RLESegment
	i := 0

	for i < len(data) {
		if data[i] == 0 {
			// Count zero run
			count := 0
			for i < len(data) && data[i] == 0 {
				count++
				i++
			}
			segments = append(segments, RLESegment{
				IsZero: true,
				Count:  uint32(count),
			})
		} else {
			// Collect non-zero values
			start := i
			for i < len(data) && data[i] != 0 {
				i++
			}
			values := make([]byte, i-start)
			for j := start; j < i; j++ {
				values[j-start] = byte(data[j])
			}
			segments = append(segments, RLESegment{
				IsZero: false,
				Count:  uint32(len(values)),
				Values: values,
			})
		}
	}

	return segments
}

// DecodeRLE expands RLE segments back to int8 array.
func DecodeRLE(segments []RLESegment) []int8 {
	// Calculate total size
	total := 0
	for _, seg := range segments {
		total += int(seg.Count)
	}

	result := make([]int8, total)
	pos := 0

	for _, seg := range segments {
		if seg.IsZero {
			// Zeros are already default
			pos += int(seg.Count)
		} else {
			for _, v := range seg.Values {
				result[pos] = int8(v)
				pos++
			}
		}
	}

	return result
}

// EncodeSparseTensor encodes a sparse float32 tensor.
// Automatically chooses COO or RLE based on data patterns.
func EncodeSparseTensor(data []float32, dims []uint64) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty tensor data")
	}

	// Analyze sparsity
	zeros := 0
	for _, v := range data {
		if v == 0 {
			zeros++
		}
	}
	nnz := len(data) - zeros

	// Choose format based on sparsity pattern
	format := FormatCOO
	if hasLongZeroRuns(data) {
		format = FormatHybrid
	}

	// Estimate buffer size
	buf := make([]byte, 0, 32+nnz*8)

	// Header
	buf = append(buf, TagTensorSparse)
	buf = append(buf, byte(DTypeFloat32))
	buf = append(buf, byte(len(dims)))

	// Dimensions
	for _, d := range dims {
		buf = appendUvarint(buf, d)
	}

	// NNZ
	buf = appendUvarint(buf, uint64(nnz))

	// Format
	buf = append(buf, format)

	// Fill value (always 0 for now)
	buf = appendFloat32(buf, 0)

	// Encode based on format
	switch format {
	case FormatCOO:
		indices, values := EncodeCOO(data)
		// Write indices
		for _, idx := range indices {
			buf = appendUvarint(buf, uint64(idx))
		}
		// Write values
		for _, v := range values {
			buf = appendFloat32(buf, v)
		}

	case FormatHybrid:
		// Hybrid: RLE for zero runs, COO for non-zeros
		segments := encodeHybridFloat32(data)
		buf = appendUvarint(buf, uint64(len(segments)))
		for _, seg := range segments {
			if seg.IsZero {
				buf = append(buf, 0x00) // Zero marker
				buf = appendUvarint(buf, uint64(seg.Count))
			} else {
				buf = append(buf, 0x01) // Value marker
				buf = appendUvarint(buf, uint64(seg.Count))
				for i := 0; i < int(seg.Count); i++ {
					// Values stored in segment
					buf = append(buf, seg.Values[i*4:(i+1)*4]...)
				}
			}
		}
	}

	return buf, nil
}

// DecodeSparseTensor decodes a sparse tensor back to dense float32.
func DecodeSparseTensor(data []byte) ([]float32, []uint64, error) {
	if len(data) < 5 {
		return nil, nil, errors.New("data too short")
	}

	off := 0

	// Check tag
	if data[off] != TagTensorSparse {
		return nil, nil, errors.New("not a sparse tensor")
	}
	off++

	// DType
	dtype := DType(data[off])
	off++
	if dtype != DTypeFloat32 {
		return nil, nil, errors.New("only float32 supported currently")
	}

	// NDim
	ndim := int(data[off])
	off++

	// Dimensions
	dims := make([]uint64, ndim)
	totalSize := uint64(1)
	for i := 0; i < ndim; i++ {
		d, n, err := readUvarint(data, off)
		if err != nil {
			return nil, nil, err
		}
		dims[i] = d
		totalSize *= d
		off += n
	}

	// NNZ
	nnz, n, err := readUvarint(data, off)
	if err != nil {
		return nil, nil, err
	}
	off += n

	// Format
	format := data[off]
	off++

	// Fill value
	_, n, err = readFloat32(data, off)
	if err != nil {
		return nil, nil, err
	}
	off += n

	// Decode based on format
	result := make([]float32, totalSize)

	switch format {
	case FormatCOO:
		// Read indices
		indices := make([]uint64, nnz)
		for i := uint64(0); i < nnz; i++ {
			idx, n, err := readUvarint(data, off)
			if err != nil {
				return nil, nil, err
			}
			indices[i] = idx
			off += n
		}
		// Read values
		for i := uint64(0); i < nnz; i++ {
			v, n, err := readFloat32(data, off)
			if err != nil {
				return nil, nil, err
			}
			if indices[i] < totalSize {
				result[indices[i]] = v
			}
			off += n
		}

	case FormatHybrid:
		segCount, n, err := readUvarint(data, off)
		if err != nil {
			return nil, nil, err
		}
		off += n

		pos := 0
		for i := uint64(0); i < segCount; i++ {
			marker := data[off]
			off++
			count, n, err := readUvarint(data, off)
			if err != nil {
				return nil, nil, err
			}
			off += n

			if marker == 0x00 {
				// Zero run
				pos += int(count)
			} else {
				// Values
				for j := uint64(0); j < count; j++ {
					v, n, err := readFloat32(data, off)
					if err != nil {
						return nil, nil, err
					}
					if pos < len(result) {
						result[pos] = v
					}
					pos++
					off += n
				}
			}
		}
	}

	return result, dims, nil
}

// hasLongZeroRuns checks if data has runs of 8+ zeros (benefits from RLE).
func hasLongZeroRuns(data []float32) bool {
	runLen := 0
	for _, v := range data {
		if v == 0 {
			runLen++
			if runLen >= 8 {
				return true
			}
		} else {
			runLen = 0
		}
	}
	return false
}

// encodeHybridFloat32 creates RLE-style segments for float32 data.
func encodeHybridFloat32(data []float32) []RLESegment {
	var segments []RLESegment
	i := 0

	for i < len(data) {
		if data[i] == 0 {
			// Count zero run
			count := 0
			for i < len(data) && data[i] == 0 {
				count++
				i++
			}
			segments = append(segments, RLESegment{
				IsZero: true,
				Count:  uint32(count),
			})
		} else {
			// Collect non-zero values
			start := i
			for i < len(data) && data[i] != 0 {
				i++
			}
			// Store float32 values as bytes
			values := make([]byte, (i-start)*4)
			for j := start; j < i; j++ {
				binary.LittleEndian.PutUint32(values[(j-start)*4:], math.Float32bits(data[j]))
			}
			segments = append(segments, RLESegment{
				IsZero: false,
				Count:  uint32(i - start),
				Values: values,
			})
		}
	}

	return segments
}

// CompressionRatio calculates the compression ratio for sparse encoding.
// Returns (original_size, sparse_size, ratio).
func CompressionRatio(data []float32) (int, int, float64) {
	original := len(data) * 4 // 4 bytes per float32

	// Estimate sparse size
	zeros := 0
	for _, v := range data {
		if v == 0 {
			zeros++
		}
	}
	nnz := len(data) - zeros

	// COO: indices + values
	sparse := nnz * 8 // 4 bytes index + 4 bytes value

	// Add header overhead
	sparse += 32

	if original == 0 {
		return 0, 0, 1.0
	}

	return original, sparse, float64(sparse) / float64(original)
}

// Helper functions (duplicated from gen1 to avoid import cycle)

func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendFloat32(buf []byte, f float32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(f))
	return append(buf, tmp[:]...)
}

func readUvarint(data []byte, off int) (uint64, int, error) {
	if off >= len(data) {
		return 0, 0, errors.New("unexpected EOF")
	}
	v, n := binary.Uvarint(data[off:])
	if n <= 0 {
		return 0, 0, errors.New("invalid uvarint")
	}
	return v, n, nil
}

func readFloat32(data []byte, off int) (float32, int, error) {
	if off+4 > len(data) {
		return 0, 0, errors.New("short float32")
	}
	bits := binary.LittleEndian.Uint32(data[off : off+4])
	return math.Float32frombits(bits), 4, nil
}
