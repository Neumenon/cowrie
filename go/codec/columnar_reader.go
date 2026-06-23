package codec

import (
	"encoding/binary"

	"github.com/Neumenon/cowrie/go/v2"
)

// ColumnarGroupView is the primary columnar read API: a column-wise, zero-copy
// view over a columnar tensor-batch frame's payload. Parsing is lazy — the
// directory + path blob are decoded at open time, but no chunk bytes are touched
// until a column is requested.
type ColumnarGroupView struct {
	payload    []byte // frame payload (aliases the underlying file buffer)
	numColumns uint32
	numRows    uint64
	dir        []ColumnDesc
	paths      []string // path string per directory entry
}

// ColumnView is a single column's resolved bytes plus its dtype. For PLAIN
// (compression==0) columns, raw aliases the mmapped file buffer (true zero-copy);
// for zstd columns, raw is a freshly decompressed heap buffer (copy fallback).
type ColumnView struct {
	Path       string
	DType      cowrie.DType
	raw        []byte
	compressed bool
}

// IsColumnarGroup reports whether group g uses the COLUMNAR_V1 physical layout.
func (r *IndexedReader) IsColumnarGroup(g uint32) bool {
	if int(g) >= len(r.groups) {
		return false
	}
	return r.groups[g].LayoutID == LayoutColumnarV1
}

// OpenColumnarGroup validates LayoutID==COLUMNAR_V1, slices the frame payload via
// AbsOffset..AbsOffset+ByteLen, skips the 24-byte frame header (meta_len must be
// 0), cross-checks ChunkDirOff == frameStart+FrameHeaderLen, and parses the
// header + ColumnDesc directory + path blob WITHOUT reading chunk bytes. It does
// NOT recompute frame CRC or chunk_digest (lazy integrity, preserves zero-copy).
func (r *IndexedReader) OpenColumnarGroup(g uint32) (*ColumnarGroupView, error) {
	if int(g) >= len(r.groups) {
		return nil, ErrGroupOutOfRange
	}
	grp := r.groups[g]
	if grp.LayoutID != LayoutColumnarV1 {
		return nil, ErrNotColumnarGroup
	}

	frameStart := grp.AbsOffset
	frameEnd := grp.AbsOffset + grp.ByteLen
	if frameEnd > uint64(len(r.data)) || frameStart+uint64(FrameHeaderLen) > frameEnd {
		return nil, ErrColumnarMalformed
	}

	// Validate the per-frame magic + frame_kind, and that meta_len == 0.
	hdr := r.data[frameStart : frameStart+uint64(FrameHeaderLen)]
	if hdr[0] != MasterMagic0 || hdr[1] != MasterMagic1 ||
		hdr[2] != MasterMagic2 || hdr[3] != MasterMagic3 {
		return nil, ErrColumnarMalformed
	}
	flags := hdr[5]
	metaLen := binary.LittleEndian.Uint32(hdr[20:24])
	if metaLen != 0 || flags&FlagMasterMeta != 0 {
		return nil, ErrColumnarMalformed
	}
	if flags&FlagMasterCompressed != 0 {
		// Frame-level compression is never used for columnar (per-chunk only).
		return nil, ErrColumnarMalformed
	}
	payloadLen := uint64(binary.LittleEndian.Uint32(hdr[12:16]))

	payloadStart := frameStart + uint64(FrameHeaderLen)
	if payloadStart+payloadLen > frameEnd {
		return nil, ErrColumnarMalformed
	}
	// Cross-check the footer's recorded directory offset against the payload start.
	if grp.ChunkDirOff != payloadStart {
		return nil, ErrColumnarMalformed
	}
	payload := r.data[payloadStart : payloadStart+payloadLen]

	return parseColumnarPayload(payload)
}

// parseColumnarPayload decodes the ColumnarBatchV1 header + directory + path blob.
// It validates encoding==PLAIN and known compression for every column, and that
// every chunk region lies within the payload. No chunk bytes are read.
func parseColumnarPayload(payload []byte) (*ColumnarGroupView, error) {
	if uint64(len(payload)) < uint64(ColumnarHeaderLen) {
		return nil, ErrColumnarMalformed
	}
	numColumns := binary.LittleEndian.Uint32(payload[0:4])
	numRows := binary.LittleEndian.Uint64(payload[4:12])
	pathBlobLen := uint64(binary.LittleEndian.Uint32(payload[12:16]))

	dirEnd := uint64(ColumnarHeaderLen) + uint64(ColumnDescLen)*uint64(numColumns)
	blobEnd := dirEnd + pathBlobLen
	if dirEnd > uint64(len(payload)) || blobEnd > uint64(len(payload)) {
		return nil, ErrColumnarMalformed
	}
	blob := payload[dirEnd:blobEnd]

	dir := make([]ColumnDesc, numColumns)
	paths := make([]string, numColumns)
	doff := uint64(ColumnarHeaderLen)
	for i := uint32(0); i < numColumns; i++ {
		d := decodeColumnDesc(payload[doff : doff+ColumnDescLen])
		doff += ColumnDescLen

		// Reject unsupported encoding/compression loudly at read time.
		if d.Encoding != ColumnarEncodingPlain {
			return nil, ErrColumnarUnsupportedEncoding
		}
		if d.Compression != ColumnarCompressionNone && d.Compression != ColumnarCompressionZstd {
			return nil, ErrColumnarUnsupportedCompression
		}
		// Resolve the path string from the blob.
		po := uint64(d.PathOff)
		pl := uint64(d.PathLen)
		if po+pl > uint64(len(blob)) {
			return nil, ErrColumnarMalformed
		}
		paths[i] = string(blob[po : po+pl])
		// Validate the chunk region lies within the payload.
		if d.ChunkOff%uint64(AlignK) != 0 {
			return nil, ErrColumnarMalformed
		}
		if d.ChunkOff+d.ChunkLen > uint64(len(payload)) {
			return nil, ErrColumnarMalformed
		}
		dir[i] = d
	}

	return &ColumnarGroupView{
		payload:    payload,
		numColumns: numColumns,
		numRows:    numRows,
		dir:        dir,
		paths:      paths,
	}, nil
}

// NumColumns returns the number of columns in the batch.
func (cv *ColumnarGroupView) NumColumns() int { return int(cv.numColumns) }

// NumRows returns N, the number of tensors in the batch.
func (cv *ColumnarGroupView) NumRows() uint64 { return cv.numRows }

// resolveColumn materializes a ColumnView for directory index i: it slices the
// chunk bytes from the payload and decompresses zstd chunks into a fresh buffer.
func (cv *ColumnarGroupView) resolveColumn(i int) (*ColumnView, error) {
	d := cv.dir[i]
	chunk := cv.payload[d.ChunkOff : d.ChunkOff+d.ChunkLen]

	var raw []byte
	compressed := false
	switch d.Compression {
	case ColumnarCompressionNone:
		// Zero-copy: alias the mmapped payload bytes.
		raw = chunk
	case ColumnarCompressionZstd:
		out, err := decompressZstdWithLimit(chunk, int64(d.RawLen)+1)
		if err != nil {
			return nil, err
		}
		raw = out
		compressed = true
	default:
		return nil, ErrColumnarUnsupportedCompression
	}

	return &ColumnView{
		Path:       cv.paths[i],
		DType:      cowrie.DType(d.LogicalDType),
		raw:        raw,
		compressed: compressed,
	}, nil
}

// ColumnAt returns the column view at directory index i, or nil if out of range.
func (cv *ColumnarGroupView) ColumnAt(i int) *ColumnView {
	if i < 0 || i >= len(cv.dir) {
		return nil
	}
	c, err := cv.resolveColumn(i)
	if err != nil {
		return nil
	}
	return c
}

// Column returns the column view for the given canonical path (e.g.
// "tensor.values"), and whether it was found.
func (cv *ColumnarGroupView) Column(path string) (*ColumnView, bool) {
	for i := range cv.paths {
		if cv.paths[i] == path {
			c, err := cv.resolveColumn(i)
			if err != nil {
				return nil, false
			}
			return c, true
		}
	}
	return nil, false
}

// ShapeTail decodes the shared inner shape from the tensor.shape_tail column as
// LE u64[]. Returns nil if the column is absent or malformed.
func (cv *ColumnarGroupView) ShapeTail() []uint64 {
	c, ok := cv.Column(ColPathShapeTail)
	if !ok {
		return nil
	}
	b := c.raw
	if len(b)%8 != 0 {
		return nil
	}
	out := make([]uint64, len(b)/8)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return out
}

// Validity wraps the optional tensor.validity column as a *cowrie.BitmaskData
// (LSB-first, matching types.go ordering), reporting whether it is present.
func (cv *ColumnarGroupView) Validity() (*cowrie.BitmaskData, bool) {
	c, ok := cv.Column(ColPathValidity)
	if !ok {
		return nil, false
	}
	return &cowrie.BitmaskData{Count: cv.numRows, Bits: c.raw}, true
}

// ---- ColumnView zero-copy value accessors (reuse view.go helpers) ----

// ValuesFloat32 reinterprets the column bytes as []float32. Zero-copy for PLAIN
// aligned chunks (the slice aliases the file buffer); a copy for zstd chunks (the
// decompressed buffer) or on a big-endian host. Returns ok=false on dtype
// mismatch, bad length multiple, or (defensively) misaligned base pointer.
func (c *ColumnView) ValuesFloat32() ([]float32, bool) {
	if c.DType != cowrie.DTypeFloat32 {
		return nil, false
	}
	return cowrie.ViewBytesFloat32(c.raw)
}

// ValuesFloat64 reinterprets the column bytes as []float64 (see ValuesFloat32).
func (c *ColumnView) ValuesFloat64() ([]float64, bool) {
	if c.DType != cowrie.DTypeFloat64 {
		return nil, false
	}
	return cowrie.ViewBytesFloat64(c.raw)
}

// ValuesInt32 reinterprets the column bytes as []int32 (see ValuesFloat32).
func (c *ColumnView) ValuesInt32() ([]int32, bool) {
	if c.DType != cowrie.DTypeInt32 {
		return nil, false
	}
	return cowrie.ViewBytesInt32(c.raw)
}

// ValuesInt64 reinterprets the column bytes as []int64 (see ValuesFloat32).
func (c *ColumnView) ValuesInt64() ([]int64, bool) {
	if c.DType != cowrie.DTypeInt64 {
		return nil, false
	}
	return cowrie.ViewBytesInt64(c.raw)
}

// ValuesUint8 returns the column bytes as []uint8 (identity passthrough). Valid
// for Uint8 and Bool columns (both one byte per element).
func (c *ColumnView) ValuesUint8() ([]uint8, bool) {
	if c.DType != cowrie.DTypeUint8 && c.DType != cowrie.DTypeBool {
		return nil, false
	}
	return cowrie.ViewBytesUint8(c.raw)
}

// RawBytes returns the resolved (decompressed-or-aliased) chunk bytes.
func (c *ColumnView) RawBytes() []byte { return c.raw }

// IsZeroCopy reports whether RawBytes aliases the underlying file buffer (PLAIN,
// uncompressed). False for zstd columns, whose bytes were freshly decompressed.
func (c *ColumnView) IsZeroCopy() bool { return !c.compressed }
