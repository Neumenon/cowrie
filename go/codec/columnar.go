package codec

import (
	"encoding/binary"
	"errors"
)

// ---------------------------------------------------------------------------
// Phase 2 — columnar tensor-batch (ColumnarBatchV1).
//
// type_id stays LOGICAL (row and columnar tensor-batches share the type_id
// space); layout_id dispatches physical decode. A columnar group's RowGroupEntry
// carries LayoutID = LayoutColumnarV1 and a non-zero ChunkDirOff.
// ---------------------------------------------------------------------------

const (
	// LayoutColumnarV1 is the physical layout id for a columnar tensor-batch
	// group (Phase 2), alongside LayoutRow (= 0) for row-oriented groups.
	LayoutColumnarV1 uint16 = 1

	// ColumnDescLen is the fixed on-disk size of one ColumnDesc, in bytes.
	//
	// CANONICAL (authoritative) layout — stride 64, every u64 naturally aligned,
	// digest[16] lands 4-aligned, encoded/decoded field-by-field with
	// binary.LittleEndian (NEVER unsafe.Slice — alignment-safe on ARM mmap):
	//
	//	off  size  field          type   notes
	//	  0    4   path_id        u32    advisory FNV-1a(32) of canonical path
	//	  4    4   path_off       u32    byte offset into the per-group path blob
	//	  8    2   path_len       u16    length of the path string in the blob
	//	 10    1   logical_dtype  u8     cowrie.DType (0x00 for validity bitmap)
	//	 11    1   encoding       u8     v1: 0 = PLAIN only; other => rejected
	//	 12    1   compression    u8     0 = none, 1 = zstd (PER-CHUNK)
	//	 13    1   _pad0          u8     0x00
	//	 14    2   _pad1          u16    0x00 (brings chunk_off to offset 16)
	//	 16    8   chunk_off      u64    payload-relative; ≡ 0 mod AlignK(16)
	//	 24    8   chunk_len      u64    on-disk (maybe zstd) chunk length
	//	 32    8   raw_len        u64    uncompressed chunk length
	//	 40   16   chunk_digest   [16]   BLAKE3-128 over RAW bytes (may be zero)
	//	 56    4   stats_off      u32    payload-relative offset of this column's
	//	                                 48B StatsBlock; 0 = no stats block
	//	 60    4   _pad2          u32    0x00 (rounds stride to 64)
	//	 64  = END.
	//
	// Phase 3 extends the stride 56 -> 64 (greenfield re-pin, zero users): every
	// pre-existing field keeps its byte offset (chunk_off@16 .. chunk_digest ends
	// @56), and a naturally-aligned stats_off u32 lands at 56 with 4 reserved pad
	// bytes at 60. stats_off is payload-relative and points at this column's 48B
	// uncompressed StatsBlock (ColumnStatsLen); 0 means the column carries no
	// stats. In Phase 3 only the tensor.values column ever gets a non-zero
	// stats_off; shape_tail / validity always encode 0. The stride is a power of
	// two and a multiple of AlignK(16), keeping the directory K-aligned.
	ColumnDescLen = 64

	// ColumnStatsLen is the fixed on-disk size of one StatsBlock, in bytes. It is
	// dtype-INDEPENDENT (always 48): ver/dtype/flags/pad (8) + null_count (8) +
	// nan_count (8) + min_cell (8) + max_cell (8) + pad_tail (8). The min/max
	// cells are raw 8-byte LE slots reinterpreted per stats_dtype, which is why
	// the block is fixed-width regardless of the values dtype. The block is
	// UNCOMPRESSED and lives in the payload front (before the first chunk_off).
	ColumnStatsLen = 48

	// StatsVersion1 is the only recognized StatsBlock.stats_ver; any other value
	// at read time is reported as ErrColumnarMalformed.
	StatsVersion1 uint8 = 1

	// ColumnarHeaderLen is the fixed ColumnarBatchV1 payload header size:
	//   num_columns u32 | num_rows u64 | path_blob_len u32  => 16 bytes.
	// Sized so the ColumnDesc directory (immediately after) starts 16-aligned.
	ColumnarHeaderLen = 16

	// Columnar encoding ids (ColumnDesc.encoding). v1 supports PLAIN only.
	ColumnarEncodingPlain uint8 = 0

	// Columnar per-chunk compression ids (ColumnDesc.compression).
	ColumnarCompressionNone uint8 = 0
	ColumnarCompressionZstd uint8 = 1
)

// Canonical column path strings for a fixed-shape tensor batch.
const (
	ColPathValues    = "tensor.values"
	ColPathShapeTail = "tensor.shape_tail"
	ColPathValidity  = "tensor.validity"
)

// Columnar errors.
var (
	// ErrColumnarUnsupportedEncoding is returned (at write AND read) when a
	// ColumnDesc.encoding is anything other than PLAIN (0).
	ErrColumnarUnsupportedEncoding = errors.New("cowrie columnar: encoding != PLAIN")

	// ErrColumnarUnsupportedCompression is returned when ColumnDesc.compression
	// is not one of {0 = none, 1 = zstd}.
	ErrColumnarUnsupportedCompression = errors.New("cowrie columnar: unsupported compression")

	// ErrColumnarShapeMismatch is returned at write time when the Values length
	// does not equal rows*prod(shape_tail)*elemsize.
	ErrColumnarShapeMismatch = errors.New("cowrie columnar: values length != rows*prod(shape_tail)*elemsize")

	// ErrNotColumnarGroup is returned when a group's layout_id is not COLUMNAR_V1.
	ErrNotColumnarGroup = errors.New("cowrie columnar: group layout_id is not COLUMNAR_V1")

	// ErrRecordIsColumnar is returned by SeekRecord when the target ordinal lands
	// in a columnar group; per-record gather is deferred in v2 (use OpenColumnarGroup).
	ErrRecordIsColumnar = errors.New("cowrie columnar: per-record SeekRecord not supported in v2; use OpenColumnarGroup")

	// ErrColumnarMalformed is returned when payload/directory bytes are
	// structurally invalid (truncated, out-of-range offsets, bad blob indices).
	ErrColumnarMalformed = errors.New("cowrie columnar: malformed batch payload")

	// ErrColumnarRowOverflow is returned at write time when NumRows exceeds the
	// u32 range of RowGroupEntry.record_count.
	ErrColumnarRowOverflow = errors.New("cowrie columnar: num_rows exceeds uint32 record_count")
)

// ColumnDesc is the fixed-width (ColumnDescLen) directory entry describing one
// column chunk. It is encoded/decoded field-by-field with binary.LittleEndian.
type ColumnDesc struct {
	PathID       uint32
	PathOff      uint32
	PathLen      uint16
	LogicalDType uint8
	Encoding     uint8
	Compression  uint8
	ChunkOff     uint64
	ChunkLen     uint64
	RawLen       uint64
	ChunkDigest  [16]byte
	StatsOff     uint32
}

// encodeInto writes this descriptor as a fixed ColumnDescLen little-endian record
// into dst (>= ColumnDescLen bytes). Field-by-field, never unsafe — mirrors
// footer.go's RowGroupEntry.encodeInto, alignment-safe on all platforms. The
// caller is responsible for zeroing reserved/pad bytes (this writes them as 0).
func (d *ColumnDesc) encodeInto(dst []byte) {
	_ = dst[ColumnDescLen-1] // bounds-check hint
	binary.LittleEndian.PutUint32(dst[0:4], d.PathID)
	binary.LittleEndian.PutUint32(dst[4:8], d.PathOff)
	binary.LittleEndian.PutUint16(dst[8:10], d.PathLen)
	dst[10] = d.LogicalDType
	dst[11] = d.Encoding
	dst[12] = d.Compression
	dst[13] = 0 // _pad0
	dst[14] = 0 // _pad1 (lo)
	dst[15] = 0 // _pad1 (hi)
	binary.LittleEndian.PutUint64(dst[16:24], d.ChunkOff)
	binary.LittleEndian.PutUint64(dst[24:32], d.ChunkLen)
	binary.LittleEndian.PutUint64(dst[32:40], d.RawLen)
	copy(dst[40:56], d.ChunkDigest[:])
	// stats_off @56: payload-relative offset of this column's 48B StatsBlock, or 0.
	binary.LittleEndian.PutUint32(dst[56:60], d.StatsOff)
	dst[60] = 0 // _pad2
	dst[61] = 0
	dst[62] = 0
	dst[63] = 0
}

// decodeColumnDesc decodes a fixed ColumnDescLen little-endian record from src
// (>= ColumnDescLen bytes) via a field-by-field loop (never unsafe.Slice, which
// would panic on unaligned mmapped buffers on ARM).
func decodeColumnDesc(src []byte) ColumnDesc {
	_ = src[ColumnDescLen-1] // bounds-check hint
	var d ColumnDesc
	d.PathID = binary.LittleEndian.Uint32(src[0:4])
	d.PathOff = binary.LittleEndian.Uint32(src[4:8])
	d.PathLen = binary.LittleEndian.Uint16(src[8:10])
	d.LogicalDType = src[10]
	d.Encoding = src[11]
	d.Compression = src[12]
	// src[13:16] are _pad0/_pad1 — ignored.
	d.ChunkOff = binary.LittleEndian.Uint64(src[16:24])
	d.ChunkLen = binary.LittleEndian.Uint64(src[24:32])
	d.RawLen = binary.LittleEndian.Uint64(src[32:40])
	copy(d.ChunkDigest[:], src[40:56])
	// stats_off @56 (src[60:64] are _pad2 — ignored).
	d.StatsOff = binary.LittleEndian.Uint32(src[56:60])
	return d
}

// AppendColumnDesc encodes one descriptor and appends its ColumnDescLen bytes to dst.
func AppendColumnDesc(dst []byte, d *ColumnDesc) []byte {
	var buf [ColumnDescLen]byte
	d.encodeInto(buf[:])
	return append(dst, buf[:]...)
}

// align16 rounds n up to the next multiple of AlignK (16).
func align16(n uint64) uint64 {
	return (n + uint64(AlignK) - 1) &^ (uint64(AlignK) - 1)
}

// fnv1a32 computes the 32-bit FNV-1a hash of s (advisory path_id; routing/debug).
func fnv1a32(s string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}
