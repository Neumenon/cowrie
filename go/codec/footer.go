package codec

import (
	"encoding/binary"
	"errors"
)

// RowGroupEntry is the fixed 64-byte little-endian Level-1 footer record that
// describes one row-group in the data region. Entries are append-ordered and
// therefore monotonic in both ordinal_first and abs_offset, so a single sorted
// array serves random-access lookups by global ordinal or by byte offset.
//
// On-disk layout (all multi-byte fields little-endian), 64 bytes total:
//
//	offset size field
//	  0     8   ordinal_first u64  global ordinal of first record (monotonic)
//	  8     4   record_count  u32  records in this group
//	 12     8   abs_offset    u64  byte offset of the group (≡ 0 mod AlignK)
//	 20     8   byte_len      u64  total bytes of the group
//	 28     4   type_id       u32  logical schema fingerprint (routing only)
//	 32     2   layout_id     u16  physical profile: 0 = row (Phase 1)
//	 34     2   flags         u16
//	 36     8   chunk_dir_off u64  columnar directory offset (0 = row)
//	 44    16   group_digest  [16] BLAKE3-128 truncation (zeroed in Phase 1)
//	 60     4   <reserved/pad>     zero (rounds the record to 64 bytes)
//
// Note: the design's listed fields sum to 60 bytes; the record is fixed at a
// 64-byte stride for clean alignment, with the trailing 4 bytes reserved zero.
type RowGroupEntry struct {
	OrdinalFirst uint64
	RecordCount  uint32
	AbsOffset    uint64
	ByteLen      uint64
	TypeID       uint32
	LayoutID     uint16
	Flags        uint16
	ChunkDirOff  uint64
	GroupDigest  [16]byte
}

// ErrFooterMalformed is returned when the footer byte length is not a whole
// multiple of RowGroupEntryLen.
var ErrFooterMalformed = errors.New("cowrie footer: length not a multiple of RowGroupEntry size")

// encodeInto writes this entry as a fixed 64-byte little-endian record into
// dst, which must be at least RowGroupEntryLen bytes long. It uses
// binary.LittleEndian (never unsafe) so it is alignment-safe on all platforms.
func (e *RowGroupEntry) encodeInto(dst []byte) {
	_ = dst[RowGroupEntryLen-1] // bounds-check hint
	binary.LittleEndian.PutUint64(dst[0:8], e.OrdinalFirst)
	binary.LittleEndian.PutUint32(dst[8:12], e.RecordCount)
	binary.LittleEndian.PutUint64(dst[12:20], e.AbsOffset)
	binary.LittleEndian.PutUint64(dst[20:28], e.ByteLen)
	binary.LittleEndian.PutUint32(dst[28:32], e.TypeID)
	binary.LittleEndian.PutUint16(dst[32:34], e.LayoutID)
	binary.LittleEndian.PutUint16(dst[34:36], e.Flags)
	binary.LittleEndian.PutUint64(dst[36:44], e.ChunkDirOff)
	copy(dst[44:60], e.GroupDigest[:])
	// dst[60:64] reserved; caller is responsible for zeroing the buffer.
}

// AppendRowGroupEntry encodes one entry and appends its 64 bytes to dst.
func AppendRowGroupEntry(dst []byte, e *RowGroupEntry) []byte {
	var buf [RowGroupEntryLen]byte
	e.encodeInto(buf[:])
	return append(dst, buf[:]...)
}

// decodeRowGroupEntry decodes a fixed 64-byte little-endian record from src,
// which must be at least RowGroupEntryLen bytes long.
func decodeRowGroupEntry(src []byte) RowGroupEntry {
	_ = src[RowGroupEntryLen-1] // bounds-check hint
	var e RowGroupEntry
	e.OrdinalFirst = binary.LittleEndian.Uint64(src[0:8])
	e.RecordCount = binary.LittleEndian.Uint32(src[8:12])
	e.AbsOffset = binary.LittleEndian.Uint64(src[12:20])
	e.ByteLen = binary.LittleEndian.Uint64(src[20:28])
	e.TypeID = binary.LittleEndian.Uint32(src[28:32])
	e.LayoutID = binary.LittleEndian.Uint16(src[32:34])
	e.Flags = binary.LittleEndian.Uint16(src[34:36])
	e.ChunkDirOff = binary.LittleEndian.Uint64(src[36:44])
	copy(e.GroupDigest[:], src[44:60])
	return e
}

// DecodeRowGroupEntries decodes a footer byte slice into a freshly allocated
// []RowGroupEntry via a little-endian loop (never unsafe.Slice, which would
// panic on unaligned mmapped buffers on ARM). The footer length must be a whole
// multiple of RowGroupEntryLen.
func DecodeRowGroupEntries(footer []byte) ([]RowGroupEntry, error) {
	if len(footer)%RowGroupEntryLen != 0 {
		return nil, ErrFooterMalformed
	}
	n := len(footer) / RowGroupEntryLen
	entries := make([]RowGroupEntry, n)
	for i := 0; i < n; i++ {
		off := i * RowGroupEntryLen
		entries[i] = decodeRowGroupEntry(footer[off : off+RowGroupEntryLen])
	}
	return entries, nil
}
