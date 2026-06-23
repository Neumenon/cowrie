package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/Neumenon/cowrie/go/v2"
	"lukechampine.com/blake3"
)

// IndexedReader provides O(1)-open random access over a finalized Cowrie v2
// stream using the in-file footer index + EOF locator. It holds the whole file
// buffer (e.g. an mmapped region) plus the decoded Level-1 row-group directory.
type IndexedReader struct {
	data        []byte
	groups      []RowGroupEntry
	footerStart uint64
}

// Indexed-reader errors.
var (
	// ErrNotCowrieFile is returned when the buffer is too short or lacks the
	// trailing 'SJST' locator magic.
	ErrNotCowrieFile = errors.New("cowrie indexed: not a cowrie file")

	// ErrTruncatedLocator is returned when the locator claims a footer that
	// does not fit within the buffer.
	ErrTruncatedLocator = errors.New("cowrie indexed: truncated locator/footer")

	// ErrFooterDigestMismatch is returned when the footer bytes do not hash to
	// the digest stored in the locator.
	ErrFooterDigestMismatch = errors.New("cowrie indexed: footer digest mismatch")

	// ErrRecordOutOfRange is returned by SeekRecord when the global ordinal is
	// beyond the last record in the file.
	ErrRecordOutOfRange = errors.New("cowrie indexed: record ordinal out of range")

	// ErrGroupOutOfRange is returned by SeekGroup when the group index is
	// beyond the number of row-groups.
	ErrGroupOutOfRange = errors.New("cowrie indexed: group index out of range")

	// ErrCorruptIndex is returned when an offset inside the index resolves to
	// an out-of-bounds or malformed frame.
	ErrCorruptIndex = errors.New("cowrie indexed: corrupt index/frame")
)

// OpenIndexed opens a finalized Cowrie v2 buffer for random access. It seeks the
// trailing 44-byte EOF locator, validates the 'SJST' magic, reads footer_len +
// digest, slices the footer, verifies its BLAKE3-256 digest, and decodes the
// Level-1 RowGroupEntry directory via a little-endian loop (never unsafe.Slice).
//
// A buffer shorter than PreambleLen+LocatorLen, or lacking the trailing magic,
// is reported as ErrNotCowrieFile — never an out-of-bounds read.
func OpenIndexed(data []byte) (*IndexedReader, error) {
	if len(data) < PreambleLen+LocatorLen {
		return nil, ErrNotCowrieFile
	}

	loc := data[len(data)-LocatorLen:]
	if loc[40] != MasterMagic0 || loc[41] != MasterMagic1 ||
		loc[42] != MasterMagic2 || loc[43] != MasterMagic3 {
		return nil, ErrNotCowrieFile
	}

	footerLen := binary.LittleEndian.Uint64(loc[0:8])
	digest := loc[8:40]

	// footer_start = len - locator - footer_len. Validate it fits in bounds and
	// after the preamble (no overflow / negative).
	total := uint64(len(data))
	if footerLen > total-uint64(LocatorLen)-uint64(PreambleLen) {
		return nil, ErrTruncatedLocator
	}
	footerStart := total - uint64(LocatorLen) - footerLen
	footer := data[footerStart : footerStart+footerLen]

	// Verify the footer digest (the only mandatory read-path integrity check).
	actual := blake3.Sum256(footer)
	if !bytes.Equal(actual[:], digest) {
		return nil, ErrFooterDigestMismatch
	}

	groups, err := DecodeRowGroupEntries(footer)
	if err != nil {
		return nil, err
	}

	return &IndexedReader{
		data:        data,
		groups:      groups,
		footerStart: footerStart,
	}, nil
}

// Groups returns the decoded Level-1 row-group directory (read-only view).
func (r *IndexedReader) Groups() []RowGroupEntry {
	return r.groups
}

// NumRecords returns the total number of records indexed across all groups.
func (r *IndexedReader) NumRecords() uint64 {
	if len(r.groups) == 0 {
		return 0
	}
	last := r.groups[len(r.groups)-1]
	return last.OrdinalFirst + uint64(last.RecordCount)
}

// groupIndexForOrdinal binary-searches the (monotonic-in-ordinal_first) group
// directory for the group containing global ordinal n.
func (r *IndexedReader) groupIndexForOrdinal(n uint64) (int, bool) {
	return slices.BinarySearchFunc(r.groups, n, func(g RowGroupEntry, target uint64) int {
		if target < g.OrdinalFirst {
			return 1 // group is after the target
		}
		if target >= g.OrdinalFirst+uint64(g.RecordCount) {
			return -1 // group is before the target
		}
		return 0 // hit
	})
}

// SeekGroup returns the RowGroupEntry for group index g.
func (r *IndexedReader) SeekGroup(g uint32) (RowGroupEntry, error) {
	if int(g) >= len(r.groups) {
		return RowGroupEntry{}, ErrGroupOutOfRange
	}
	return r.groups[g], nil
}

// SeekRecord returns the raw payload bytes of the record with global ordinal n.
// It binary-searches Level-1 for the containing group, reads the Level-2
// record_offsets table to locate the frame, and returns the (decompressed)
// payload. Two seeks, never a scan.
func (r *IndexedReader) SeekRecord(n uint64) ([]byte, error) {
	idx, ok := r.groupIndexForOrdinal(n)
	if !ok {
		return nil, ErrRecordOutOfRange
	}
	g := r.groups[idx]

	// Columnar groups have no Level-2 record_offsets table; per-record gather is
	// deferred in v2. Callers must use OpenColumnarGroup for column-wise access.
	if g.LayoutID == LayoutColumnarV1 {
		return nil, ErrRecordIsColumnar
	}

	// Level-2: the record_offsets table (LE u32 array) sits at the end of the
	// group: byte_len bytes from group start, RecordCount*4 of which are table.
	localN := n - g.OrdinalFirst
	tableBytes := uint64(g.RecordCount) * 4
	if g.ByteLen < tableBytes {
		return nil, ErrCorruptIndex
	}
	tableOff := g.AbsOffset + g.ByteLen - tableBytes
	entryOff := tableOff + localN*4
	if entryOff+4 > uint64(len(r.data)) {
		return nil, ErrCorruptIndex
	}
	delta := binary.LittleEndian.Uint32(r.data[entryOff : entryOff+4])

	frameStart := g.AbsOffset + uint64(delta)
	return readFramePayload(r.data, frameStart, len(r.data))
}

// FilterByType returns the indices of all row-groups whose type_id matches
// typeID, WITHOUT reading any payloads (routing/filtering uses Level-1 only).
func (r *IndexedReader) FilterByType(typeID uint32) []uint32 {
	var out []uint32
	for i := range r.groups {
		if r.groups[i].TypeID == typeID {
			out = append(out, uint32(i))
		}
	}
	return out
}

// readFramePayload parses the frame at absolute offset start in data and returns
// its decompressed payload bytes. It verifies the per-frame magic and (when
// present) does NOT re-verify CRC here — Phase 1 random access trusts the footer
// digest and reads the payload directly. limit bounds the readable region.
func readFramePayload(data []byte, start uint64, limit int) ([]byte, error) {
	if start+uint64(FrameHeaderLen) > uint64(limit) {
		return nil, ErrCorruptIndex
	}
	s := int(start)
	if data[s] != MasterMagic0 || data[s+1] != MasterMagic1 ||
		data[s+2] != MasterMagic2 || data[s+3] != MasterMagic3 {
		return nil, ErrCorruptIndex
	}

	// Header layout: magic(4) | frame_kind(1) | flags(1) | hdr_len u16 |
	// type_id u32 | payload_len u32 | raw_len u32 | meta_len u32.
	flags := data[s+5]
	hdrLen := binary.LittleEndian.Uint16(data[s+6:])
	payloadLen := binary.LittleEndian.Uint32(data[s+12:])
	rawLen := binary.LittleEndian.Uint32(data[s+16:])
	metaLen := binary.LittleEndian.Uint32(data[s+20:])

	pos := start + uint64(hdrLen)
	// Skip metadata if present.
	if flags&FlagMasterMeta != 0 && metaLen > 0 {
		pos += uint64(metaLen)
	}
	if pos+uint64(payloadLen) > uint64(limit) {
		return nil, ErrCorruptIndex
	}
	payload := data[pos : pos+uint64(payloadLen)]

	// Decompress if needed; otherwise return the raw payload bytes.
	if flags&FlagMasterCompressed != 0 {
		comp := cowrie.CompressionNone
		if flags&FlagMasterCompZstd != 0 {
			comp = cowrie.CompressionZstd
		} else if flags&FlagMasterCompGzip != 0 {
			comp = cowrie.CompressionGzip
		}
		out, err := decompressPayload(payload, comp, int64(rawLen)+1)
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	return payload, nil
}
