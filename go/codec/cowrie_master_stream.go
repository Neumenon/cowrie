package codec

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/Neumenon/cowrie/go/v2"
	"lukechampine.com/blake3"
)

// Master Stream wire format constants
const (
	// MasterMagic is the 4-byte magic header for Cowrie stream frames.
	// It is reused both as the 8-byte file preamble magic and as the
	// per-frame magic (needed for crash-recovery scans and streaming
	// self-description).
	MasterMagic0 = 'S'
	MasterMagic1 = 'J'
	MasterMagic2 = 'S'
	MasterMagic3 = 'T'

	// PreambleLen is the size of the file preamble written once at byte 0.
	//   magic 'SJST' (4) | format_version u16 (LE) | file_flags u16 (LE)
	PreambleLen = 8

	// FormatVersion is the Cowrie v2 file format version. It lives ONCE in
	// the preamble, not on every frame.
	FormatVersion = 1

	// FrameHeaderLen is the fixed per-frame header size (24 bytes).
	FrameHeaderLen = 24

	// AlignK is the frame/chunk alignment constant. Every frame is zero-padded
	// (0x00) so that its abs_offset ≡ 0 mod AlignK. Pinned spec constant.
	AlignK = 16

	// RowGroupRecords is the fixed number of records per row-group. This is a
	// pinned spec constant (determinism requires it be fixed, not configurable).
	RowGroupRecords = 65536

	// RowGroupEntryLen is the fixed on-disk size of one Level-1 RowGroupEntry.
	RowGroupEntryLen = 64

	// LocatorLen is the fixed size of the EOF footer locator:
	//   footer_len u64 (8) | footer_digest BLAKE3-256 (32) | magic 'SJST' (4).
	LocatorLen = 44

	// LayoutRow is the physical layout id for a row-oriented group (Phase 1).
	LayoutRow = 0
)

// Frame kinds. The per-frame "frame_kind" byte replaces the old per-frame
// "ver" byte at the same header offset. Only data frames are emitted in this
// phase; checkpoint-index is reserved for the streaming fallback path.
const (
	// FrameKindData is an ordinary self-describing data frame.
	FrameKindData = 0x00

	// FrameKindCheckpointIndex is an inline checkpoint-index frame (streaming
	// fallback). Reserved; not emitted in this phase.
	FrameKindCheckpointIndex = 0x01
)

// Master stream frame flags
const (
	// FlagMasterCompressed indicates the payload is compressed.
	FlagMasterCompressed = 0x01

	// FlagMasterCRC indicates a CRC32 checksum follows the payload.
	FlagMasterCRC = 0x02

	// FlagMasterDeterministic indicates the payload was encoded deterministically.
	FlagMasterDeterministic = 0x04

	// FlagMasterMeta indicates metadata is present before the payload.
	FlagMasterMeta = 0x08

	// Compression type bits (bits 4-5)
	FlagMasterCompGzip = 0x10 // compression = gzip
	FlagMasterCompZstd = 0x20 // compression = zstd
)

// Master stream errors
var (
	ErrMasterInvalidMagic     = errors.New("cowrie master: invalid magic")
	ErrMasterInvalidFrameKind = errors.New("cowrie master: unsupported frame kind")
	ErrMasterCRCMismatch      = errors.New("cowrie master: CRC mismatch")
	ErrMasterTruncated        = errors.New("cowrie master: truncated frame")

	// ErrMasterNotCowrieFile is returned when the file preamble is missing or
	// does not carry the expected 'SJST' magic.
	ErrMasterNotCowrieFile = errors.New("cowrie master: not a cowrie file (bad preamble magic)")

	// ErrMasterUnsupportedFormat is returned when the preamble format_version
	// is not understood by this reader.
	ErrMasterUnsupportedFormat = errors.New("cowrie master: unsupported format version")

	// ErrMasterClosed is returned when Write is called after Close.
	ErrMasterClosed = errors.New("cowrie master: writer closed")
)

// MasterFrameHeader represents a parsed master stream frame header.
type MasterFrameHeader struct {
	FrameKind   uint8
	Flags       uint8
	HeaderLen   uint16
	TypeID      uint32 // Schema fingerprint (low 32 bits)
	PayloadLen  uint32
	RawLen      uint32 // 0 if not compressed
	MetaLen     uint32
	Compression cowrie.Compression
}

// MasterWriterOptions configures the MasterWriter.
type MasterWriterOptions struct {
	// Deterministic encodes with sorted keys for reproducible output.
	Deterministic bool

	// Compression specifies the compression algorithm.
	Compression cowrie.Compression

	// EnableCRC enables CRC32 checksum for data integrity.
	EnableCRC bool

	// TypeRegistry maps type IDs to handlers for type routing.
	TypeRegistry map[uint32]string
}

// DefaultMasterWriterOptions returns sensible defaults.
func DefaultMasterWriterOptions() MasterWriterOptions {
	return MasterWriterOptions{
		Deterministic: true,
		Compression:   cowrie.CompressionZstd,
		EnableCRC:     true,
	}
}

// MasterWriter writes Cowrie master stream frames.
//
// In addition to emitting frames it accumulates the Phase 1 random-access index
// in memory: per-record offsets (Level-2) for the current row-group and the
// list of RowGroupEntry (Level-1). The footer + EOF locator are emitted by
// Close(). A group is closed when it reaches RowGroupRecords frames OR when the
// frame's type_id changes (a row-group is a single type_id). Every frame is
// zero-padded so its abs_offset ≡ 0 mod AlignK.
type MasterWriter struct {
	w             io.Writer
	opts          MasterWriterOptions
	wrotePreamble bool
	closed        bool

	// absOffset is the current global byte offset in the file (bytes written).
	absOffset uint64

	// globalOrdinal is the count of records written so far across all groups.
	globalOrdinal uint64

	// Level-1: accumulated finished row-group entries.
	groups []RowGroupEntry

	// Level-2: current (open) row-group state.
	groupOpen    bool
	groupStart   uint64   // abs_offset of the current group
	groupTypeID  uint32   // single type_id of the current group
	groupOffsets []uint32 // per-record delta offsets from groupStart
}

// NewMasterWriter creates a new master stream writer.
func NewMasterWriter(w io.Writer, opts MasterWriterOptions) *MasterWriter {
	return &MasterWriter{w: w, opts: opts}
}

// writeTracked writes b to the underlying writer and advances absOffset.
func (mw *MasterWriter) writeTracked(b []byte) error {
	n, err := mw.w.Write(b)
	mw.absOffset += uint64(n)
	return err
}

// padToAlign writes 0x00 pad bytes so the next write lands at an offset that is
// ≡ 0 mod AlignK. The pad is written BEFORE the frame header.
func (mw *MasterWriter) padToAlign() error {
	pad := (AlignK - (mw.absOffset % AlignK)) % AlignK
	if pad == 0 {
		return nil
	}
	var zeros [AlignK]byte
	return mw.writeTracked(zeros[:pad])
}

// writePreamble emits the 8-byte file preamble exactly once, before the first
// frame. Layout: magic 'SJST' (4) | format_version u16 LE | file_flags u16 LE.
func (mw *MasterWriter) writePreamble() error {
	if mw.wrotePreamble {
		return nil
	}
	var pre [PreambleLen]byte
	pre[0] = MasterMagic0
	pre[1] = MasterMagic1
	pre[2] = MasterMagic2
	pre[3] = MasterMagic3
	binary.LittleEndian.PutUint16(pre[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(pre[6:8], 0) // file_flags = 0
	if err := mw.writeTracked(pre[:]); err != nil {
		return err
	}
	mw.wrotePreamble = true
	return nil
}

// flushGroup closes the currently open row-group: it writes the Level-2
// record_offsets table (LE u32 array) at the group's end, computes the group's
// byte_len, and appends a Level-1 RowGroupEntry. It is a no-op if no group is
// open. group_digest is zeroed and layout_id=0 (row) / chunk_dir_off=0 in
// Phase 1.
func (mw *MasterWriter) flushGroup() error {
	if !mw.groupOpen || len(mw.groupOffsets) == 0 {
		mw.groupOpen = false
		return nil
	}
	// Level-2: record_offsets table (LE u32 array) at the group's end.
	tbl := make([]byte, 0, len(mw.groupOffsets)*4)
	for _, off := range mw.groupOffsets {
		tbl = binary.LittleEndian.AppendUint32(tbl, off)
	}
	if err := mw.writeTracked(tbl); err != nil {
		return err
	}

	byteLen := mw.absOffset - mw.groupStart
	count := uint32(len(mw.groupOffsets))
	mw.groups = append(mw.groups, RowGroupEntry{
		OrdinalFirst: mw.globalOrdinal - uint64(count),
		RecordCount:  count,
		AbsOffset:    mw.groupStart,
		ByteLen:      byteLen,
		TypeID:       mw.groupTypeID,
		LayoutID:     LayoutRow,
		Flags:        0,
		ChunkDirOff:  0,
		// GroupDigest zeroed in Phase 1.
	})

	mw.groupOpen = false
	mw.groupOffsets = mw.groupOffsets[:0]
	return nil
}

// Write encodes a value and writes it as a master stream frame.
func (mw *MasterWriter) Write(v any) error {
	return mw.WriteWithMeta(v, nil)
}

// WriteWithMeta encodes a value with optional metadata.
func (mw *MasterWriter) WriteWithMeta(v any, meta *cowrie.Value) error {
	if mw.closed {
		return ErrMasterClosed
	}

	// Emit the file preamble once, before any frame.
	if err := mw.writePreamble(); err != nil {
		return err
	}

	// Convert to Cowrie value
	cowrieVal := toCowrieValue(v)

	// Compute schema fingerprint for type routing
	typeID := cowrie.SchemaFingerprint32(cowrieVal)

	// Build encode options
	encOpts := cowrie.EncodeOptions{Deterministic: mw.opts.Deterministic}

	// Encode payload
	payload, err := cowrie.EncodeWithOptions(cowrieVal, encOpts)
	if err != nil {
		return err
	}

	// Encode metadata if present
	var metaBytes []byte
	if meta != nil {
		metaBytes, err = cowrie.EncodeWithOptions(meta, encOpts)
		if err != nil {
			return err
		}
	}

	// Build flags
	var flags uint8
	if mw.opts.Deterministic {
		flags |= FlagMasterDeterministic
	}
	if mw.opts.EnableCRC {
		flags |= FlagMasterCRC
	}
	if len(metaBytes) > 0 {
		flags |= FlagMasterMeta
	}

	// Compress payload if configured
	rawLen := uint32(0)
	if mw.opts.Compression != cowrie.CompressionNone && len(payload) >= 256 {
		compressed, compErr := compressPayload(payload, mw.opts.Compression)
		if compErr == nil && len(compressed) < len(payload) {
			rawLen = uint32(len(payload))
			payload = compressed
			flags |= FlagMasterCompressed
			switch mw.opts.Compression {
			case cowrie.CompressionGzip:
				flags |= FlagMasterCompGzip
			case cowrie.CompressionZstd:
				flags |= FlagMasterCompZstd
			}
		}
	}

	// Header length (fixed at 24 bytes for v2):
	// magic(4) + frame_kind(1) + flags(1) + hdrLen(2) + typeID(4) +
	// payloadLen(4) + rawLen(4) + metaLen(4) = 24
	hdrLen := uint16(FrameHeaderLen)

	// Build frame
	frame := make([]byte, 0, int(hdrLen)+len(metaBytes)+len(payload)+4)

	// Write header
	frame = append(frame, MasterMagic0, MasterMagic1, MasterMagic2, MasterMagic3)
	frame = append(frame, FrameKindData) // frame_kind (was per-frame version)
	frame = append(frame, flags)
	frame = binary.LittleEndian.AppendUint16(frame, hdrLen)
	frame = binary.LittleEndian.AppendUint32(frame, typeID)
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(payload)))
	frame = binary.LittleEndian.AppendUint32(frame, rawLen)
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(metaBytes)))

	// Write metadata
	frame = append(frame, metaBytes...)

	// Write payload
	frame = append(frame, payload...)

	// Write CRC if enabled
	if mw.opts.EnableCRC {
		checksum := crc32.ChecksumIEEE(frame)
		frame = binary.LittleEndian.AppendUint32(frame, checksum)
	}

	// --- Random-access index management (Phase 1) ---

	// A row-group is a single type_id. Close the open group if the type_id
	// changes or the group is full, BEFORE this frame is padded/written.
	if mw.groupOpen {
		if mw.groupTypeID != typeID || len(mw.groupOffsets) >= RowGroupRecords {
			if err := mw.flushGroup(); err != nil {
				return err
			}
		}
	}

	// Pad so this frame's abs_offset ≡ 0 mod AlignK (pad BEFORE the header).
	if err := mw.padToAlign(); err != nil {
		return err
	}

	// Open a new group if needed; its start is this aligned offset.
	if !mw.groupOpen {
		mw.groupOpen = true
		mw.groupStart = mw.absOffset
		mw.groupTypeID = typeID
	}

	// Record the Level-2 delta offset (from group start) for this frame.
	mw.groupOffsets = append(mw.groupOffsets, uint32(mw.absOffset-mw.groupStart))
	mw.globalOrdinal++

	return mw.writeTracked(frame)
}

// Close flushes the final partial row-group, writes the Level-1 RowGroupEntry
// footer, computes the BLAKE3-256 footer digest, and appends the 44-byte EOF
// locator (footer_len u64 | digest[32] | 'SJST'). It also emits the preamble if
// no frames were written (yielding a valid empty stream with footer_len=0).
// Close is idempotent.
func (mw *MasterWriter) Close() error {
	if mw.closed {
		return nil
	}

	// Ensure the preamble exists even for an empty stream.
	if err := mw.writePreamble(); err != nil {
		return err
	}

	// Flush the last partial group.
	if err := mw.flushGroup(); err != nil {
		return err
	}

	// Build the footer (Level-1 directory): all RowGroupEntry, 64B each, LE.
	footer := make([]byte, 0, len(mw.groups)*RowGroupEntryLen)
	for i := range mw.groups {
		footer = AppendRowGroupEntry(footer, &mw.groups[i])
	}
	if err := mw.writeTracked(footer); err != nil {
		return err
	}

	footerLen := uint64(len(footer))
	digest := blake3.Sum256(footer)

	// EOF locator: footer_len u64 | footer_digest[32] | magic 'SJST'.
	var loc [LocatorLen]byte
	binary.LittleEndian.PutUint64(loc[0:8], footerLen)
	copy(loc[8:40], digest[:])
	loc[40] = MasterMagic0
	loc[41] = MasterMagic1
	loc[42] = MasterMagic2
	loc[43] = MasterMagic3
	if err := mw.writeTracked(loc[:]); err != nil {
		return err
	}

	mw.closed = true
	return nil
}

// compressPayload compresses data using the specified algorithm.
func compressPayload(data []byte, comp cowrie.Compression) ([]byte, error) {
	// Use the existing compression from cowrie package
	switch comp {
	case cowrie.CompressionGzip:
		return compressGzip(data)
	case cowrie.CompressionZstd:
		return compressZstd(data)
	default:
		return data, nil
	}
}

// MasterReaderOptions configures the MasterReader.
type MasterReaderOptions struct {
	// TypeHandlers maps type IDs to decode functions.
	TypeHandlers map[uint32]func(*cowrie.Value) (any, error)

	// MaxDecompressedSize limits decompression (prevents bombs).
	MaxDecompressedSize int
}

// DefaultMasterReaderOptions returns sensible defaults.
func DefaultMasterReaderOptions() MasterReaderOptions {
	return MasterReaderOptions{
		MaxDecompressedSize: 100 * 1024 * 1024, // 100 MB
	}
}

// MasterReader reads Cowrie master stream frames.
type MasterReader struct {
	data         []byte
	pos          int
	opts         MasterReaderOptions
	readPreamble bool

	// dataEnd marks the end of the frame data region. For a finalized stream
	// (one carrying a valid EOF locator) this is footer_start, so the
	// sequential reader stops cleanly before the footer rather than trying to
	// decode index bytes as frames. For a non-finalized stream it is len(data).
	dataEnd int
}

// NewMasterReader creates a new master stream reader.
func NewMasterReader(data []byte, opts MasterReaderOptions) *MasterReader {
	end := len(data)
	// If the stream is finalized (valid trailing locator), bound the data
	// region at footer_start so Next() never walks into the footer/locator.
	if fs, ok := finalizedDataEnd(data); ok {
		end = fs
	}
	return &MasterReader{data: data, opts: opts, dataEnd: end}
}

// finalizedDataEnd reports footer_start if data ends with a structurally valid
// EOF locator (trailing 'SJST' magic and an in-bounds footer_len). It does not
// verify the BLAKE3 digest; that is the IndexedReader's job.
func finalizedDataEnd(data []byte) (int, bool) {
	if len(data) < PreambleLen+LocatorLen {
		return 0, false
	}
	loc := data[len(data)-LocatorLen:]
	if loc[40] != MasterMagic0 || loc[41] != MasterMagic1 ||
		loc[42] != MasterMagic2 || loc[43] != MasterMagic3 {
		return 0, false
	}
	footerLen := binary.LittleEndian.Uint64(loc[0:8])
	footerStart := uint64(len(data)) - LocatorLen - footerLen
	if footerStart < PreambleLen || footerStart > uint64(len(data)-LocatorLen) {
		return 0, false
	}
	return int(footerStart), true
}

// MasterFrame represents a decoded master stream frame.
type MasterFrame struct {
	Header  MasterFrameHeader
	Meta    *cowrie.Value // nil if no metadata
	Payload *cowrie.Value
	TypeID  uint32
}

// ensurePreamble reads and validates the 8-byte file preamble exactly once,
// on the first read. It validates the 'SJST' magic and format_version == 1,
// returning a typed error otherwise.
func (mr *MasterReader) ensurePreamble() error {
	if mr.readPreamble {
		return nil
	}
	if len(mr.data)-mr.pos < PreambleLen {
		return ErrMasterNotCowrieFile
	}
	if mr.data[mr.pos] != MasterMagic0 ||
		mr.data[mr.pos+1] != MasterMagic1 ||
		mr.data[mr.pos+2] != MasterMagic2 ||
		mr.data[mr.pos+3] != MasterMagic3 {
		return ErrMasterNotCowrieFile
	}
	formatVersion := binary.LittleEndian.Uint16(mr.data[mr.pos+4 : mr.pos+6])
	if formatVersion != FormatVersion {
		return ErrMasterUnsupportedFormat
	}
	// file_flags (mr.data[pos+6:pos+8]) reserved = 0; ignored this phase.
	mr.pos += PreambleLen
	mr.readPreamble = true
	return nil
}

// Next reads the next frame from the stream.
// Returns io.EOF when no more frames.
func (mr *MasterReader) Next() (*MasterFrame, error) {
	// Validate + consume the file preamble on the first read.
	if !mr.readPreamble {
		if err := mr.ensurePreamble(); err != nil {
			return nil, err
		}
	}

	// Skip 0x00 alignment pad bytes written before each frame header so that
	// abs_offset ≡ 0 mod AlignK. Pad never appears inside a frame body here
	// because we are positioned at a frame boundary.
	for mr.pos < mr.dataEnd && mr.data[mr.pos] == 0x00 {
		mr.pos++
	}

	if mr.pos >= mr.dataEnd {
		return nil, io.EOF
	}

	// Need at least 4 bytes to validate the per-frame magic.
	if mr.pos+4 > mr.dataEnd {
		return nil, ErrMasterTruncated
	}

	// Every frame carries the magic for recovery-scan / streaming
	// self-description.
	if mr.data[mr.pos] != MasterMagic0 ||
		mr.data[mr.pos+1] != MasterMagic1 ||
		mr.data[mr.pos+2] != MasterMagic2 ||
		mr.data[mr.pos+3] != MasterMagic3 {
		return nil, ErrMasterInvalidMagic
	}

	return mr.readMasterFrame()
}

// readMasterFrame reads a master stream frame.
func (mr *MasterReader) readMasterFrame() (*MasterFrame, error) {
	startPos := mr.pos

	// Read header (minimum 24 bytes)
	if mr.pos+FrameHeaderLen > len(mr.data) {
		return nil, ErrMasterTruncated
	}

	// Skip magic (already verified)
	mr.pos += 4

	frameKind := mr.data[mr.pos]
	mr.pos++
	if frameKind != FrameKindData {
		// Only data frames are produced/consumed this phase.
		return nil, ErrMasterInvalidFrameKind
	}

	flags := mr.data[mr.pos]
	mr.pos++

	hdrLen := binary.LittleEndian.Uint16(mr.data[mr.pos:])
	mr.pos += 2

	typeID := binary.LittleEndian.Uint32(mr.data[mr.pos:])
	mr.pos += 4

	payloadLen := binary.LittleEndian.Uint32(mr.data[mr.pos:])
	mr.pos += 4

	rawLen := binary.LittleEndian.Uint32(mr.data[mr.pos:])
	mr.pos += 4

	metaLen := binary.LittleEndian.Uint32(mr.data[mr.pos:])
	mr.pos += 4

	// Determine compression type
	var compression cowrie.Compression
	if flags&FlagMasterCompressed != 0 {
		if flags&FlagMasterCompZstd != 0 {
			compression = cowrie.CompressionZstd
		} else if flags&FlagMasterCompGzip != 0 {
			compression = cowrie.CompressionGzip
		}
	}

	header := MasterFrameHeader{
		FrameKind:   frameKind,
		Flags:       flags,
		HeaderLen:   hdrLen,
		TypeID:      typeID,
		PayloadLen:  payloadLen,
		RawLen:      rawLen,
		MetaLen:     metaLen,
		Compression: compression,
	}

	// Skip any extra header bytes
	if hdrLen > FrameHeaderLen {
		mr.pos += int(hdrLen) - FrameHeaderLen
	}

	// Locate metadata bytes (do NOT decode yet — CRC must be verified first).
	var metaStart int
	if flags&FlagMasterMeta != 0 && metaLen > 0 {
		if mr.pos+int(metaLen) > len(mr.data) {
			return nil, ErrMasterTruncated
		}
		metaStart = mr.pos
		mr.pos += int(metaLen)
	}

	// Locate payload bytes (do NOT decode yet).
	if mr.pos+int(payloadLen) > len(mr.data) {
		return nil, ErrMasterTruncated
	}
	payloadData := mr.data[mr.pos : mr.pos+int(payloadLen)]
	mr.pos += int(payloadLen)

	// Verify CRC BEFORE decoding any trusted content (metadata or payload).
	// The writer computes crc32.ChecksumIEEE over header+meta+payload (everything
	// before the CRC uint32), which is exactly data[startPos:mr.pos] at this point.
	if flags&FlagMasterCRC != 0 {
		if mr.pos+4 > len(mr.data) {
			return nil, ErrMasterTruncated
		}
		expectedCRC := binary.LittleEndian.Uint32(mr.data[mr.pos:])
		actualCRC := crc32.ChecksumIEEE(mr.data[startPos:mr.pos])
		if actualCRC != expectedCRC {
			return nil, ErrMasterCRCMismatch
		}
		mr.pos += 4
	}

	// CRC verified (or not present) — safe to decode metadata now.
	var meta *cowrie.Value
	if flags&FlagMasterMeta != 0 && metaLen > 0 {
		var err error
		meta, err = cowrie.Decode(mr.data[metaStart : metaStart+int(metaLen)])
		if err != nil {
			return nil, err
		}
	}

	// Decompress if needed
	if flags&FlagMasterCompressed != 0 {
		limit := effectiveDecompressedLimit(mr.opts.MaxDecompressedSize)

		// Validate decompressed size
		if uint64(rawLen) > uint64(limit) {
			return nil, cowrie.ErrDecompressedTooLarge
		}

		var err error
		payloadData, err = decompressPayload(payloadData, compression, limit)
		if err != nil {
			return nil, err
		}

		// Post-decompress check: validate ACTUAL decompressed size, not just claimed rawLen.
		// A crafted payload could claim a small rawLen but decompress to a much larger size.
		if int64(len(payloadData)) > limit {
			return nil, cowrie.ErrDecompressedTooLarge
		}
	}

	// Decode payload
	payload, err := cowrie.Decode(payloadData)
	if err != nil {
		return nil, err
	}

	return &MasterFrame{
		Header:  header,
		Meta:    meta,
		Payload: payload,
		TypeID:  typeID,
	}, nil
}

// decompressPayload decompresses data using the specified algorithm.
func decompressPayload(data []byte, comp cowrie.Compression, limit int64) ([]byte, error) {
	switch comp {
	case cowrie.CompressionGzip:
		return decompressGzipWithLimit(data, limit)
	case cowrie.CompressionZstd:
		return decompressZstdWithLimit(data, limit)
	default:
		return data, nil
	}
}

// Position returns the current read position.
func (mr *MasterReader) Position() int {
	return mr.pos
}

// Remaining returns the number of bytes remaining.
func (mr *MasterReader) Remaining() int {
	return len(mr.data) - mr.pos
}

// ReadAll reads all frames from the stream.
func ReadAllMasterStream(data []byte, opts MasterReaderOptions) ([]*MasterFrame, error) {
	mr := NewMasterReader(data, opts)
	var frames []*MasterFrame
	for {
		frame, err := mr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	return frames, nil
}

// IsMasterStream checks if data starts with the master stream preamble magic.
func IsMasterStream(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return data[0] == MasterMagic0 &&
		data[1] == MasterMagic1 &&
		data[2] == MasterMagic2 &&
		data[3] == MasterMagic3
}
