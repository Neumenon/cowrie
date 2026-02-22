package codec

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/Neumenon/cowrie"
)

// Master Stream wire format constants
const (
	// MasterMagic is the 4-byte magic header for Cowrie stream frames.
	// Distinct from Cowrie document magic ('SJ') and legacy stream ([u32 len]).
	MasterMagic0 = 'S'
	MasterMagic1 = 'J'
	MasterMagic2 = 'S'
	MasterMagic3 = 'T'

	// MasterVersion is the current master stream version.
	MasterVersion = 0x02
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
	ErrMasterInvalidMagic   = errors.New("cowrie master: invalid magic")
	ErrMasterInvalidVersion = errors.New("cowrie master: unsupported version")
	ErrMasterCRCMismatch    = errors.New("cowrie master: CRC mismatch")
	ErrMasterTruncated      = errors.New("cowrie master: truncated frame")
)

// MasterFrameHeader represents a parsed master stream frame header.
type MasterFrameHeader struct {
	Version     uint8
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
type MasterWriter struct {
	w    io.Writer
	opts MasterWriterOptions
}

// NewMasterWriter creates a new master stream writer.
func NewMasterWriter(w io.Writer, opts MasterWriterOptions) *MasterWriter {
	return &MasterWriter{w: w, opts: opts}
}

// Write encodes a value and writes it as a master stream frame.
func (mw *MasterWriter) Write(v any) error {
	return mw.WriteWithMeta(v, nil)
}

// WriteWithMeta encodes a value with optional metadata.
func (mw *MasterWriter) WriteWithMeta(v any, meta *cowrie.Value) error {
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

	// Calculate header length (fixed at 22 bytes for v2)
	// magic(4) + ver(1) + flags(1) + hdrLen(2) + typeID(4) + payloadLen(4) + rawLen(4) + metaLen(4) = 24
	hdrLen := uint16(24)

	// Build frame
	frame := make([]byte, 0, int(hdrLen)+len(metaBytes)+len(payload)+4)

	// Write header
	frame = append(frame, MasterMagic0, MasterMagic1, MasterMagic2, MasterMagic3)
	frame = append(frame, MasterVersion)
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

	_, err = mw.w.Write(frame)
	return err
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

	// AllowLegacy enables reading legacy [u32 len][payload] streams.
	AllowLegacy bool
}

// DefaultMasterReaderOptions returns sensible defaults.
func DefaultMasterReaderOptions() MasterReaderOptions {
	return MasterReaderOptions{
		MaxDecompressedSize: 100 * 1024 * 1024, // 100 MB
		AllowLegacy:         true,
	}
}

// MasterReader reads Cowrie master stream frames.
type MasterReader struct {
	data []byte
	pos  int
	opts MasterReaderOptions
}

// NewMasterReader creates a new master stream reader.
func NewMasterReader(data []byte, opts MasterReaderOptions) *MasterReader {
	return &MasterReader{data: data, opts: opts}
}

// MasterFrame represents a decoded master stream frame.
type MasterFrame struct {
	Header  MasterFrameHeader
	Meta    *cowrie.Value // nil if no metadata
	Payload *cowrie.Value
	TypeID  uint32
}

// Next reads the next frame from the stream.
// Returns io.EOF when no more frames.
func (mr *MasterReader) Next() (*MasterFrame, error) {
	if mr.pos >= len(mr.data) {
		return nil, io.EOF
	}

	// Need at least 4 bytes to detect frame type
	if mr.pos+4 > len(mr.data) {
		return nil, ErrMasterTruncated
	}

	// Check for master stream magic
	if mr.data[mr.pos] == MasterMagic0 &&
		mr.data[mr.pos+1] == MasterMagic1 &&
		mr.data[mr.pos+2] == MasterMagic2 &&
		mr.data[mr.pos+3] == MasterMagic3 {
		return mr.readMasterFrame()
	}

	// Check for legacy Cowrie document magic
	if mr.data[mr.pos] == cowrie.Magic0 &&
		mr.data[mr.pos+1] == cowrie.Magic1 {
		return mr.readLegacyDocument()
	}

	// Check for legacy stream format [u32 len][payload]
	if mr.opts.AllowLegacy {
		return mr.readLegacyStream()
	}

	return nil, ErrMasterInvalidMagic
}

// readMasterFrame reads a master stream frame.
func (mr *MasterReader) readMasterFrame() (*MasterFrame, error) {
	startPos := mr.pos

	// Read header (minimum 24 bytes)
	if mr.pos+24 > len(mr.data) {
		return nil, ErrMasterTruncated
	}

	// Skip magic (already verified)
	mr.pos += 4

	version := mr.data[mr.pos]
	mr.pos++
	if version != MasterVersion {
		return nil, ErrMasterInvalidVersion
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
		Version:     version,
		Flags:       flags,
		HeaderLen:   hdrLen,
		TypeID:      typeID,
		PayloadLen:  payloadLen,
		RawLen:      rawLen,
		MetaLen:     metaLen,
		Compression: compression,
	}

	// Skip any extra header bytes
	if hdrLen > 24 {
		mr.pos += int(hdrLen) - 24
	}

	// Read metadata
	var meta *cowrie.Value
	if flags&FlagMasterMeta != 0 && metaLen > 0 {
		if mr.pos+int(metaLen) > len(mr.data) {
			return nil, ErrMasterTruncated
		}
		var err error
		meta, err = cowrie.Decode(mr.data[mr.pos : mr.pos+int(metaLen)])
		if err != nil {
			return nil, err
		}
		mr.pos += int(metaLen)
	}

	// Read payload
	if mr.pos+int(payloadLen) > len(mr.data) {
		return nil, ErrMasterTruncated
	}
	payloadData := mr.data[mr.pos : mr.pos+int(payloadLen)]
	mr.pos += int(payloadLen)

	// Verify CRC if present
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

	// Decompress if needed
	if flags&FlagMasterCompressed != 0 {
		// Validate decompressed size
		if mr.opts.MaxDecompressedSize > 0 && rawLen > uint32(mr.opts.MaxDecompressedSize) {
			return nil, cowrie.ErrDecompressedTooLarge
		}

		var err error
		payloadData, err = decompressPayload(payloadData, compression)
		if err != nil {
			return nil, err
		}

		// Post-decompress check: validate ACTUAL decompressed size, not just claimed rawLen.
		// A crafted payload could claim a small rawLen but decompress to a much larger size.
		if mr.opts.MaxDecompressedSize > 0 && int64(len(payloadData)) > int64(mr.opts.MaxDecompressedSize) {
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

// readLegacyDocument reads a plain Cowrie document (no stream framing).
func (mr *MasterReader) readLegacyDocument() (*MasterFrame, error) {
	// Find the end of the document (requires full decode)
	payload, err := cowrie.Decode(mr.data[mr.pos:])
	if err != nil {
		return nil, err
	}

	// Re-encode to find length (not ideal, but works for compatibility)
	encoded, err := cowrie.Encode(payload)
	if err != nil {
		return nil, err
	}
	mr.pos += len(encoded)

	return &MasterFrame{
		Header: MasterFrameHeader{
			Version: MasterVersion,
		},
		Payload: payload,
		TypeID:  cowrie.SchemaFingerprint32(payload),
	}, nil
}

// readLegacyStream reads a legacy [u32 len][payload] frame.
func (mr *MasterReader) readLegacyStream() (*MasterFrame, error) {
	if mr.pos+4 > len(mr.data) {
		return nil, ErrMasterTruncated
	}

	frameLen := binary.LittleEndian.Uint32(mr.data[mr.pos:])
	mr.pos += 4

	// Zero length means end of stream
	if frameLen == 0 {
		return nil, io.EOF
	}

	if mr.pos+int(frameLen) > len(mr.data) {
		return nil, ErrMasterTruncated
	}

	payloadData := mr.data[mr.pos : mr.pos+int(frameLen)]
	mr.pos += int(frameLen)

	payload, err := cowrie.Decode(payloadData)
	if err != nil {
		return nil, err
	}

	return &MasterFrame{
		Header: MasterFrameHeader{
			Version:    MasterVersion,
			PayloadLen: frameLen,
		},
		Payload: payload,
		TypeID:  cowrie.SchemaFingerprint32(payload),
	}, nil
}

// decompressPayload decompresses data using the specified algorithm.
func decompressPayload(data []byte, comp cowrie.Compression) ([]byte, error) {
	switch comp {
	case cowrie.CompressionGzip:
		return decompressGzip(data)
	case cowrie.CompressionZstd:
		return decompressZstd(data)
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

// IsMasterStream checks if data starts with master stream magic.
func IsMasterStream(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return data[0] == MasterMagic0 &&
		data[1] == MasterMagic1 &&
		data[2] == MasterMagic2 &&
		data[3] == MasterMagic3
}

// IsLegacyStream checks if data starts with legacy stream format.
func IsLegacyStream(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// Legacy format: [u32 len][payload]
	// Check if first 4 bytes look like a reasonable length
	frameLen := binary.LittleEndian.Uint32(data)
	return frameLen > 0 && frameLen < uint32(len(data))
}

// IsCowrieDocument checks if data starts with Cowrie document magic.
// Returns false for master stream format (which also starts with 'SJ' but has 'ST' after).
func IsCowrieDocument(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// Check for Cowrie magic but NOT master stream magic
	if data[0] != cowrie.Magic0 || data[1] != cowrie.Magic1 {
		return false
	}
	// Exclude master stream format (SJST)
	if data[2] == MasterMagic2 && data[3] == MasterMagic3 {
		return false
	}
	return true
}
