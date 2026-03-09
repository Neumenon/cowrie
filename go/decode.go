package cowrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"
)

// Errors
var (
	ErrInvalidMagic     = errors.New("cowrie: invalid magic bytes")
	ErrInvalidVersion   = errors.New("cowrie: unsupported version")
	ErrUnexpectedEOF    = errors.New("cowrie: unexpected end of data")
	ErrInvalidTag       = errors.New("cowrie: invalid type tag")
	ErrInvalidFieldID   = errors.New("cowrie: invalid field ID")
	ErrInvalidDType     = errors.New("cowrie: invalid tensor dtype")
	ErrInvalidImgFormat = errors.New("cowrie: invalid image format")
	ErrInvalidAudioEnc  = errors.New("cowrie: invalid audio encoding")
	ErrInvalidIDWidth   = errors.New("cowrie: invalid ID width")
	ErrInvalidDeltaOp   = errors.New("cowrie: invalid delta opcode")

	// Security limit errors
	ErrMalformedLength = errors.New("cowrie: malformed length exceeds remaining data")
	ErrDepthExceeded   = errors.New("cowrie: maximum nesting depth exceeded")
	ErrArrayTooLarge   = errors.New("cowrie: array exceeds maximum length")
	ErrObjectTooLarge  = errors.New("cowrie: object exceeds maximum field count")
	ErrStringTooLarge  = errors.New("cowrie: string exceeds maximum length")
	ErrBytesTooLarge   = errors.New("cowrie: bytes exceeds maximum length")
	ErrExtTooLarge     = errors.New("cowrie: extension payload exceeds maximum length")
	ErrUnknownExt      = errors.New("cowrie: unknown extension type (strict mode)")
	ErrInvalidVarint   = errors.New("cowrie: invalid or overflow varint encoding")
	ErrDictTooLarge    = errors.New("cowrie: dictionary exceeds maximum size")
	ErrTooManyHints    = errors.New("cowrie: too many column hints")
	ErrTrailingData    = errors.New("cowrie: trailing data after root value")
)

// Default security limits (can be overridden via DecodeOptions)
// These are generous to support ML workloads while preventing extreme allocations.
// The sanity checks (length vs remaining data) are the primary protection;
// these limits are a secondary defense against legitimately-sized but huge payloads.
const (
	DefaultMaxDepth     = 1000          // Maximum nesting depth
	DefaultMaxArrayLen  = 1_000_000     // 1M elements (tightened: was 100M)
	DefaultMaxObjectLen = 1_000_000     // 1M fields (tightened: was 10M)
	DefaultMaxStringLen = 10_000_000    // 10MB strings (tightened: was 500MB)
	DefaultMaxBytesLen  = 50_000_000    // 50MB bytes (tightened: was 1GB)
	DefaultMaxExtLen    = 1_000_000     // 1MB max extension payload (tightened: was 100MB)
	DefaultMaxDictLen   = 1_000_000     // 1M dictionary entries (tightened: was 10M)
	DefaultMaxHintCount = 10_000        // 10K column hints max
	DefaultMaxRank      = 32            // Maximum tensor rank (dimensions)
)

// UnknownExtBehavior controls how the decoder handles unknown TagExt extensions.
type UnknownExtBehavior int

const (
	// UnknownExtKeep preserves unknown extensions as UnknownExtData values (default).
	// This enables round-tripping without data loss.
	UnknownExtKeep UnknownExtBehavior = iota

	// UnknownExtSkipAsNull skips unknown extensions and returns Null.
	// Use this for callers who don't care about preserving unknown data.
	UnknownExtSkipAsNull

	// UnknownExtError returns an error when an unknown extension is encountered.
	// Use this for strict decoding where unknown data should fail.
	UnknownExtError
)

// DecodeOptions configures security limits for decoding.
// Zero values use defaults. Set to -1 for unlimited (not recommended).
type DecodeOptions struct {
	MaxDepth     int // Maximum nesting depth for arrays/objects
	MaxArrayLen  int // Maximum array element count
	MaxObjectLen int // Maximum object field count
	MaxStringLen int // Maximum string byte length
	MaxBytesLen  int // Maximum bytes length (also applies to tensor/image/audio data)
	MaxExtLen    int // Maximum extension payload length (default 100MB)
	MaxDictLen   int // Maximum dictionary size (default 10M)
	MaxHintCount int // Maximum column hints (default 10K)
	MaxRank      int // Maximum tensor rank/dimensions (default 32)

	// OnUnknownExt controls behavior when an unknown TagExt is encountered.
	// Default (zero value) is UnknownExtKeep.
	OnUnknownExt UnknownExtBehavior

	// UnsafeStrings enables zero-copy string decoding.
	// When true, decoded strings point directly into the input buffer.
	// The caller MUST NOT mutate the input []byte after decoding.
	// Default false (safe copy).
	UnsafeStrings bool

	// TensorSink, when non-nil, receives tensor data via streaming callback
	// instead of allocating a []byte for the tensor body.
	// The sink MUST consume all bytes from the Reader.
	TensorSink TensorSink
}

// TensorSink receives streamed tensor data during decode.
// When set in DecodeOptions, tensor bodies are streamed to the sink
// instead of being allocated as []byte.
type TensorSink interface {
	AcceptTensor(dtype DType, dims []uint64, data io.Reader) error
}

// DefaultDecodeOptions returns options with default security limits.
func DefaultDecodeOptions() DecodeOptions {
	return DecodeOptions{
		MaxDepth:     DefaultMaxDepth,
		MaxArrayLen:  DefaultMaxArrayLen,
		MaxObjectLen: DefaultMaxObjectLen,
		MaxStringLen: DefaultMaxStringLen,
		MaxBytesLen:  DefaultMaxBytesLen,
		MaxExtLen:    DefaultMaxExtLen,
		MaxDictLen:   DefaultMaxDictLen,
		MaxHintCount: DefaultMaxHintCount,
		MaxRank:      DefaultMaxRank,
		OnUnknownExt: UnknownExtKeep,
	}
}

// TagError provides detailed information about an invalid tag.
type TagError struct {
	Tag byte
}

func (e *TagError) Error() string {
	return "cowrie: unknown type tag 0x" + hexByte(e.Tag)
}

// hexByte converts a byte to a 2-char hex string.
func hexByte(b byte) string {
	const hex = "0123456789abcdef"
	return string([]byte{hex[b>>4], hex[b&0x0F]})
}

// isValidDType checks if the dtype is a known value.
func isValidDType(d DType) bool {
	switch d {
	case DTypeFloat32, DTypeFloat16, DTypeBFloat16, DTypeFloat64,
		DTypeInt8, DTypeInt16, DTypeInt32, DTypeInt64,
		DTypeUint8, DTypeUint16, DTypeUint32, DTypeUint64,
		DTypeBool,
		DTypeQINT4, DTypeQINT2, DTypeQINT3, DTypeTernary, DTypeBinary:
		return true
	default:
		return false
	}
}

// isValidImageFormat checks if the image format is a known value.
func isValidImageFormat(f ImageFormat) bool {
	return f >= ImageFormatJPEG && f <= ImageFormatBMP
}

// isValidAudioEncoding checks if the audio encoding is a known value.
func isValidAudioEncoding(e AudioEncoding) bool {
	return e >= AudioEncodingPCMInt16 && e <= AudioEncodingAAC
}

// isValidIDWidth checks if the ID width is a known value.
func isValidIDWidth(w IDWidth) bool {
	return w == IDWidthInt32 || w == IDWidthInt64
}

// isValidDeltaOpCode checks if the delta opcode is a known value.
func isValidDeltaOpCode(op DeltaOpCode) bool {
	return op >= DeltaOpSetField && op <= DeltaOpAppendArray
}

// Decode decodes Cowrie v2 binary data into a Value.
// Uses default security limits to prevent memory exhaustion attacks.
func Decode(data []byte) (*Value, error) {
	return DecodeWithOptions(data, DefaultDecodeOptions())
}

// DecodeWithOptions decodes Cowrie v2 with custom security limits.
func DecodeWithOptions(data []byte, opts DecodeOptions) (*Value, error) {
	// Apply defaults for zero values
	if opts.MaxDepth == 0 {
		opts.MaxDepth = DefaultMaxDepth
	}
	if opts.MaxArrayLen == 0 {
		opts.MaxArrayLen = DefaultMaxArrayLen
	}
	if opts.MaxObjectLen == 0 {
		opts.MaxObjectLen = DefaultMaxObjectLen
	}
	if opts.MaxStringLen == 0 {
		opts.MaxStringLen = DefaultMaxStringLen
	}
	if opts.MaxBytesLen == 0 {
		opts.MaxBytesLen = DefaultMaxBytesLen
	}
	if opts.MaxExtLen == 0 {
		opts.MaxExtLen = DefaultMaxExtLen
	}
	if opts.MaxDictLen == 0 {
		opts.MaxDictLen = DefaultMaxDictLen
	}
	if opts.MaxHintCount == 0 {
		opts.MaxHintCount = DefaultMaxHintCount
	}
	if opts.MaxRank == 0 {
		opts.MaxRank = DefaultMaxRank
	}

	r := &reader{data: data, opts: opts}
	return decode(r)
}

// DecodeFrom decodes from an io.Reader.
// Warning: This reads the entire input into memory. For untrusted input,
// use DecodeFromLimited to prevent OOM attacks.
func DecodeFrom(rd io.Reader) (*Value, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}

// DefaultMaxInputSize is the default size limit for DecodeFromLimited (100MB).
const DefaultMaxInputSize = 100 * 1024 * 1024

// ErrInputTooLarge is returned when input exceeds the size limit.
var ErrInputTooLarge = errors.New("cowrie: input exceeds size limit")

// DecodeFromLimited decodes from an io.Reader with a size limit.
// Returns ErrInputTooLarge if the input exceeds maxBytes.
// Use this for untrusted input to prevent OOM attacks.
//
// Example:
//
//	val, err := cowrie.DecodeFromLimited(resp.Body, 10*1024*1024) // 10MB limit
func DecodeFromLimited(rd io.Reader, maxBytes int64) (*Value, error) {
	// Read up to maxBytes+1 to detect overflow
	lr := io.LimitReader(rd, maxBytes+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrInputTooLarge
	}
	return Decode(data)
}

// DecodeFromWithOptions decodes from an io.Reader with custom security limits.
// Warning: This reads the entire input into memory. For untrusted input,
// combine with io.LimitReader or use DecodeFromLimited.
func DecodeFromWithOptions(rd io.Reader, opts DecodeOptions) (*Value, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return DecodeWithOptions(data, opts)
}

// reader wraps a byte slice for reading with security limits.
type reader struct {
	data  []byte
	pos   int
	depth int           // Current nesting depth
	opts  DecodeOptions // Security limits
}

// remaining returns the number of bytes left to read.
func (r *reader) remaining() int {
	return len(r.data) - r.pos
}

func (r *reader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, ErrUnexpectedEOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) read(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *reader) readUvarint() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.pos:])
	if n == 0 {
		return 0, ErrUnexpectedEOF
	}
	if n < 0 {
		// n < 0 means overflow (varint too large for uint64)
		return 0, ErrInvalidVarint
	}
	r.pos += n
	return v, nil
}

func (r *reader) readString() (string, error) {
	length, err := r.readUvarint()
	if err != nil {
		return "", err
	}
	// Sanity check: length can't exceed remaining data
	if length > uint64(r.remaining()) {
		return "", ErrMalformedLength
	}
	b, err := r.read(int(length))
	if err != nil {
		return "", err
	}
	if r.opts.UnsafeStrings {
		return unsafe.String(&b[0], len(b)), nil
	}
	return string(b), nil
}

// readStringWithLimit reads a string with explicit length limit check.
func (r *reader) readStringWithLimit(maxLen int) (string, error) {
	length, err := r.readUvarint()
	if err != nil {
		return "", err
	}
	// Sanity check: length can't exceed remaining data
	if length > uint64(r.remaining()) {
		return "", ErrMalformedLength
	}
	// Configurable limit check
	if maxLen > 0 && length > uint64(maxLen) {
		return "", ErrStringTooLarge
	}
	b, err := r.read(int(length))
	if err != nil {
		return "", err
	}
	if r.opts.UnsafeStrings {
		if len(b) == 0 {
			return "", nil
		}
		return unsafe.String(&b[0], len(b)), nil
	}
	return string(b), nil
}

// readBytesWithLimit reads bytes with sanity and limit checks.
func (r *reader) readBytesWithLimit(maxLen int) ([]byte, error) {
	length, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: length can't exceed remaining data
	if length > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	// Configurable limit check
	if maxLen > 0 && length > uint64(maxLen) {
		return nil, ErrBytesTooLarge
	}
	return r.read(int(length))
}

// enterNested increments depth and checks limit.
func (r *reader) enterNested() error {
	r.depth++
	if r.opts.MaxDepth > 0 && r.depth > r.opts.MaxDepth {
		return ErrDepthExceeded
	}
	return nil
}

// exitNested decrements depth.
func (r *reader) exitNested() {
	r.depth--
}

func (r *reader) readUint16LE() (uint16, error) {
	b, err := r.read(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *reader) readUint32LE() (uint32, error) {
	b, err := r.read(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) readInt32LE() (int32, error) {
	b, err := r.read(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

// zigzagDecode decodes a zigzag-encoded value.
func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

// skipHints reads and discards column hints from the stream with limit enforcement.
func skipHints(r *reader) error {
	count, err := r.readUvarint()
	if err != nil {
		return err
	}
	// Enforce hint count limit to prevent CPU spin attacks
	if r.opts.MaxHintCount > 0 && count > uint64(r.opts.MaxHintCount) {
		return ErrTooManyHints
	}
	// Sanity check: each hint is at least 3 bytes (1 field name len + 1 type + 1 flags)
	if count*3 > uint64(r.remaining()) {
		return ErrMalformedLength
	}
	for i := uint64(0); i < count; i++ {
		// Skip field name (use limit-checked version)
		if _, err := r.readStringWithLimit(r.opts.MaxStringLen); err != nil {
			return err
		}
		// Skip type (1 byte)
		if _, err := r.readByte(); err != nil {
			return err
		}
		// Skip shape
		shapeLen, err := r.readUvarint()
		if err != nil {
			return err
		}
		// Enforce rank limit (shapeLen is effectively the tensor rank)
		if r.opts.MaxRank > 0 && shapeLen > uint64(r.opts.MaxRank) {
			return ErrMalformedLength
		}
		for j := uint64(0); j < shapeLen; j++ {
			if _, err := r.readUvarint(); err != nil {
				return err
			}
		}
		// Skip flags (1 byte)
		if _, err := r.readByte(); err != nil {
			return err
		}
	}
	return nil
}

// decode reads the complete Cowrie v2 format.
func decode(r *reader) (*Value, error) {
	// Read header
	magic0, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic1, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if magic0 != Magic0 || magic1 != Magic1 {
		return nil, ErrInvalidMagic
	}

	version, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, ErrInvalidVersion
	}

	// Read flags
	flags, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Skip column hints if present (use DecodeWithHints to extract them)
	if flags&FlagHasColumnHints != 0 {
		if err := skipHints(r); err != nil {
			return nil, err
		}
	}

	// Read dictionary
	dictLen, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Enforce dictionary size limit before allocation (DoS prevention)
	if r.opts.MaxDictLen > 0 && dictLen > uint64(r.opts.MaxDictLen) {
		return nil, ErrDictTooLarge
	}
	// Sanity check: each dict entry needs at least 1 byte (length prefix)
	if dictLen > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}

	dict := make([]string, dictLen)
	for i := uint64(0); i < dictLen; i++ {
		s, err := r.readStringWithLimit(r.opts.MaxStringLen)
		if err != nil {
			return nil, err
		}
		dict[i] = s
	}

	// Decode root value
	val, err := decodeValue(r, dict)
	if err != nil {
		return nil, err
	}

	// Verify all input consumed — trailing bytes indicate corruption or concatenated data
	if r.pos != len(r.data) {
		return nil, fmt.Errorf("%w: %d unconsumed bytes at position %d", ErrTrailingData, len(r.data)-r.pos, r.pos)
	}

	return val, nil
}

// decodeValue reads a single value.
func decodeValue(r *reader, dict []string) (*Value, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, err
	}

	switch tag {
	case TagNull:
		return Null(), nil

	case TagFalse:
		return Bool(false), nil

	case TagTrue:
		return Bool(true), nil

	case TagInt64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return Int64(zigzagDecode(u)), nil

	case TagUint64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return Uint64(u), nil

	case TagFloat64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint64(b)
		return Float64(math.Float64frombits(bits)), nil

	case TagDecimal128:
		scale, err := r.readByte()
		if err != nil {
			return nil, err
		}
		coefBytes, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var coef [16]byte
		copy(coef[:], coefBytes)
		return NewDecimal128(int8(scale), coef), nil

	case TagString:
		s, err := r.readStringWithLimit(r.opts.MaxStringLen)
		if err != nil {
			return nil, err
		}
		return String(s), nil

	case TagBytes:
		b, err := r.readBytesWithLimit(r.opts.MaxBytesLen)
		if err != nil {
			return nil, err
		}
		return Bytes(b), nil

	case TagDatetime64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		nanos := int64(binary.LittleEndian.Uint64(b))
		return Datetime64(nanos), nil

	case TagUUID128:
		b, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var uuid [16]byte
		copy(uuid[:], b)
		return UUID128(uuid), nil

	case TagFloat32:
		b, err := r.read(4)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint32(b)
		return Float64(float64(math.Float32frombits(bits))), nil

	case TagBigInt:
		length, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: length can't exceed remaining data
		if length > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Enforce MaxBytesLen for bigint data
		if r.opts.MaxBytesLen > 0 && length > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		b, err := r.read(int(length))
		if err != nil {
			return nil, err
		}
		return BigInt(b), nil

	case TagArray:
		// Depth tracking
		if err := r.enterNested(); err != nil {
			return nil, err
		}
		defer r.exitNested()

		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: count can't exceed remaining bytes (each element is at least 1 byte)
		if count > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Limit check
		if r.opts.MaxArrayLen > 0 && count > uint64(r.opts.MaxArrayLen) {
			return nil, ErrArrayTooLarge
		}
		items := make([]*Value, count)
		for i := uint64(0); i < count; i++ {
			v, err := decodeValue(r, dict)
			if err != nil {
				return nil, err
			}
			items[i] = v
		}
		return Array(items...), nil

	case TagObject:
		// Depth tracking
		if err := r.enterNested(); err != nil {
			return nil, err
		}
		defer r.exitNested()

		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: count can't exceed remaining bytes (each field is at least 2 bytes: fieldID + value tag)
		if count > uint64(r.remaining()/2) {
			return nil, ErrMalformedLength
		}
		// Limit check
		if r.opts.MaxObjectLen > 0 && count > uint64(r.opts.MaxObjectLen) {
			return nil, ErrObjectTooLarge
		}
		members := make([]Member, count)
		for i := uint64(0); i < count; i++ {
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			if fieldID >= uint64(len(dict)) {
				return nil, ErrInvalidFieldID
			}
			v, err := decodeValue(r, dict)
			if err != nil {
				return nil, err
			}
			members[i] = Member{Key: dict[fieldID], Value: v}
		}
		return Object(members...), nil

	// v2.1 Extension Types
	case TagTensor:
		dtype, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if !isValidDType(DType(dtype)) {
			return nil, ErrInvalidDType
		}
		rank, err := r.readByte()
		if err != nil {
			return nil, err
		}
		// Enforce rank limit
		if r.opts.MaxRank > 0 && int(rank) > r.opts.MaxRank {
			return nil, ErrMalformedLength
		}
		dims := make([]uint64, rank)
		for i := uint8(0); i < rank; i++ {
			dim, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			dims[i] = dim
		}
		dataLen, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: dataLen can't exceed remaining data
		if dataLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Enforce MaxBytesLen for tensor data
		if r.opts.MaxBytesLen > 0 && dataLen > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		// Streaming tensor decode via TensorSink
		if r.opts.TensorSink != nil {
			subslice := r.data[r.pos : r.pos+int(dataLen)]
			r.pos += int(dataLen)
			if err := r.opts.TensorSink.AcceptTensor(DType(dtype), dims, bytes.NewReader(subslice)); err != nil {
				return nil, err
			}
			// Return a tensor with nil data (header only)
			return Tensor(DType(dtype), dims, nil), nil
		}
		data, err := r.read(int(dataLen))
		if err != nil {
			return nil, err
		}
		return Tensor(DType(dtype), dims, data), nil

	case TagTensorRef:
		storeID, err := r.readByte()
		if err != nil {
			return nil, err
		}
		keyLen, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: keyLen can't exceed remaining data
		if keyLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// TensorRef keys should be bounded (UUIDs, hashes, etc.) - use MaxStringLen as reasonable limit
		if r.opts.MaxStringLen > 0 && keyLen > uint64(r.opts.MaxStringLen) {
			return nil, ErrStringTooLarge
		}
		key, err := r.read(int(keyLen))
		if err != nil {
			return nil, err
		}
		return TensorRef(storeID, key), nil

	case TagImage:
		format, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if !isValidImageFormat(ImageFormat(format)) {
			return nil, ErrInvalidImgFormat
		}
		width, err := r.readUint16LE()
		if err != nil {
			return nil, err
		}
		height, err := r.readUint16LE()
		if err != nil {
			return nil, err
		}
		dataLen, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: dataLen can't exceed remaining data
		if dataLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Enforce MaxBytesLen for image data
		if r.opts.MaxBytesLen > 0 && dataLen > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		data, err := r.read(int(dataLen))
		if err != nil {
			return nil, err
		}
		return Image(ImageFormat(format), width, height, data), nil

	case TagAudio:
		encoding, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if !isValidAudioEncoding(AudioEncoding(encoding)) {
			return nil, ErrInvalidAudioEnc
		}
		sampleRate, err := r.readUint32LE()
		if err != nil {
			return nil, err
		}
		channels, err := r.readByte()
		if err != nil {
			return nil, err
		}
		dataLen, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: dataLen can't exceed remaining data
		if dataLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Enforce MaxBytesLen for audio data
		if r.opts.MaxBytesLen > 0 && dataLen > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		data, err := r.read(int(dataLen))
		if err != nil {
			return nil, err
		}
		return Audio(AudioEncoding(encoding), sampleRate, channels, data), nil

	case TagAdjlist:
		idWidth, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if !isValidIDWidth(IDWidth(idWidth)) {
			return nil, ErrInvalidIDWidth
		}
		nodeCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		edgeCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Guard against nodeCount+1 overflow (MaxUint64 + 1 wraps to 0)
		if nodeCount == math.MaxUint64 {
			return nil, ErrMalformedLength
		}
		// Sanity check: nodeCount+1 offsets, each at least 1 byte
		if nodeCount+1 > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		rowOffsets := make([]uint64, nodeCount+1)
		for i := uint64(0); i <= nodeCount; i++ {
			offset, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			rowOffsets[i] = offset
		}
		// Calculate colIndices byte length with overflow-safe arithmetic
		var adjIDWidth int
		if IDWidth(idWidth) == IDWidthInt32 {
			adjIDWidth = 4
		} else {
			adjIDWidth = 8
		}
		bytesPerID := uint64(adjIDWidth)
		if edgeCount > 0 && bytesPerID > 0 && edgeCount > uint64(r.remaining())/bytesPerID {
			return nil, ErrMalformedLength
		}
		colBytesLen := int(edgeCount * bytesPerID)
		// Sanity check: colBytesLen can't exceed remaining data
		if colBytesLen > r.remaining() {
			return nil, ErrMalformedLength
		}
		colIndices, err := r.read(colBytesLen)
		if err != nil {
			return nil, err
		}
		return Adjlist(IDWidth(idWidth), nodeCount, edgeCount, rowOffsets, colIndices), nil

	case TagRichText:
		text, err := r.readStringWithLimit(r.opts.MaxStringLen)
		if err != nil {
			return nil, err
		}
		flags, err := r.readByte()
		if err != nil {
			return nil, err
		}
		var tokens []int32
		if flags&0x01 != 0 {
			tokenCount, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			// Sanity check: tokenCount * 4 bytes can't exceed remaining
			if tokenCount*4 > uint64(r.remaining()) {
				return nil, ErrMalformedLength
			}
			tokens = make([]int32, tokenCount)
			for i := uint64(0); i < tokenCount; i++ {
				tok, err := r.readInt32LE()
				if err != nil {
					return nil, err
				}
				tokens[i] = tok
			}
		}
		var spans []RichTextSpan
		if flags&0x02 != 0 {
			spanCount, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			// Sanity check: each span needs at least 3 bytes (3 uvarints)
			if spanCount*3 > uint64(r.remaining()) {
				return nil, ErrMalformedLength
			}
			spans = make([]RichTextSpan, spanCount)
			for i := uint64(0); i < spanCount; i++ {
				start, err := r.readUvarint()
				if err != nil {
					return nil, err
				}
				end, err := r.readUvarint()
				if err != nil {
					return nil, err
				}
				kindID, err := r.readUvarint()
				if err != nil {
					return nil, err
				}
				spans[i] = RichTextSpan{Start: start, End: end, KindID: kindID}
			}
		}
		return RichText(text, tokens, spans), nil

	case TagDelta:
		baseID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		opCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: each op needs at least 2 bytes (opCode + fieldID)
		if opCount*2 > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		ops := make([]DeltaOp, opCount)
		for i := uint64(0); i < opCount; i++ {
			opCode, err := r.readByte()
			if err != nil {
				return nil, err
			}
			if !isValidDeltaOpCode(DeltaOpCode(opCode)) {
				return nil, ErrInvalidDeltaOp
			}
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			var val *Value
			if DeltaOpCode(opCode) == DeltaOpSetField || DeltaOpCode(opCode) == DeltaOpAppendArray {
				val, err = decodeValue(r, dict)
				if err != nil {
					return nil, err
				}
			}
			ops[i] = DeltaOp{OpCode: DeltaOpCode(opCode), FieldID: fieldID, Value: val}
		}
		return Delta(baseID, ops), nil

	// v2.1 Graph Types
	case TagNode:
		nodeData, err := decodeNodeData(r, dict)
		if err != nil {
			return nil, err
		}
		return Node(nodeData.ID, nodeData.Labels, nodeData.Props), nil

	case TagEdge:
		edgeData, err := decodeEdgeData(r, dict)
		if err != nil {
			return nil, err
		}
		return Edge(edgeData.From, edgeData.To, edgeData.Type, edgeData.Props), nil

	case TagNodeBatch:
		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: each node needs at least a few bytes
		if count > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Limit check using array length limit
		if r.opts.MaxArrayLen > 0 && count > uint64(r.opts.MaxArrayLen) {
			return nil, ErrArrayTooLarge
		}
		nodes := make([]NodeData, count)
		for i := uint64(0); i < count; i++ {
			nodeData, err := decodeNodeData(r, dict)
			if err != nil {
				return nil, err
			}
			nodes[i] = *nodeData
		}
		return NodeBatch(nodes), nil

	case TagEdgeBatch:
		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: each edge needs at least a few bytes
		if count > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Limit check using array length limit
		if r.opts.MaxArrayLen > 0 && count > uint64(r.opts.MaxArrayLen) {
			return nil, ErrArrayTooLarge
		}
		edges := make([]EdgeData, count)
		for i := uint64(0); i < count; i++ {
			edgeData, err := decodeEdgeData(r, dict)
			if err != nil {
				return nil, err
			}
			edges[i] = *edgeData
		}
		return EdgeBatch(edges), nil

	case TagGraphShard:
		// Depth tracking for nested structure
		if err := r.enterNested(); err != nil {
			return nil, err
		}
		defer r.exitNested()

		// Decode nodes
		nodeCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if nodeCount > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		if r.opts.MaxArrayLen > 0 && nodeCount > uint64(r.opts.MaxArrayLen) {
			return nil, ErrArrayTooLarge
		}
		nodes := make([]NodeData, nodeCount)
		for i := uint64(0); i < nodeCount; i++ {
			nodeData, err := decodeNodeData(r, dict)
			if err != nil {
				return nil, err
			}
			nodes[i] = *nodeData
		}

		// Decode edges
		edgeCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if edgeCount > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		if r.opts.MaxArrayLen > 0 && edgeCount > uint64(r.opts.MaxArrayLen) {
			return nil, ErrArrayTooLarge
		}
		edges := make([]EdgeData, edgeCount)
		for i := uint64(0); i < edgeCount; i++ {
			edgeData, err := decodeEdgeData(r, dict)
			if err != nil {
				return nil, err
			}
			edges[i] = *edgeData
		}

		// Decode metadata
		metaCount, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if metaCount > uint64(r.remaining()/2) {
			return nil, ErrMalformedLength
		}
		if r.opts.MaxObjectLen > 0 && metaCount > uint64(r.opts.MaxObjectLen) {
			return nil, ErrObjectTooLarge
		}
		metadata := make(map[string]any, metaCount)
		for i := uint64(0); i < metaCount; i++ {
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			if fieldID >= uint64(len(dict)) {
				return nil, ErrInvalidFieldID
			}
			v, err := decodeValue(r, dict)
			if err != nil {
				return nil, err
			}
			metadata[dict[fieldID]] = valueToAny(v)
		}
		return GraphShard(nodes, edges, metadata), nil

	case TagBitmask:
		count, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		byteLen := (count + 7) / 8
		// Sanity check
		if byteLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		if r.opts.MaxBytesLen > 0 && byteLen > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		bits, err := r.read(int(byteLen))
		if err != nil {
			return nil, err
		}
		return Bitmask(count, bits), nil

	case TagExt:
		// Extension envelope: ExtType:uvarint | Len:uvarint | Payload:Len bytes
		extType, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		payloadLen, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// Sanity check: payloadLen can't exceed remaining data
		if payloadLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Security limit check
		if r.opts.MaxExtLen > 0 && payloadLen > uint64(r.opts.MaxExtLen) {
			return nil, ErrExtTooLarge
		}

		// Handle based on OnUnknownExt setting
		switch r.opts.OnUnknownExt {
		case UnknownExtError:
			return nil, ErrUnknownExt
		case UnknownExtSkipAsNull:
			// Skip the payload and return Null
			if _, err := r.read(int(payloadLen)); err != nil {
				return nil, err
			}
			return Null(), nil
		default: // UnknownExtKeep
			// Read and preserve the payload
			payload, err := r.read(int(payloadLen))
			if err != nil {
				return nil, err
			}
			return UnknownExtension(extType, payload), nil
		}

	default:
		// v3 inline types
		switch {
		case tag >= TagFixintBase && tag <= TagFixintMax:
			return Int64(int64(tag - TagFixintBase)), nil

		case tag >= TagFixarrayBase && tag <= TagFixarrayMax:
			count := int(tag - TagFixarrayBase)
			// Limit check
			if r.opts.MaxArrayLen > 0 && count > r.opts.MaxArrayLen {
				return nil, ErrArrayTooLarge
			}
			if err := r.enterNested(); err != nil {
				return nil, err
			}
			defer r.exitNested()
			items := make([]*Value, count)
			for i := 0; i < count; i++ {
				v, err := decodeValue(r, dict)
				if err != nil {
					return nil, err
				}
				items[i] = v
			}
			return Array(items...), nil

		case tag >= TagFixmapBase && tag <= TagFixmapMax:
			count := int(tag - TagFixmapBase)
			// Limit check
			if r.opts.MaxObjectLen > 0 && count > r.opts.MaxObjectLen {
				return nil, ErrObjectTooLarge
			}
			if err := r.enterNested(); err != nil {
				return nil, err
			}
			defer r.exitNested()
			members := make([]Member, count)
			for i := 0; i < count; i++ {
				fieldID, err := r.readUvarint()
				if err != nil {
					return nil, err
				}
				if fieldID >= uint64(len(dict)) {
					return nil, ErrInvalidFieldID
				}
				v, err := decodeValue(r, dict)
				if err != nil {
					return nil, err
				}
				members[i] = Member{Key: dict[fieldID], Value: v}
			}
			return Object(members...), nil

		case tag >= TagFixnegBase && tag <= TagFixnegMax:
			// value = -1 - (tag - TagFixnegBase)
			return Int64(int64(-1) - int64(tag-TagFixnegBase)), nil

		default:
			return nil, &TagError{Tag: tag}
		}
	}
}

// decodeNodeData decodes a node (without tag byte).
// Wire format: idLen:varint | idBytes | labelCount:varint | (labelLen:varint | labelBytes)* | propCount:varint | (dictIdx:varint | value)*
func decodeNodeData(r *reader, dict []string) (*NodeData, error) {
	// Read ID
	id, err := r.readStringWithLimit(r.opts.MaxStringLen)
	if err != nil {
		return nil, err
	}

	// Read labels
	labelCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each label needs at least 1 byte
	if labelCount > uint64(r.remaining()) {
		return nil, ErrMalformedLength
	}
	// Limit check
	if r.opts.MaxArrayLen > 0 && labelCount > uint64(r.opts.MaxArrayLen) {
		return nil, ErrArrayTooLarge
	}
	labels := make([]string, labelCount)
	for i := uint64(0); i < labelCount; i++ {
		label, err := r.readStringWithLimit(r.opts.MaxStringLen)
		if err != nil {
			return nil, err
		}
		labels[i] = label
	}

	// Read properties (dictionary-coded)
	propCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each prop needs at least 2 bytes (dictIdx + value tag)
	if propCount > uint64(r.remaining()/2) {
		return nil, ErrMalformedLength
	}
	if r.opts.MaxObjectLen > 0 && propCount > uint64(r.opts.MaxObjectLen) {
		return nil, ErrObjectTooLarge
	}
	props := make(map[string]any, propCount)
	for i := uint64(0); i < propCount; i++ {
		fieldID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if fieldID >= uint64(len(dict)) {
			return nil, ErrInvalidFieldID
		}
		v, err := decodeValue(r, dict)
		if err != nil {
			return nil, err
		}
		props[dict[fieldID]] = valueToAny(v)
	}

	return &NodeData{
		ID:     id,
		Labels: labels,
		Props:  props,
	}, nil
}

// decodeEdgeData decodes an edge (without tag byte).
// Wire format: srcLen:varint | srcBytes | dstLen:varint | dstBytes | typeLen:varint | typeBytes | propCount:varint | (dictIdx:varint | value)*
func decodeEdgeData(r *reader, dict []string) (*EdgeData, error) {
	// Read source ID
	from, err := r.readStringWithLimit(r.opts.MaxStringLen)
	if err != nil {
		return nil, err
	}

	// Read destination ID
	to, err := r.readStringWithLimit(r.opts.MaxStringLen)
	if err != nil {
		return nil, err
	}

	// Read edge type
	edgeType, err := r.readStringWithLimit(r.opts.MaxStringLen)
	if err != nil {
		return nil, err
	}

	// Read properties (dictionary-coded)
	propCount, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	// Sanity check: each prop needs at least 2 bytes (dictIdx + value tag)
	if propCount > uint64(r.remaining()/2) {
		return nil, ErrMalformedLength
	}
	if r.opts.MaxObjectLen > 0 && propCount > uint64(r.opts.MaxObjectLen) {
		return nil, ErrObjectTooLarge
	}
	props := make(map[string]any, propCount)
	for i := uint64(0); i < propCount; i++ {
		fieldID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if fieldID >= uint64(len(dict)) {
			return nil, ErrInvalidFieldID
		}
		v, err := decodeValue(r, dict)
		if err != nil {
			return nil, err
		}
		props[dict[fieldID]] = valueToAny(v)
	}

	return &EdgeData{
		From:  from,
		To:    to,
		Type:  edgeType,
		Props: props,
	}, nil
}

// valueToAny converts a *Value to its Go any equivalent.
// This is used for graph property values which store map[string]any.
func valueToAny(v *Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case TypeNull:
		return nil
	case TypeBool:
		return v.boolVal
	case TypeInt64:
		return v.int64Val
	case TypeUint64:
		return v.uint64Val
	case TypeFloat64:
		return v.float64Val
	case TypeString:
		return v.stringVal
	case TypeBytes:
		return v.bytesVal
	case TypeDatetime64:
		return v.datetime64
	case TypeDecimal128:
		return v.decimal128
	case TypeUUID128:
		return v.uuid128
	case TypeBigInt:
		return v.bigintVal
	case TypeArray:
		arr := make([]any, len(v.arrayVal))
		for i, item := range v.arrayVal {
			arr[i] = valueToAny(item)
		}
		return arr
	case TypeObject:
		obj := make(map[string]any, len(v.objectVal))
		for _, m := range v.objectVal {
			obj[m.Key] = valueToAny(m.Value)
		}
		return obj
	case TypeTensor:
		return v.tensorVal
	case TypeTensorRef:
		return v.tensorRefVal
	case TypeImage:
		return v.imageVal
	case TypeAudio:
		return v.audioVal
	case TypeAdjlist:
		return v.adjlistVal
	case TypeRichText:
		return v.richTextVal
	case TypeDelta:
		return v.deltaVal
	case TypeNode:
		return v.nodeVal
	case TypeEdge:
		return v.edgeVal
	case TypeNodeBatch:
		return v.nodeBatchVal
	case TypeEdgeBatch:
		return v.edgeBatchVal
	case TypeGraphShard:
		return v.graphShardVal
	case TypeBitmask:
		return v.bitmaskVal
	case TypeUnknownExt:
		return v.unknownExtVal
	default:
		return nil
	}
}
