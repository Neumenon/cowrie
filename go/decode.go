package cowrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"unicode/utf8"
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
	ErrInvalidAudioCh   = errors.New("cowrie: invalid audio channel count")
	ErrInvalidAudioRate = errors.New("cowrie: audio sample rate out of range")
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
	// ErrInvalidUTF8 is returned when a String value or header dictionary key
	// holds bytes that are not valid UTF-8 (§2.4). The Python oracle
	// (decode.py:69-73 keys, :116-124 string values), Rust and TS all reject
	// these with ERR_INVALID_UTF8; Go must match.
	ErrInvalidUTF8 = errors.New("cowrie: string is not valid UTF-8")
	ErrDictTooLarge    = errors.New("cowrie: dictionary exceeds maximum size")
	ErrTrailingData    = errors.New("cowrie: trailing data after root value")
	// ErrDuplicateKey is returned when a header dictionary contains two identical
	// keys (§2.4). Distinct from ErrNonCanonical (key < previous = not sorted) so
	// the conformance gate sees ERR_DUPLICATE_KEY. Mirrors decode.py.
	ErrDuplicateKey = errors.New("cowrie: duplicate dictionary key")
	// ErrTooLarge is returned for §2.7 size limits (e.g. tensor rank > MAX_RANK).
	ErrTooLarge = errors.New("cowrie: value exceeds §2.7 size limit")
	// ErrTooDeep is returned when nesting depth exceeds §2.7 MAX_DEPTH.
	ErrTooDeep = errors.New("cowrie: nesting depth exceeds §2.7 MAX_DEPTH")
	// ErrNonCanonical is returned in strict-decode mode (§5.3) when input is
	// well-formed but not in canonical form (e.g. an Int that should be a
	// FIXINT, a non-minimal BigInt, a negative-zero Float, an unsorted header
	// dictionary, etc.). Mirrors tools/cowrie_ref/decode.py ERR_NON_CANONICAL.
	ErrNonCanonical = errors.New("cowrie: non-canonical encoding (strict mode)")
)

// Default security limits (can be overridden via DecodeOptions)
// These are generous to support ML workloads while preventing extreme allocations.
// The sanity checks (length vs remaining data) are the primary protection;
// these limits are a secondary defense against legitimately-sized but huge payloads.
const (
	DefaultMaxDepth     = 1000       // Maximum nesting depth
	DefaultMaxArrayLen  = 1_000_000  // 1M elements (tightened: was 100M)
	DefaultMaxObjectLen = 1_000_000  // 1M fields (tightened: was 10M)
	DefaultMaxStringLen = 10_000_000 // 10MB strings (tightened: was 500MB)
	DefaultMaxBytesLen  = 50_000_000 // 50MB bytes (tightened: was 1GB)
	DefaultMaxExtLen    = 1_000_000  // 1MB max extension payload (tightened: was 100MB)
	DefaultMaxDictLen   = 1_000_000  // 1M dictionary entries (tightened: was 10M)
	DefaultMaxRank      = 32         // Maximum tensor rank (dimensions)
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

	// Strict enables strict-decode mode (SPEC-v1 §5.3): well-formed but
	// non-canonical input is REJECTED with ErrNonCanonical rather than
	// silently accepted. This mirrors tools/cowrie_ref/decode.py strict=True.
	// Default false (lenient) — behavior is unchanged when not set.
	Strict bool
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

// dtypeElemSize returns the element size in bytes for byte-aligned dtypes
// (bool included: one byte per element). Returns (size, true) for those, and
// (0, false) for sub-byte packed types (qint4, qint2, qint3, ternary, binary)
// whose packing formula is non-trivial and not validated here.
func dtypeElemSize(d DType) (uint64, bool) {
	switch d {
	case DTypeFloat32:
		return 4, true
	case DTypeFloat16:
		return 2, true
	case DTypeBFloat16:
		return 2, true
	case DTypeFloat64:
		return 8, true
	case DTypeInt8:
		return 1, true
	case DTypeInt16:
		return 2, true
	case DTypeInt32:
		return 4, true
	case DTypeInt64:
		return 8, true
	case DTypeUint8:
		return 1, true
	case DTypeUint16:
		return 2, true
	case DTypeUint32:
		return 4, true
	case DTypeUint64:
		return 8, true
	case DTypeBool:
		// Bool tensors store one byte per element; bit-packed booleans use the
		// separate Bitmask type. So shape validation applies (one byte/element).
		return 1, true
	default:
		// Sub-byte packed types (DTypeQINT4, DTypeQINT2, DTypeQINT3, DTypeTernary,
		// DTypeBinary) use non-trivial packing; skip shape validation.
		return 0, false
	}
}

// tensorExpectedBytes computes the expected byte length for a tensor with the
// given dims and dtype. Returns (expected, true) when a definitive check is
// possible, or (0, false) when the dtype uses sub-byte packing.
// Overflow in the product of dims is detected and causes (0, false) so the
// caller skips the check rather than accepting a corrupt tensor silently.
// A rank-0 tensor (no dims) is a scalar — exactly one element; a zero dimension
// (e.g. shape [0]) yields an empty 0-byte tensor.
func tensorExpectedBytes(dtype DType, dims []uint64) (uint64, bool) {
	elemSize, ok := dtypeElemSize(dtype)
	if !ok {
		return 0, false
	}
	// Rank-0 (no dims) is a scalar: one element (NumPy convention,
	// np.array(5).shape == ()). A zero dimension is handled in the loop below.
	if len(dims) == 0 {
		return elemSize, true
	}
	product := uint64(1)
	for _, d := range dims {
		if d == 0 {
			return 0, true
		}
		// Overflow-safe multiplication: if product > MaxUint64/d, it overflows.
		if product > ^uint64(0)/d {
			// Overflow: can't validate safely; skip check.
			return 0, false
		}
		product *= d
	}
	// Overflow-safe multiplication for elemSize.
	if elemSize > 0 && product > ^uint64(0)/elemSize {
		return 0, false
	}
	return product * elemSize, true
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
	if opts.MaxRank == 0 {
		opts.MaxRank = DefaultMaxRank
	}

	r := &reader{data: data, opts: opts, strict: opts.Strict}
	return decode(r)
}

// TensorSpans decodes a canonical (uncompressed) Cowrie v1 message and returns
// one TensorSpan per tensor in document order (the order tensors appear during
// decode), locating each tensor's contiguous little-endian data run. Tensors
// nested inside arrays/objects are included. Offsets are ABSOLUTE byte offsets
// into data. Returns nil (no spans) when the message contains no tensors.
// Mirrors tools/cowrie_ref/decode.py tensor_spans.
func TensorSpans(data []byte) ([]TensorSpan, error) {
	opts := DefaultDecodeOptions()
	r := &reader{data: data, opts: opts, strict: opts.Strict}
	spans := make([]TensorSpan, 0)
	r.spans = &spans
	if _, err := decode(r); err != nil {
		return nil, err
	}
	return spans, nil
}

// DecodeFrom decodes from an io.Reader.
// Applies DefaultMaxBytesLen (50MB) as an input size limit to prevent
// resource exhaustion. Use DecodeFromLimited for a custom limit.
func DecodeFrom(rd io.Reader) (*Value, error) {
	maxBytes := int64(DefaultMaxBytesLen)
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
// Applies opts.MaxBytesLen (or DefaultMaxBytesLen) as an input size limit
// to prevent resource exhaustion.
func DecodeFromWithOptions(rd io.Reader, opts DecodeOptions) (*Value, error) {
	maxBytes := int64(opts.MaxBytesLen)
	if maxBytes <= 0 {
		maxBytes = int64(DefaultMaxBytesLen)
	}
	lr := io.LimitReader(rd, maxBytes+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrInputTooLarge
	}
	return DecodeWithOptions(data, opts)
}

// TensorSpan locates a tensor's contiguous little-endian data run within a
// canonical Cowrie message (Phase 2 zero-copy tensor locator). DataOffset is the
// ABSOLUTE byte offset (from byte 0 of the message) of the first data byte — the
// decoder position immediately AFTER the dataLen uvarint — and DataLen is the
// run's byte length. A zero-copy reader views data[DataOffset:DataOffset+DataLen]
// with no decode/copy. Mirrors tools/cowrie_ref/decode.py span tuples
// (dtype, shape, data_offset, data_len).
type TensorSpan struct {
	DType      DType
	Shape      []uint64
	DataOffset int
	DataLen    int
}

// reader wraps a byte slice for reading with security limits.
type reader struct {
	data   []byte
	pos    int
	depth  int           // Current nesting depth
	opts   DecodeOptions // Security limits
	strict bool          // §5.3 strict-decode: reject non-canonical input

	// spans, when non-nil, accumulates one TensorSpan per tensor encountered
	// during decode, in document order. Threaded through decodeValue so nested
	// tensors (inside arrays/objects) are captured too.
	spans *[]TensorSpan
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

// maxUvarintBytes is the maximum LEB128 length of a 64-bit value (§2.1).
const maxUvarintBytes = 10

func (r *reader) readUvarint() (uint64, error) {
	// Strict, minimal LEB128 decode (§2.1). Mirrors tools/cowrie_ref/varint.py:
	// reject over-length (>10 bytes), truncation, 64-bit overflow, AND non-minimal
	// (overlong) encodings — a final continuation-terminating byte of 0x00 with
	// length>1 carries no bits and is overlong (e.g. 82 00 for value 2).
	var result uint64
	var shift uint
	start := r.pos
	for {
		if r.pos-start >= maxUvarintBytes {
			return 0, ErrInvalidVarint
		}
		if r.pos >= len(r.data) {
			return 0, ErrUnexpectedEOF
		}
		b := r.data[r.pos]
		r.pos++
		// Overflow guard: bits shifted past bit 63 must be zero. shift can be up to
		// 63 here (10th byte at shift 63); only the low bit is valid in that byte.
		if shift >= 64 || (shift == 63 && b&0x7F > 1) {
			return 0, ErrInvalidVarint
		}
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			// Minimality: the final byte must be non-zero unless the value is the
			// single byte 0x00.
			if b == 0 && r.pos-start > 1 {
				return 0, ErrInvalidVarint
			}
			return result, nil
		}
		shift += 7
	}
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
	// §2.4: String/key payloads MUST be valid UTF-8. Match the Python oracle,
	// Rust and TS, which reject invalid UTF-8 with ERR_INVALID_UTF8.
	if !utf8.Valid(b) {
		return "", ErrInvalidUTF8
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
	// §2.4: String/key payloads MUST be valid UTF-8. Match the Python oracle,
	// Rust and TS, which reject invalid UTF-8 with ERR_INVALID_UTF8.
	if !utf8.Valid(b) {
		return "", ErrInvalidUTF8
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
	magic2, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic3, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if magic0 != Magic0 || magic1 != Magic1 || magic2 != Magic2 || magic3 != Magic3 {
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

	// bits 1-3 reserved; ignore unknown flag bits for forward compatibility
	_ = flags

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
		// §5.3 strict: header dictionary MUST be strictly byte-ascending and
		// unique. Go string comparison is byte-lexicographic, which matches the
		// reference's raw-UTF-8 byte comparison. Distinguish (mirrors decode.py):
		// key == previous -> ERR_DUPLICATE_KEY; key < previous -> ERR_NON_CANONICAL.
		if r.strict && i > 0 {
			if s == dict[i-1] {
				return nil, fmt.Errorf("%w: duplicate dictionary key", ErrDuplicateKey)
			}
			if s < dict[i-1] {
				return nil, fmt.Errorf("%w: dictionary not byte-sorted", ErrNonCanonical)
			}
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
		v := zigzagDecode(u)
		// §5.3 strict: an Int carrying a value in [-16,127] must use
		// FIXINT/FIXNEG, not the Int(0x03) tag.
		if r.strict && v >= -16 && v <= 127 {
			return nil, fmt.Errorf("%w: int in FIXINT/FIXNEG range", ErrNonCanonical)
		}
		return Int64(v), nil

	case TagUint64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		// §5.3 strict: a Uint carrying a value <= 2^63-1 must use Int(0x03).
		if r.strict && u <= math.MaxInt64 {
			return nil, fmt.Errorf("%w: uint fits Int", ErrNonCanonical)
		}
		return Uint64(u), nil

	case TagFloat64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint64(b)
		// §5.3 strict: reject negative zero (0x...0080 LE) and any NaN bit
		// pattern other than the canonical quiet NaN 0x7FF8000000000000.
		if r.strict {
			if bits == 0x8000000000000000 {
				return nil, fmt.Errorf("%w: negative zero", ErrNonCanonical)
			}
			f := math.Float64frombits(bits)
			if f != f && bits != 0x7FF8000000000000 {
				return nil, fmt.Errorf("%w: non-canonical NaN", ErrNonCanonical)
			}
		}
		return Float64(math.Float64frombits(bits)), nil

	case TagDecimal128:
		// §2.3: scale is an SVARINT (zigzag + LEB128), NOT a raw int8 byte.
		// Mirrors tools/cowrie_ref/decode.py T_DECIMAL.
		su, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		scale := zigzagDecode(su)
		coefBytes, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var coef [16]byte
		copy(coef[:], coefBytes)
		// §5.3 strict: a Decimal128 must be in lowest terms — a coefficient
		// divisible by 10 (with coeff != 0) should have been normalized into a
		// smaller scale.
		if r.strict && decimalNotLowestTerms(coefBytes, scale) {
			return nil, fmt.Errorf("%w: decimal not lowest terms", ErrNonCanonical)
		}
		return NewDecimal128(int32(scale), coef), nil // scale is int64-svarint on the wire; int32 covers the decimal domain

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

	// NOTE: scalar Float32 (tag 0x0F) was removed in the COWR/v1 migration.
	// Per SPEC-v1 §2.3, 0x0F is a RESERVED tag and MUST be rejected — it falls
	// through to the default branch below (TagError -> ERR_RESERVED_TAG).

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
		// §5.3 strict: a BigInt must not carry a value that fits Int/Uint
		// ([-2^63, 2^64-1]) and must use minimal-length little-endian two's
		// complement (no redundant sign-extension bytes).
		if r.strict {
			if err := strictCheckBigInt(b); err != nil {
				return nil, err
			}
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
		// §5.3 strict: an Array(0x06) with count<=15 must use FIXARRAY.
		if r.strict && count <= 15 {
			return nil, fmt.Errorf("%w: array should use FIXARRAY", ErrNonCanonical)
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
		// §5.3 strict: an Object(0x07) with count<=15 must use FIXMAP.
		if r.strict && count <= 15 {
			return nil, fmt.Errorf("%w: object should use FIXMAP", ErrNonCanonical)
		}
		members := make([]Member, count)
		lastIdx := int64(-1)
		for i := uint64(0); i < count; i++ {
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			if fieldID >= uint64(len(dict)) {
				return nil, ErrInvalidFieldID
			}
			// §5.3 strict: object fields MUST be in strictly ascending dictIdx.
			if r.strict && int64(fieldID) <= lastIdx {
				return nil, fmt.Errorf("%w: object fields not in ascending dictIdx", ErrNonCanonical)
			}
			lastIdx = int64(fieldID)
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
		// §2.7: enforce rank limit. rank > MAX_RANK -> ERR_TOO_LARGE (mirrors
		// decode.py "tensor rank > MAX_RANK").
		if r.opts.MaxRank > 0 && int(rank) > r.opts.MaxRank {
			return nil, ErrTooLarge
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
		// §2.5: tensor data is 64-byte aligned relative to byte 0 of the message.
		// r.pos is the absolute offset immediately after the dataLen uvarint;
		// consume pad = (-pos) mod 64 zero bytes so the data run starts aligned.
		// Strict mode rejects any non-zero padding byte (non-canonical).
		pad := (-r.pos) & (TensorAlign - 1)
		padBytes, err := r.read(pad)
		if err != nil {
			return nil, err
		}
		if r.strict {
			for _, pb := range padBytes {
				if pb != 0 {
					return nil, fmt.Errorf("%w: tensor alignment padding not zero", ErrNonCanonical)
				}
			}
		}
		// Phase 2 zero-copy tensor locator: r.pos is now the ABSOLUTE byte offset
		// of the tensor's contiguous data run (the 64B-aligned position after the
		// padding), exactly matching the Python oracle's data_offset. Capture it
		// before any data is consumed. Done here so a span is recorded even when
		// streaming via TensorSink. Nested tensors are captured because spans is
		// threaded through every decodeValue call.
		if r.spans != nil {
			*r.spans = append(*r.spans, TensorSpan{
				DType:      DType(dtype),
				Shape:      dims,
				DataOffset: r.pos,
				DataLen:    int(dataLen),
			})
		}
		// Sanity check: dataLen can't exceed remaining data
		if dataLen > uint64(r.remaining()) {
			return nil, ErrMalformedLength
		}
		// Enforce MaxBytesLen for tensor data
		if r.opts.MaxBytesLen > 0 && dataLen > uint64(r.opts.MaxBytesLen) {
			return nil, ErrBytesTooLarge
		}
		// Validate dataLen == product(dims) * elemSize to catch malformed tensors
		// whose declared shape does not match their byte payload. Sub-byte packed
		// types and overflow cases are skipped (tensorExpectedBytes returns false).
		if expected, ok := tensorExpectedBytes(DType(dtype), dims); ok {
			if dataLen != expected {
				return nil, fmt.Errorf("cowrie: tensor dataLen %d does not match shape/dtype (expected %d bytes)", dataLen, expected)
			}
		}
		// §5.3 strict: for sub-byte dtypes the final byte's padding bits (above
		// the last used bit) MUST be zero. Mirrors the reference's
		// `data[-1] >> (bits % 8)` check.
		if r.strict {
			if bits, ok := tensorTotalBits(DType(dtype), dims); ok && bits%8 != 0 && dataLen > 0 {
				last := r.data[r.pos+int(dataLen)-1]
				if last>>(bits%8) != 0 {
					return nil, fmt.Errorf("%w: tensor sub-byte padding not zero", ErrNonCanonical)
				}
			}
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

	// SPEC-v1 §2.3: TensorRef(0x21), Image(0x22), Audio(0x23), Node(0x35),
	// Edge(0x36), NodeBatch(0x37), EdgeBatch(0x38) are NOT in the v1 core set —
	// they are RESERVED and MUST reject with ERR_RESERVED_TAG (in both lenient and
	// strict mode). Their decode arms are removed so these tags fall through to the
	// default branch, which returns *TagError -> ERR_RESERVED_TAG. Mirrors
	// tools/cowrie_ref/decode.py (any non-core tag -> ERR_RESERVED_TAG).

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
		// §5.3 strict: trailing (unused) bits in the final byte MUST be zero.
		// Checked before Bitmask() masks them off. Mirrors the reference's
		// `data[-1] >> (count % 8)` check.
		if r.strict && count%8 != 0 && len(bits) > 0 {
			if bits[len(bits)-1]>>(count%8) != 0 {
				return nil, fmt.Errorf("%w: bitmask trailing bits not zero", ErrNonCanonical)
			}
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

	// SPEC-v1 §2.3: 0x30-0x34 and 0x39 (Adjlist/RichText/Delta/GraphShard etc.,
	// moved to attic) are RESERVED. They MUST reject with ERR_RESERVED_TAG rather
	// than being skipped-as-Null — the previous skip arm silently accepted them.
	// They now fall through to the default branch -> *TagError -> ERR_RESERVED_TAG.

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
			lastIdx := int64(-1)
			for i := 0; i < count; i++ {
				fieldID, err := r.readUvarint()
				if err != nil {
					return nil, err
				}
				if fieldID >= uint64(len(dict)) {
					return nil, ErrInvalidFieldID
				}
				// §5.3 strict: object fields MUST be in strictly ascending dictIdx.
				if r.strict && int64(fieldID) <= lastIdx {
					return nil, fmt.Errorf("%w: object fields not in ascending dictIdx", ErrNonCanonical)
				}
				lastIdx = int64(fieldID)
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
	case TypeNode:
		return v.nodeVal
	case TypeEdge:
		return v.edgeVal
	case TypeNodeBatch:
		return v.nodeBatchVal
	case TypeEdgeBatch:
		return v.edgeBatchVal
	case TypeBitmask:
		return v.bitmaskVal
	case TypeUnknownExt:
		return v.unknownExtVal
	default:
		return nil
	}
}

// ============================================================
// §5.3 strict-decode helpers (mirror tools/cowrie_ref/decode.py + model.py)
// ============================================================

// bigIntFromLE interprets little-endian two's-complement bytes as a signed
// big.Int (equivalent to Python int.from_bytes(raw, "little", signed=True)).
func bigIntFromLE(le []byte) *big.Int {
	// Convert LE -> BE for big.Int.SetBytes (which is unsigned big-endian).
	be := make([]byte, len(le))
	for i := 0; i < len(le); i++ {
		be[i] = le[len(le)-1-i]
	}
	v := new(big.Int).SetBytes(be)
	// Apply sign: if the top bit of the most-significant byte is set, the value
	// is negative: subtract 2^(8*len).
	if len(le) > 0 && le[len(le)-1]&0x80 != 0 {
		shift := new(big.Int).Lsh(big.NewInt(1), uint(8*len(le)))
		v.Sub(v, shift)
	}
	return v
}

// minTwosComplementLE returns the minimal-length little-endian two's-complement
// encoding of v. Mirrors model.min_twos_complement_le.
func minTwosComplementLE(v *big.Int) []byte {
	// length = max(1, (bitLen + 8) // 8); grow until it fits.
	length := (v.BitLen() + 8) / 8
	if length < 1 {
		length = 1
	}
	for {
		if b, ok := tryTwosComplementLE(v, length); ok {
			return b
		}
		length++
	}
}

// tryTwosComplementLE encodes v as signed little-endian in exactly `length`
// bytes, returning ok=false if v does not fit (OverflowError analogue).
func tryTwosComplementLE(v *big.Int, length int) ([]byte, bool) {
	bits := uint(8 * length)
	lo := new(big.Int).Lsh(big.NewInt(-1), bits-1)              // -2^(bits-1)
	hi := new(big.Int).Sub(new(big.Int).Neg(lo), big.NewInt(1)) // 2^(bits-1)-1
	if v.Cmp(lo) < 0 || v.Cmp(hi) > 0 {
		return nil, false
	}
	mod := new(big.Int).Lsh(big.NewInt(1), bits) // 2^bits
	u := new(big.Int).Mod(v, mod)                // two's-complement bit pattern
	be := u.Bytes()
	out := make([]byte, length)
	// be is big-endian, right-aligned into out, then reversed to little-endian.
	copy(out[length-len(be):], be)
	for i, j := 0, length-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, true
}

// strictCheckBigInt rejects a BigInt whose value fits Int/Uint, or whose
// encoding is non-minimal two's complement. Mirrors decode.py T_BIGINT strict.
func strictCheckBigInt(le []byte) error {
	v := bigIntFromLE(le)
	int64Min := new(big.Int).Lsh(big.NewInt(-1), 63)                                    // -2^63
	uint64Max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))  // 2^64-1
	if v.Cmp(int64Min) >= 0 && v.Cmp(uint64Max) <= 0 {
		return fmt.Errorf("%w: bigint fits Int/Uint", ErrNonCanonical)
	}
	if !bytes.Equal(le, minTwosComplementLE(v)) {
		return fmt.Errorf("%w: non-minimal bigint", ErrNonCanonical)
	}
	return nil
}

// decimalNotLowestTerms reports whether a Decimal128 (16-byte LE signed
// coefficient + svarint scale) is non-canonical. Mirrors
// model.Decimal128.canonical:
//   - zero coefficient: canonical iff scale == 0 (canonical() collapses any
//     zero to Decimal128(0, 0)), so coeff==0 && scale!=0 is NON-canonical.
//   - non-zero coefficient: canonical iff not divisible by 10 (the
//     scale-stripping loop divides coeff by 10 while reducing scale).
func decimalNotLowestTerms(coefLE []byte, scale int64) bool {
	c := bigIntFromLE(coefLE)
	if c.Sign() == 0 {
		return scale != 0
	}
	rem := new(big.Int).Mod(new(big.Int).Abs(c), big.NewInt(10))
	return rem.Sign() == 0
}

// dtypeBits returns the bit-width per element for a dtype (including sub-byte
// quantized types), mirroring model.DTYPE_BITS.
func dtypeBits(d DType) (uint64, bool) {
	switch d {
	case DTypeFloat32:
		return 32, true
	case DTypeFloat16, DTypeBFloat16, DTypeInt16, DTypeUint16:
		return 16, true
	case DTypeInt8, DTypeUint8, DTypeBool:
		return 8, true
	case DTypeInt32, DTypeUint32:
		return 32, true
	case DTypeInt64, DTypeUint64, DTypeFloat64:
		return 64, true
	case DTypeQINT4:
		return 4, true
	case DTypeQINT2, DTypeTernary:
		return 2, true
	case DTypeQINT3:
		return 3, true
	case DTypeBinary:
		return 1, true
	default:
		return 0, false
	}
}

// tensorTotalBits computes nelem * bitsPerElem for a tensor, returning ok=false
// on unknown dtype or multiplication overflow. Rank-0 is one element.
func tensorTotalBits(d DType, dims []uint64) (uint64, bool) {
	bpe, ok := dtypeBits(d)
	if !ok {
		return 0, false
	}
	nelem := uint64(1)
	for _, dim := range dims {
		if dim == 0 {
			return 0, true
		}
		if nelem > (^uint64(0))/dim {
			return 0, false
		}
		nelem *= dim
	}
	if bpe > 0 && nelem > (^uint64(0))/bpe {
		return 0, false
	}
	return nelem * bpe, true
}
