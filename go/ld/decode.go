package ld

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"

	"github.com/Neumenon/cowrie/go"
)

// Errors
var (
	ErrInvalidMagic    = errors.New("cowrie-ld: invalid magic bytes")
	ErrInvalidVersion  = errors.New("cowrie-ld: unsupported version")
	ErrUnexpectedEOF   = errors.New("cowrie-ld: unexpected end of data")
	ErrInvalidTag      = errors.New("cowrie-ld: invalid type tag")
	ErrInvalidFieldID  = errors.New("cowrie-ld: invalid field ID")
	ErrInvalidIRIID    = errors.New("cowrie-ld: invalid IRI ID")
	ErrMalformedLength = errors.New("cowrie-ld: malformed length exceeds remaining data")
	ErrInvalidVarint   = errors.New("cowrie-ld: invalid varint (overflow)")
	ErrDepthExceeded   = errors.New("cowrie-ld: maximum nesting depth exceeded")
	ErrStringTooLarge  = errors.New("cowrie-ld: string exceeds maximum length")
	ErrArrayTooLarge   = errors.New("cowrie-ld: array exceeds maximum length")
	ErrInputTooLarge   = errors.New("cowrie-ld: input exceeds size limit")
)

// Default security limits for the LD decoder.
const (
	defaultMaxDepth     = 1000
	defaultMaxStringLen = 10_000_000 // 10MB
	defaultMaxArrayLen  = 1_000_000  // 1M elements
	defaultMaxInputSize = 50_000_000 // 50MB (matches cowrie DefaultMaxBytesLen)
)

// Decode decodes Cowrie-LD binary data into an LDDocument.
func Decode(data []byte) (*LDDocument, error) {
	r := &reader{
		data:         data,
		maxDepth:     defaultMaxDepth,
		maxStringLen: defaultMaxStringLen,
		maxArrayLen:  defaultMaxArrayLen,
	}
	return decode(r)
}

// DecodeFrom decodes from an io.Reader.
// Applies defaultMaxInputSize (50MB) as an input size limit to prevent
// resource exhaustion. Use DecodeFromLimited for a custom limit.
func DecodeFrom(rd io.Reader) (*LDDocument, error) {
	maxBytes := int64(defaultMaxInputSize)
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

// DecodeFromLimited decodes from an io.Reader with a custom size limit.
// Returns ErrInputTooLarge if the input exceeds maxBytes.
func DecodeFromLimited(rd io.Reader, maxBytes int64) (*LDDocument, error) {
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

// reader wraps a byte slice for reading with security limits.
type reader struct {
	data        []byte
	pos         int
	depth       int // Current nesting depth
	maxDepth    int // Maximum allowed nesting depth
	maxStringLen int // Maximum string byte length
	maxArrayLen int // Maximum array/object element count
}

// remaining returns bytes left to read.
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
		return 0, ErrInvalidVarint
	}
	r.pos += n
	return v, nil
}

func (r *reader) readString() (string, error) {
	length, err := r.readLength()
	if err != nil {
		return "", err
	}
	b, err := r.read(length)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readStringWithLimit reads a string with an explicit length limit.
func (r *reader) readStringWithLimit() (string, error) {
	length, err := r.readLength()
	if err != nil {
		return "", err
	}
	if r.maxStringLen > 0 && length > r.maxStringLen {
		return "", ErrStringTooLarge
	}
	b, err := r.read(length)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// enterNested increments depth and checks limit.
func (r *reader) enterNested() error {
	r.depth++
	if r.maxDepth > 0 && r.depth > r.maxDepth {
		return ErrDepthExceeded
	}
	return nil
}

// exitNested decrements depth.
func (r *reader) exitNested() {
	r.depth--
}

func uint64ToInt(v uint64) (int, error) {
	if strconv.IntSize == 32 && v > 1<<31-1 {
		return 0, ErrMalformedLength
	}

	// #nosec G115 -- bounded by the platform int size above.
	return int(v), nil
}

func (r *reader) readCount(minBytes int) (int, error) {
	count, err := r.readUvarint()
	if err != nil {
		return 0, err
	}
	countInt, err := uint64ToInt(count)
	if err != nil {
		return 0, err
	}
	if countInt > r.remaining()/minBytes {
		return 0, ErrMalformedLength
	}
	return countInt, nil
}

func (r *reader) readLength() (int, error) {
	length, err := r.readUvarint()
	if err != nil {
		return 0, err
	}
	lengthInt, err := uint64ToInt(length)
	if err != nil {
		return 0, ErrMalformedLength
	}
	if lengthInt > r.remaining() {
		return 0, ErrMalformedLength
	}
	return lengthInt, nil
}

// zigzagDecode decodes a zigzag-encoded value.
func zigzagDecode(n uint64) int64 {
	// #nosec G115 -- shifting right by 1 bounds the value to MaxInt64.
	decoded := int64(n >> 1)
	if n&1 != 0 {
		return ^decoded
	}
	return decoded
}

// decode reads the complete Cowrie-LD format.
func decode(r *reader) (*LDDocument, error) {
	// Read header: 'SJLD' + version + flags
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

	// Read flags (reserved for future use)
	_, err = r.readByte()
	if err != nil {
		return nil, err
	}

	doc := NewDocument()

	// Read field dictionary
	fieldDictLen, err := r.readCount(1)
	if err != nil {
		return nil, err
	}
	doc.FieldDict = make([]string, fieldDictLen)
	for i := 0; i < fieldDictLen; i++ {
		s, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		doc.FieldDict[i] = s
	}

	// Read terms table
	termCount, err := r.readCount(3)
	if err != nil {
		return nil, err
	}
	doc.Terms = make([]TermEntry, termCount)
	for i := 0; i < termCount; i++ {
		term, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		iri, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		flags, err := r.readByte()
		if err != nil {
			return nil, err
		}
		doc.Terms[i] = TermEntry{
			Term:  term,
			IRI:   IRI(iri),
			Flags: TermFlags(flags),
		}
	}

	// Read IRIs table
	iriCount, err := r.readCount(1)
	if err != nil {
		return nil, err
	}
	doc.IRIs = make([]IRI, iriCount)
	for i := 0; i < iriCount; i++ {
		s, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		doc.IRIs[i] = IRI(s)
	}

	// Read datatypes table
	dtCount, err := r.readCount(1)
	if err != nil {
		return nil, err
	}
	doc.Datatypes = make([]IRI, dtCount)
	for i := 0; i < dtCount; i++ {
		s, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		doc.Datatypes[i] = IRI(s)
	}

	// Decode root value
	doc.Root, err = decodeValue(r, doc)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// decodeValue reads a single value.
func decodeValue(r *reader, doc *LDDocument) (*cowrie.Value, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, err
	}

	switch tag {
	case cowrie.TagNull:
		return cowrie.Null(), nil

	case cowrie.TagFalse:
		return cowrie.Bool(false), nil

	case cowrie.TagTrue:
		return cowrie.Bool(true), nil

	case cowrie.TagInt64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return cowrie.Int64(zigzagDecode(u)), nil

	case cowrie.TagUint64:
		u, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		return cowrie.Uint64(u), nil

	case cowrie.TagFloat64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint64(b)
		return cowrie.Float64(math.Float64frombits(bits)), nil

	case cowrie.TagDecimal128:
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
		scaleInt := int16(scale)
		if scaleInt >= 1<<7 {
			scaleInt -= 1 << 8
		}
		// #nosec G115 -- scaleInt is normalized into the int8 range above.
		return cowrie.NewDecimal128(int8(scaleInt), coef), nil

	case cowrie.TagString:
		s, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		return cowrie.String(s), nil

	case cowrie.TagBytes:
		length, err := r.readLength()
		if err != nil {
			return nil, err
		}
		b, err := r.read(length)
		if err != nil {
			return nil, err
		}
		return cowrie.Bytes(b), nil

	case cowrie.TagDatetime64:
		b, err := r.read(8)
		if err != nil {
			return nil, err
		}
		lo := binary.LittleEndian.Uint32(b[:4])
		hi := int64(binary.LittleEndian.Uint32(b[4:]))
		if hi >= 1<<31 {
			hi -= 1 << 32
		}
		nanos := hi<<32 | int64(lo)
		return cowrie.Datetime64(nanos), nil

	case cowrie.TagUUID128:
		b, err := r.read(16)
		if err != nil {
			return nil, err
		}
		var uuid [16]byte
		copy(uuid[:], b)
		return cowrie.UUID128(uuid), nil

	case cowrie.TagBigInt:
		length, err := r.readLength()
		if err != nil {
			return nil, err
		}
		b, err := r.read(length)
		if err != nil {
			return nil, err
		}
		return cowrie.BigInt(b), nil

	case cowrie.TagArray:
		if err := r.enterNested(); err != nil {
			return nil, err
		}
		defer r.exitNested()

		count, err := r.readCount(1)
		if err != nil {
			return nil, err
		}
		if r.maxArrayLen > 0 && count > r.maxArrayLen {
			return nil, ErrArrayTooLarge
		}
		items := make([]*cowrie.Value, count)
		for i := 0; i < count; i++ {
			v, err := decodeValue(r, doc)
			if err != nil {
				return nil, err
			}
			items[i] = v
		}
		return cowrie.Array(items...), nil

	case cowrie.TagObject:
		if err := r.enterNested(); err != nil {
			return nil, err
		}
		defer r.exitNested()

		count, err := r.readCount(2)
		if err != nil {
			return nil, err
		}
		if r.maxArrayLen > 0 && count > r.maxArrayLen {
			return nil, ErrArrayTooLarge
		}
		members := make([]cowrie.Member, count)
		for i := 0; i < count; i++ {
			fieldID, err := r.readUvarint()
			if err != nil {
				return nil, err
			}
			if fieldID >= uint64(len(doc.FieldDict)) {
				return nil, ErrInvalidFieldID
			}
			fieldIDInt, err := uint64ToInt(fieldID)
			if err != nil {
				return nil, ErrInvalidFieldID
			}
			v, err := decodeValue(r, doc)
			if err != nil {
				return nil, err
			}
			members[i] = cowrie.Member{Key: doc.FieldDict[fieldIDInt], Value: v}
		}
		return cowrie.Object(members...), nil

	case TagIRI:
		// IRI reference - decode ID and return as string value with IRI marker
		iriID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if iriID >= uint64(len(doc.IRIs)) {
			return nil, ErrInvalidIRIID
		}
		iriIDInt, err := uint64ToInt(iriID)
		if err != nil {
			return nil, ErrInvalidIRIID
		}
		// Return the IRI as a string value
		// Callers can check the LDDocument to understand this is an IRI
		return cowrie.String(string(doc.IRIs[iriIDInt])), nil

	case TagBNode:
		// Blank node - decode ID string
		id, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		// Return as string value with _: prefix
		return cowrie.String("_:" + id), nil

	default:
		return nil, ErrInvalidTag
	}
}

// DecodeLDValue reads a value that may be an IRI, BNode, or regular value.
// Returns an LDValue with metadata about the value type.
func DecodeLDValue(r *reader, doc *LDDocument) (*LDValue, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Handle LD-specific tags
	switch tag {
	case TagIRI:
		iriID, err := r.readUvarint()
		if err != nil {
			return nil, err
		}
		if iriID >= uint64(len(doc.IRIs)) {
			return nil, ErrInvalidIRIID
		}
		iriIDInt, err := uint64ToInt(iriID)
		if err != nil {
			return nil, ErrInvalidIRIID
		}
		return &LDValue{
			Value: cowrie.String(string(doc.IRIs[iriIDInt])),
			IsIRI: true,
			IRIID: iriIDInt,
		}, nil

	case TagBNode:
		id, err := r.readStringWithLimit()
		if err != nil {
			return nil, err
		}
		return &LDValue{
			Value:   cowrie.String(id),
			IsBNode: true,
			BNodeID: id,
		}, nil
	}

	// For non-LD tags, put the tag back and decode normally
	r.pos-- // unread the tag
	v, err := decodeValue(r, doc)
	if err != nil {
		return nil, err
	}
	return &LDValue{Value: v}, nil
}
