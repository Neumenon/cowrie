package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader reads GS1-T (text) frames from an io.Reader.
type Reader struct {
	r          *bufio.Reader
	maxPayload int
	verifyCRC  bool
}

// ReaderOption configures a Reader.
type ReaderOption func(*Reader)

// WithMaxPayload sets the maximum payload size (default: 64 MiB).
func WithMaxPayload(max int) ReaderOption {
	return func(r *Reader) {
		r.maxPayload = max
	}
}

// WithCRCVerification enables CRC verification.
func WithCRCVerification() ReaderOption {
	return func(r *Reader) {
		r.verifyCRC = true
	}
}

// NewReader creates a new GS1-T frame reader.
func NewReader(r io.Reader, opts ...ReaderOption) *Reader {
	reader := &Reader{
		r:          bufio.NewReader(r),
		maxPayload: MaxPayloadSize,
		verifyCRC:  true, // verify by default
	}
	for _, opt := range opts {
		opt(reader)
	}
	return reader
}

// Next reads and returns the next frame.
// Returns io.EOF when no more frames are available.
func (r *Reader) Next() (*Frame, error) {
	// Read header line
	headerLine, err := r.r.ReadString('\n')
	if err != nil {
		if err == io.EOF && headerLine == "" {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Parse header
	frame, err := r.parseHeader(headerLine)
	if err != nil {
		return nil, err
	}

	// Read exact payload bytes
	payloadLen := len(frame.Payload) // temporary, set by parseHeader
	if payloadLen > r.maxPayload {
		return nil, &ParseError{Reason: fmt.Sprintf("payload too large: %d > %d", payloadLen, r.maxPayload), Offset: -1}
	}

	if payloadLen > 0 {
		frame.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r.r, frame.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	} else {
		frame.Payload = nil
	}

	// Consume trailing newline (optional at EOF)
	if b, err := r.r.ReadByte(); err == nil {
		if b != '\n' {
			// Put it back - it's part of the next frame
			r.r.UnreadByte()
		}
	}

	// Verify CRC if present and verification enabled
	if r.verifyCRC && frame.CRC != nil {
		computed := ComputeCRC(frame.Payload)
		if computed != *frame.CRC {
			return nil, &CRCMismatchError{Expected: *frame.CRC, Got: computed}
		}
	}

	return frame, nil
}

// parseHeader parses the @frame{...} header line.
func (r *Reader) parseHeader(line string) (*Frame, error) {
	line = strings.TrimSpace(line)

	// Check prefix
	if !strings.HasPrefix(line, "@frame{") {
		return nil, &ParseError{Reason: "expected @frame{", Offset: 0}
	}

	// Find closing brace
	endIdx := strings.LastIndex(line, "}")
	if endIdx < 0 {
		return nil, &ParseError{Reason: "missing closing }", Offset: len(line)}
	}

	// Extract key=value content
	content := line[7:endIdx]

	// Parse key=value pairs
	frame := &Frame{Version: 1}
	var payloadLen int

	for _, pair := range tokenize(content) {
		eqIdx := strings.Index(pair, "=")
		if eqIdx < 0 {
			continue // skip malformed pairs
		}
		key := pair[:eqIdx]
		val := pair[eqIdx+1:]

		switch key {
		case "v":
			v, err := strconv.ParseUint(val, 10, 8)
			if err != nil {
				return nil, &ParseError{Reason: "invalid version", Offset: -1}
			}
			frame.Version = uint8(v)

		case "sid":
			sid, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return nil, &ParseError{Reason: "invalid sid", Offset: -1}
			}
			frame.SID = sid

		case "seq":
			seq, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return nil, &ParseError{Reason: "invalid seq", Offset: -1}
			}
			frame.Seq = seq

		case "kind":
			kind, ok := ParseKind(val)
			if !ok {
				return nil, &ParseError{Reason: "invalid kind: " + val, Offset: -1}
			}
			frame.Kind = kind

		case "len":
			l, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return nil, &ParseError{Reason: "invalid len", Offset: -1}
			}
			payloadLen = int(l)

		case "crc":
			crc, ok := parseCRC(val)
			if !ok {
				return nil, &ParseError{Reason: "invalid crc: " + val, Offset: -1}
			}
			frame.CRC = &crc

		case "base":
			base, ok := parseBase(val)
			if !ok {
				return nil, &ParseError{Reason: "invalid base: " + val, Offset: -1}
			}
			frame.Base = &base

		case "final":
			frame.Final = val == "true" || val == "1"

		case "flags":
			flags, err := strconv.ParseUint(val, 16, 8)
			if err == nil {
				frame.Flags = Flags(flags)
			}
		}
	}

	// Use payloadLen as temporary storage (we need it for reading)
	frame.Payload = make([]byte, payloadLen)

	return frame, nil
}

// tokenize splits key=value pairs separated by spaces or commas.
func tokenize(s string) []string {
	var tokens []string
	var current bytes.Buffer
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			current.WriteByte(c)
		case (c == ' ' || c == ',' || c == '\t') && !inQuote:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// parseCRC parses CRC value: "crc32:XXXXXXXX" or "XXXXXXXX"
func parseCRC(val string) (uint32, bool) {
	// Strip optional prefix
	val = strings.TrimPrefix(val, "crc32:")

	if len(val) != 8 {
		return 0, false
	}

	v, err := strconv.ParseUint(val, 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(v), true
}

// parseBase parses base hash: "sha256:XXXX..." or "XXXX..."
func parseBase(val string) ([32]byte, bool) {
	// Strip optional prefix
	val = strings.TrimPrefix(val, "sha256:")
	return HexToHash(val)
}

// ReadAll reads all frames until EOF.
func (r *Reader) ReadAll() ([]*Frame, error) {
	var frames []*Frame
	for {
		frame, err := r.Next()
		if err == io.EOF {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, frame)
	}
}
