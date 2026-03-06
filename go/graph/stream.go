package graph

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/Neumenon/cowrie/go"
)

// Errors
var (
	ErrInvalidMagic   = errors.New("graph-stream: invalid magic bytes")
	ErrInvalidVersion = errors.New("graph-stream: unsupported version")
	ErrUnexpectedEOF  = errors.New("graph-stream: unexpected end of data")
	ErrInvalidEvent   = errors.New("graph-stream: invalid event type")
)

// StreamWriter writes graph events to an output stream.
type StreamWriter struct {
	w      io.Writer
	header *StreamHeader
	buf    []byte
	closed bool
}

// NewStreamWriter creates a new stream writer.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{
		w:      w,
		header: NewStreamHeader(),
		buf:    make([]byte, 0, 4096),
	}
}

// WriteHeader writes the stream header. Must be called before writing events.
func (sw *StreamWriter) WriteHeader() error {
	sw.buf = sw.buf[:0]

	// Magic bytes
	sw.buf = append(sw.buf, Magic0, Magic1, Magic2, Magic3)

	// Version and flags
	sw.buf = append(sw.buf, Version)
	sw.buf = append(sw.buf, sw.header.Flags)

	// Field dictionary
	sw.buf = appendUvarint(sw.buf, uint64(len(sw.header.FieldDict)))
	for _, field := range sw.header.FieldDict {
		sw.buf = appendString(sw.buf, field)
	}

	// Label dictionary
	sw.buf = appendUvarint(sw.buf, uint64(len(sw.header.LabelDict)))
	for _, label := range sw.header.LabelDict {
		sw.buf = appendString(sw.buf, label)
	}

	// Predicate dictionary (if present)
	if sw.header.Flags&FlagHasPredDict != 0 {
		sw.buf = appendUvarint(sw.buf, uint64(len(sw.header.PredDict)))
		for _, pred := range sw.header.PredDict {
			sw.buf = appendString(sw.buf, pred)
		}
	}

	_, err := sw.w.Write(sw.buf)
	return err
}

// Header returns the stream header for pre-populating dictionaries.
func (sw *StreamWriter) Header() *StreamHeader {
	return sw.header
}

// WriteNode writes a node event.
func (sw *StreamWriter) WriteNode(evt *NodeEvent) error {
	return sw.writeEvent(NewNodeEvent(evt))
}

// WriteEdge writes an edge event.
func (sw *StreamWriter) WriteEdge(evt *EdgeEvent) error {
	return sw.writeEvent(NewEdgeEvent(evt))
}

// WriteTriple writes a triple event.
func (sw *StreamWriter) WriteTriple(evt *TripleEvent) error {
	return sw.writeEvent(NewTripleEvent(evt))
}

// writeEvent encodes an event as a frame and writes it.
func (sw *StreamWriter) writeEvent(evt Event) error {
	// Encode event to Cowrie value
	v := encodeEvent(evt, sw.header)

	// Encode Cowrie value
	data, err := cowrie.Encode(v)
	if err != nil {
		return err
	}

	// Write frame: [len:u32][data]
	sw.buf = sw.buf[:0]
	sw.buf = appendUint32(sw.buf, uint32(len(data)))
	sw.buf = append(sw.buf, data...)

	_, err = sw.w.Write(sw.buf)
	return err
}

// Close closes the stream writer.
func (sw *StreamWriter) Close() error {
	if sw.closed {
		return nil
	}
	sw.closed = true
	// Write end-of-stream marker (zero-length frame)
	sw.buf = sw.buf[:0]
	sw.buf = appendUint32(sw.buf, 0)
	_, err := sw.w.Write(sw.buf)
	return err
}

// StreamReader reads graph events from an input stream.
type StreamReader struct {
	data   []byte
	pos    int
	header *StreamHeader
}

// NewStreamReader creates a new stream reader.
func NewStreamReader(data []byte) (*StreamReader, error) {
	sr := &StreamReader{data: data}
	if err := sr.readHeader(); err != nil {
		return nil, err
	}
	return sr, nil
}

// Header returns the parsed stream header.
func (sr *StreamReader) Header() *StreamHeader {
	return sr.header
}

// readHeader parses the stream header.
func (sr *StreamReader) readHeader() error {
	// Magic bytes
	if sr.pos+6 > len(sr.data) {
		return ErrUnexpectedEOF
	}
	if sr.data[sr.pos] != Magic0 || sr.data[sr.pos+1] != Magic1 ||
		sr.data[sr.pos+2] != Magic2 || sr.data[sr.pos+3] != Magic3 {
		return ErrInvalidMagic
	}
	sr.pos += 4

	// Version
	if sr.data[sr.pos] != Version {
		return ErrInvalidVersion
	}
	sr.pos++

	// Flags
	flags := sr.data[sr.pos]
	sr.pos++

	sr.header = NewStreamHeader()
	sr.header.Flags = flags

	// Field dictionary
	fieldCount, err := sr.readUvarint()
	if err != nil {
		return err
	}
	sr.header.FieldDict = make([]string, fieldCount)
	for i := uint64(0); i < fieldCount; i++ {
		s, err := sr.readString()
		if err != nil {
			return err
		}
		sr.header.FieldDict[i] = s
	}

	// Label dictionary
	labelCount, err := sr.readUvarint()
	if err != nil {
		return err
	}
	sr.header.LabelDict = make([]string, labelCount)
	for i := uint64(0); i < labelCount; i++ {
		s, err := sr.readString()
		if err != nil {
			return err
		}
		sr.header.LabelDict[i] = s
	}

	// Predicate dictionary (if present)
	if flags&FlagHasPredDict != 0 {
		predCount, err := sr.readUvarint()
		if err != nil {
			return err
		}
		sr.header.PredDict = make([]string, predCount)
		for i := uint64(0); i < predCount; i++ {
			s, err := sr.readString()
			if err != nil {
				return err
			}
			sr.header.PredDict[i] = s
		}
	}

	return nil
}

// Next reads the next event from the stream.
// Returns nil, nil when the stream ends.
func (sr *StreamReader) Next() (*Event, error) {
	// Read frame length
	if sr.pos+4 > len(sr.data) {
		return nil, nil // End of stream
	}
	frameLen := binary.LittleEndian.Uint32(sr.data[sr.pos:])
	sr.pos += 4

	if frameLen == 0 {
		return nil, nil // End-of-stream marker
	}

	if sr.pos+int(frameLen) > len(sr.data) {
		return nil, ErrUnexpectedEOF
	}

	// Decode Cowrie frame
	frameData := sr.data[sr.pos : sr.pos+int(frameLen)]
	sr.pos += int(frameLen)

	v, err := cowrie.Decode(frameData)
	if err != nil {
		return nil, err
	}

	// Decode event from Cowrie value
	evt, err := decodeEvent(v, sr.header)
	if err != nil {
		return nil, err
	}

	return evt, nil
}

// ReadAll reads all events from the stream.
func (sr *StreamReader) ReadAll() ([]Event, error) {
	var events []Event
	for {
		evt, err := sr.Next()
		if err != nil {
			return nil, err
		}
		if evt == nil {
			break
		}
		events = append(events, *evt)
	}
	return events, nil
}

// Helper functions

func (sr *StreamReader) readUvarint() (uint64, error) {
	v, n := binary.Uvarint(sr.data[sr.pos:])
	if n <= 0 {
		return 0, ErrUnexpectedEOF
	}
	sr.pos += n
	return v, nil
}

func (sr *StreamReader) readString() (string, error) {
	length, err := sr.readUvarint()
	if err != nil {
		return "", err
	}
	if sr.pos+int(length) > len(sr.data) {
		return "", ErrUnexpectedEOF
	}
	s := string(sr.data[sr.pos : sr.pos+int(length)])
	sr.pos += int(length)
	return s, nil
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}
