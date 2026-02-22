package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

// StreamWriter writes length-prefixed Cowrie records to an output stream.
// Format: [u32 length][cowrie bytes] per record.
// This is the Cowrie equivalent of JSON Lines (JSONL).
type StreamWriter struct {
	w   io.Writer
	buf []byte
}

// NewStreamWriter creates a new Cowrie stream writer.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{
		w:   w,
		buf: make([]byte, 0, 4096),
	}
}

// Write encodes a value as Cowrie and writes it as a length-prefixed frame.
func (sw *StreamWriter) Write(v any) error {
	// Encode to Cowrie
	data, err := EncodeBytes(v)
	if err != nil {
		return err
	}

	// Write length prefix (4 bytes, little-endian)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := sw.w.Write(lenBuf[:]); err != nil {
		return err
	}

	// Write Cowrie data
	_, err = sw.w.Write(data)
	return err
}

// Sync flushes the underlying writer if it supports Sync.
func (sw *StreamWriter) Sync() error {
	if syncer, ok := sw.w.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}

// StreamReader reads length-prefixed Cowrie records from input data.
type StreamReader struct {
	data []byte
	pos  int
}

// NewStreamReader creates a new Cowrie stream reader.
func NewStreamReader(data []byte) *StreamReader {
	return &StreamReader{data: data}
}

// Next reads the next record from the stream.
// Returns io.EOF when no more records.
func (sr *StreamReader) Next(target any) error {
	// Check for end of data
	if sr.pos >= len(sr.data) {
		return io.EOF
	}

	// Need at least 4 bytes for length
	if sr.pos+4 > len(sr.data) {
		return errors.New("cowrie stream: truncated length prefix")
	}

	// Read length prefix
	frameLen := binary.LittleEndian.Uint32(sr.data[sr.pos:])
	sr.pos += 4

	// Zero length means end of stream (optional marker)
	if frameLen == 0 {
		return io.EOF
	}

	// Check we have enough data
	if sr.pos+int(frameLen) > len(sr.data) {
		return errors.New("cowrie stream: truncated frame data")
	}

	// Decode Cowrie frame
	frameData := sr.data[sr.pos : sr.pos+int(frameLen)]
	sr.pos += int(frameLen)

	return DecodeBytes(frameData, target)
}

// ReadAll reads all records from the stream into a slice.
func ReadAllStream[T any](data []byte) ([]T, error) {
	sr := NewStreamReader(data)
	var results []T
	for {
		var item T
		err := sr.Next(&item)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, nil
}

// Position returns the current read position.
func (sr *StreamReader) Position() int {
	return sr.pos
}

// Remaining returns the number of bytes remaining to read.
func (sr *StreamReader) Remaining() int {
	return len(sr.data) - sr.pos
}
