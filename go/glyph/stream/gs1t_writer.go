package stream

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Writer writes GS1-T (text) frames to an io.Writer.
type Writer struct {
	w       io.Writer
	withCRC bool // Whether to compute and include CRC
}

// NewWriter creates a new GS1-T frame writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// NewWriterWithCRC creates a writer that computes CRC for each frame.
func NewWriterWithCRC(w io.Writer) *Writer {
	return &Writer{w: w, withCRC: true}
}

// WriteFrame writes a single frame in GS1-T format.
//
// Format:
//
//	@frame{v=1 sid=N seq=N kind=K len=N [crc=X] [base=sha256:X] [final=true]}\n
//	<payload bytes>\n
func (w *Writer) WriteFrame(f *Frame) error {
	var header strings.Builder
	header.WriteString("@frame{")

	// Required fields
	header.WriteString("v=")
	if f.Version == 0 {
		header.WriteByte('1')
	} else {
		header.WriteString(strconv.Itoa(int(f.Version)))
	}

	header.WriteString(" sid=")
	header.WriteString(strconv.FormatUint(f.SID, 10))

	header.WriteString(" seq=")
	header.WriteString(strconv.FormatUint(f.Seq, 10))

	header.WriteString(" kind=")
	header.WriteString(f.Kind.String())

	header.WriteString(" len=")
	header.WriteString(strconv.Itoa(len(f.Payload)))

	// Optional CRC
	crc := f.CRC
	if crc == nil && w.withCRC && len(f.Payload) > 0 {
		computed := ComputeCRC(f.Payload)
		crc = &computed
	}
	if crc != nil {
		header.WriteString(" crc=")
		header.WriteString(fmt.Sprintf("%08x", *crc))
	}

	// Optional base hash
	if f.Base != nil {
		header.WriteString(" base=sha256:")
		header.WriteString(HashToHex(*f.Base))
	}

	// Optional final flag
	if f.Final || f.Flags&FlagFinal != 0 {
		header.WriteString(" final=true")
	}

	header.WriteString("}\n")

	// Write header
	if _, err := io.WriteString(w.w, header.String()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write payload
	if len(f.Payload) > 0 {
		if _, err := w.w.Write(f.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}

	// Write trailing newline
	if _, err := io.WriteString(w.w, "\n"); err != nil {
		return fmt.Errorf("write trailing newline: %w", err)
	}

	return nil
}

// WriteDoc writes a doc frame with the given payload.
func (w *Writer) WriteDoc(sid, seq uint64, payload []byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindDoc,
		Payload: payload,
	})
}

// WritePatch writes a patch frame with optional base hash.
func (w *Writer) WritePatch(sid, seq uint64, payload []byte, base *[32]byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindPatch,
		Payload: payload,
		Base:    base,
	})
}

// WriteRow writes a row frame.
func (w *Writer) WriteRow(sid, seq uint64, payload []byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindRow,
		Payload: payload,
	})
}

// WriteUI writes a UI event frame.
func (w *Writer) WriteUI(sid, seq uint64, payload []byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindUI,
		Payload: payload,
	})
}

// WriteAck writes an acknowledgement frame (typically no payload).
func (w *Writer) WriteAck(sid, seq uint64) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindAck,
		Payload: nil,
	})
}

// WriteErr writes an error frame.
func (w *Writer) WriteErr(sid, seq uint64, payload []byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindErr,
		Payload: payload,
	})
}

// WritePing writes a ping frame.
func (w *Writer) WritePing(sid, seq uint64) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindPing,
		Payload: nil,
	})
}

// WritePong writes a pong frame.
func (w *Writer) WritePong(sid, seq uint64) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    KindPong,
		Payload: nil,
	})
}

// WriteFinal writes a final frame for a stream.
func (w *Writer) WriteFinal(sid, seq uint64, kind FrameKind, payload []byte) error {
	return w.WriteFrame(&Frame{
		Version: Version,
		SID:     sid,
		Seq:     seq,
		Kind:    kind,
		Payload: payload,
		Final:   true,
	})
}
