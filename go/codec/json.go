package codec

import (
	"encoding/json"
	"io"
)

// JSONCodec implements Codec using standard encoding/json.
// This is the default codec for backward compatibility.
type JSONCodec struct{}

// ContentType returns "application/json".
func (JSONCodec) ContentType() string {
	return ContentTypeJSON
}

// Encode encodes v as JSON and writes to w.
func (JSONCodec) Encode(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

// Decode decodes JSON from r into v.
func (JSONCodec) Decode(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}
