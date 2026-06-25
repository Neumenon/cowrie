// Package codec provides a unified interface for encoding/decoding HTTP payloads
// with support for both JSON and Cowrie formats via content-type negotiation.
package codec

import (
	"io"
	"net/http"
	"strings"
)

// Content-Type constants for negotiation
const (
	ContentTypeJSON  = "application/json"
	ContentTypeCowrie = "application/cowrie"
)

// Codec provides encoding/decoding for HTTP payloads.
// Implementations must be safe for concurrent use.
type Codec interface {
	// ContentType returns the MIME type for this codec (e.g., "application/json")
	ContentType() string

	// Encode encodes v and writes to w
	Encode(w io.Writer, v any) error

	// Decode reads from r and decodes into v
	Decode(r io.Reader, v any) error
}

// FromRequest selects the appropriate codec based on the Accept header.
// Returns JSON codec if Accept is missing or not recognized (safe default).
func FromRequest(r *http.Request) Codec {
	accept := r.Header.Get("Accept")
	return fromAccept(accept)
}

// FromContentType selects the appropriate codec based on Content-Type header.
// Returns JSON codec if Content-Type is missing or not recognized (safe default).
func FromContentType(contentType string) Codec {
	if strings.Contains(contentType, ContentTypeCowrie) {
		return &CowrieCodec{}
	}
	return &JSONCodec{} // Safe default
}

// fromAccept parses Accept header and returns the best matching codec.
func fromAccept(accept string) Codec {
	// Simple matching: check if Cowrie is explicitly requested
	if strings.Contains(accept, ContentTypeCowrie) {
		return &CowrieCodec{}
	}
	return &JSONCodec{} // Safe default
}
