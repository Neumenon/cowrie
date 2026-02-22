package glyph

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// ============================================================
// Blob Reference Types
// ============================================================

// BlobRef represents a content-addressed blob reference.
type BlobRef struct {
	CID     string // Content ID: "sha256:<hex>" or "blake3:<hex>"
	MIME    string // MIME type: "image/png", "text/plain", etc.
	Bytes   int64  // Size in bytes
	Name    string // Optional: original filename
	Caption string // Optional: short description for LLM context (≤100 chars)
	Preview string // Optional: tiny inline preview (≤500 chars)
}

// Algorithm returns the hash algorithm from the CID.
func (b BlobRef) Algorithm() string {
	if idx := strings.Index(b.CID, ":"); idx > 0 {
		return b.CID[:idx]
	}
	return "sha256"
}

// Hash returns just the hash part of the CID.
func (b BlobRef) Hash() string {
	if idx := strings.Index(b.CID, ":"); idx > 0 {
		return b.CID[idx+1:]
	}
	return b.CID
}

// String returns the canonical blob reference format.
func (b BlobRef) String() string {
	var sb strings.Builder
	sb.WriteString("@blob cid=")
	sb.WriteString(b.CID)
	sb.WriteString(" mime=")
	sb.WriteString(b.MIME)
	sb.WriteString(" bytes=")
	fmt.Fprintf(&sb, "%d", b.Bytes)

	if b.Name != "" {
		sb.WriteString(" name=")
		sb.WriteString(canonString(b.Name))
	}
	if b.Caption != "" {
		sb.WriteString(" caption=")
		sb.WriteString(canonString(b.Caption))
	}
	if b.Preview != "" {
		sb.WriteString(" preview=")
		sb.WriteString(canonString(b.Preview))
	}

	return sb.String()
}

// ============================================================
// Blob Registry
// ============================================================

// BlobRegistry provides blob storage and retrieval.
type BlobRegistry interface {
	// Put stores content and returns its CID.
	Put(content []byte, mime string) (cid string, err error)

	// Get retrieves content by CID.
	Get(cid string) (content []byte, mime string, err error)

	// Has checks if a blob exists.
	Has(cid string) bool

	// Meta returns metadata without fetching content.
	Meta(cid string) (mime string, bytes int64, err error)
}

// MemoryBlobRegistry is a simple in-memory blob registry.
// It is thread-safe for concurrent access.
type MemoryBlobRegistry struct {
	blobs map[string]blobEntry
	mu    sync.RWMutex
}

type blobEntry struct {
	content []byte
	mime    string
}

// NewMemoryBlobRegistry creates a new in-memory blob registry.
func NewMemoryBlobRegistry() *MemoryBlobRegistry {
	return &MemoryBlobRegistry{
		blobs: make(map[string]blobEntry),
	}
}

// Put stores content and returns its CID.
func (r *MemoryBlobRegistry) Put(content []byte, mime string) (string, error) {
	cid := ComputeCID(content)
	r.mu.Lock()
	r.blobs[cid] = blobEntry{content: content, mime: mime}
	r.mu.Unlock()
	return cid, nil
}

// Get retrieves content by CID.
func (r *MemoryBlobRegistry) Get(cid string) ([]byte, string, error) {
	r.mu.RLock()
	entry, ok := r.blobs[cid]
	r.mu.RUnlock()
	if !ok {
		return nil, "", fmt.Errorf("blob not found: %s", cid)
	}
	return entry.content, entry.mime, nil
}

// Has checks if a blob exists.
func (r *MemoryBlobRegistry) Has(cid string) bool {
	r.mu.RLock()
	_, ok := r.blobs[cid]
	r.mu.RUnlock()
	return ok
}

// Meta returns metadata without fetching content.
func (r *MemoryBlobRegistry) Meta(cid string) (string, int64, error) {
	r.mu.RLock()
	entry, ok := r.blobs[cid]
	r.mu.RUnlock()
	if !ok {
		return "", 0, fmt.Errorf("blob not found: %s", cid)
	}
	return entry.mime, int64(len(entry.content)), nil
}

// ============================================================
// CID Computation
// ============================================================

// ComputeCID computes a content ID using SHA-256.
func ComputeCID(content []byte) string {
	hash := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(hash[:])
}

// ComputeCIDWithAlgorithm computes a CID with the specified algorithm.
func ComputeCIDWithAlgorithm(content []byte, algorithm string) (string, error) {
	switch algorithm {
	case "sha256":
		hash := sha256.Sum256(content)
		return "sha256:" + hex.EncodeToString(hash[:]), nil
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}
}

// ============================================================
// Blob Value Type
// ============================================================

// TypeBlob is the GType for blob references
const TypeBlob GType = 20

// Blob creates a blob reference value.
func Blob(ref BlobRef) *GValue {
	return &GValue{
		typ:     TypeBlob,
		blobVal: &ref,
	}
}

// BlobFromContent creates a blob reference from content.
func BlobFromContent(content []byte, mime, name, caption string) *GValue {
	cid := ComputeCID(content)
	return Blob(BlobRef{
		CID:     cid,
		MIME:    mime,
		Bytes:   int64(len(content)),
		Name:    name,
		Caption: caption,
	})
}

// AsBlob returns the blob reference. Panics if not a blob.
func (v *GValue) AsBlob() BlobRef {
	if v.typ != TypeBlob {
		panic("glyph: not a blob")
	}
	if v.blobVal == nil {
		return BlobRef{}
	}
	return *v.blobVal
}

// IsBlob returns true if this is a blob reference.
func (v *GValue) IsBlob() bool {
	return v != nil && v.typ == TypeBlob
}

// ============================================================
// Blob Emit/Parse
// ============================================================

// EmitBlob emits a blob reference in canonical format.
func EmitBlob(ref BlobRef) string {
	return ref.String()
}

// ParseBlobRef parses a blob reference from "@blob cid=... mime=... bytes=...".
func ParseBlobRef(input string) (*BlobRef, error) {
	if !strings.HasPrefix(input, "@blob ") {
		return nil, fmt.Errorf("expected @blob prefix")
	}

	rest := strings.TrimPrefix(input, "@blob ")
	ref := &BlobRef{}

	// Parse key=value pairs
	for len(rest) > 0 {
		rest = strings.TrimLeft(rest, " \t")
		if rest == "" {
			break
		}

		// Find key
		eqIdx := strings.Index(rest, "=")
		if eqIdx < 0 {
			break
		}
		key := rest[:eqIdx]
		rest = rest[eqIdx+1:]

		// Parse value
		var value string
		if len(rest) > 0 && rest[0] == '"' {
			// Quoted value
			endIdx := 1
			for endIdx < len(rest) {
				if rest[endIdx] == '"' && rest[endIdx-1] != '\\' {
					break
				}
				endIdx++
			}
			if endIdx >= len(rest) {
				return nil, fmt.Errorf("unterminated quoted value")
			}
			value = unquoteBlobString(rest[1:endIdx])
			rest = rest[endIdx+1:]
		} else {
			// Bare value
			spaceIdx := strings.IndexAny(rest, " \t\n")
			if spaceIdx < 0 {
				value = rest
				rest = ""
			} else {
				value = rest[:spaceIdx]
				rest = rest[spaceIdx:]
			}
		}

		switch key {
		case "cid":
			ref.CID = value
		case "mime":
			ref.MIME = value
		case "bytes":
			fmt.Sscanf(value, "%d", &ref.Bytes)
		case "name":
			ref.Name = value
		case "caption":
			ref.Caption = value
		case "preview":
			ref.Preview = value
		}
	}

	if ref.CID == "" {
		return nil, fmt.Errorf("missing required field: cid")
	}
	if ref.MIME == "" {
		return nil, fmt.Errorf("missing required field: mime")
	}

	return ref, nil
}

// unquoteBlobString removes escape sequences from a quoted string.
func unquoteBlobString(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				sb.WriteByte('\n')
			case 'r':
				sb.WriteByte('\r')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			default:
				sb.WriteByte(s[i+1])
			}
			i++
		} else {
			sb.WriteByte(s[i])
		}
	}
	return sb.String()
}
