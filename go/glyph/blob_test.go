package glyph

import (
	"fmt"
	"sync"
	"testing"
)

func TestBlobRef_String(t *testing.T) {
	tests := []struct {
		name string
		ref  BlobRef
		want string
	}{
		{
			name: "basic",
			ref: BlobRef{
				CID:   "sha256:abc123",
				MIME:  "image/png",
				Bytes: 1024,
			},
			want: "@blob cid=sha256:abc123 mime=image/png bytes=1024",
		},
		{
			name: "with_name",
			ref: BlobRef{
				CID:   "sha256:def456",
				MIME:  "text/plain",
				Bytes: 512,
				Name:  "readme.txt",
			},
			want: "@blob cid=sha256:def456 mime=text/plain bytes=512 name=readme.txt",
		},
		{
			name: "with_caption",
			ref: BlobRef{
				CID:     "sha256:ghi789",
				MIME:    "image/jpeg",
				Bytes:   45000,
				Name:    "chart.jpg",
				Caption: "Q4 revenue chart",
			},
			want: `@blob cid=sha256:ghi789 mime=image/jpeg bytes=45000 name=chart.jpg caption="Q4 revenue chart"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ref.String()
			if got != tt.want {
				t.Errorf("BlobRef.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseBlobRef(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    BlobRef
		wantErr bool
	}{
		{
			name:  "basic",
			input: "@blob cid=sha256:abc123 mime=image/png bytes=1024",
			want: BlobRef{
				CID:   "sha256:abc123",
				MIME:  "image/png",
				Bytes: 1024,
			},
		},
		{
			name:  "with_name_caption",
			input: `@blob cid=sha256:def456 mime=text/plain bytes=512 name=readme.txt caption="Hello World"`,
			want: BlobRef{
				CID:     "sha256:def456",
				MIME:    "text/plain",
				Bytes:   512,
				Name:    "readme.txt",
				Caption: "Hello World",
			},
		},
		{
			name:    "missing_cid",
			input:   "@blob mime=image/png bytes=1024",
			wantErr: true,
		},
		{
			name:    "missing_mime",
			input:   "@blob cid=sha256:abc123 bytes=1024",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBlobRef(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBlobRef() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.CID != tt.want.CID {
				t.Errorf("CID = %q, want %q", got.CID, tt.want.CID)
			}
			if got.MIME != tt.want.MIME {
				t.Errorf("MIME = %q, want %q", got.MIME, tt.want.MIME)
			}
			if got.Bytes != tt.want.Bytes {
				t.Errorf("Bytes = %d, want %d", got.Bytes, tt.want.Bytes)
			}
			if got.Name != tt.want.Name {
				t.Errorf("Name = %q, want %q", got.Name, tt.want.Name)
			}
			if got.Caption != tt.want.Caption {
				t.Errorf("Caption = %q, want %q", got.Caption, tt.want.Caption)
			}
		})
	}
}

func TestComputeCID(t *testing.T) {
	content := []byte("Hello, World!")
	cid := ComputeCID(content)

	if !hasPrefix(cid, "sha256:") {
		t.Errorf("CID should start with sha256:, got %s", cid)
	}

	// Same content should produce same CID
	cid2 := ComputeCID(content)
	if cid != cid2 {
		t.Errorf("Same content should produce same CID")
	}

	// Different content should produce different CID
	cid3 := ComputeCID([]byte("Different content"))
	if cid == cid3 {
		t.Errorf("Different content should produce different CID")
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func TestMemoryBlobRegistry(t *testing.T) {
	registry := NewMemoryBlobRegistry()

	content := []byte("Test content")
	mime := "text/plain"

	// Store
	cid, err := registry.Put(content, mime)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check existence
	if !registry.Has(cid) {
		t.Error("Has should return true for stored blob")
	}
	if registry.Has("sha256:nonexistent") {
		t.Error("Has should return false for non-existent blob")
	}

	// Retrieve
	gotContent, gotMime, err := registry.Get(cid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(gotContent) != string(content) {
		t.Errorf("Content mismatch: got %q, want %q", gotContent, content)
	}
	if gotMime != mime {
		t.Errorf("MIME mismatch: got %q, want %q", gotMime, mime)
	}

	// Metadata
	metaMime, metaBytes, err := registry.Meta(cid)
	if err != nil {
		t.Fatalf("Meta failed: %v", err)
	}
	if metaMime != mime {
		t.Errorf("Meta MIME mismatch: got %q, want %q", metaMime, mime)
	}
	if metaBytes != int64(len(content)) {
		t.Errorf("Meta Bytes mismatch: got %d, want %d", metaBytes, len(content))
	}
}

func TestBlobValue(t *testing.T) {
	ref := BlobRef{
		CID:     "sha256:test",
		MIME:    "image/png",
		Bytes:   1024,
		Caption: "Test image",
	}

	v := Blob(ref)

	if !v.IsBlob() {
		t.Error("IsBlob should return true")
	}

	got := v.AsBlob()
	if got.CID != ref.CID {
		t.Errorf("CID mismatch: got %q, want %q", got.CID, ref.CID)
	}
	if got.Caption != ref.Caption {
		t.Errorf("Caption mismatch: got %q, want %q", got.Caption, ref.Caption)
	}
}

func TestBlobFromContent(t *testing.T) {
	content := []byte("Hello blob!")
	v := BlobFromContent(content, "text/plain", "hello.txt", "Greeting file")

	if !v.IsBlob() {
		t.Error("Should be a blob")
	}

	ref := v.AsBlob()
	if ref.MIME != "text/plain" {
		t.Errorf("MIME = %q, want text/plain", ref.MIME)
	}
	if ref.Bytes != int64(len(content)) {
		t.Errorf("Bytes = %d, want %d", ref.Bytes, len(content))
	}
	if ref.Name != "hello.txt" {
		t.Errorf("Name = %q, want hello.txt", ref.Name)
	}
	if ref.Caption != "Greeting file" {
		t.Errorf("Caption = %q, want Greeting file", ref.Caption)
	}
}

func TestMemoryBlobRegistry_Concurrent(t *testing.T) {
	registry := NewMemoryBlobRegistry()

	// Run concurrent operations
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			content := []byte(fmt.Sprintf("content-%d", i))

			// Put
			cid, err := registry.Put(content, "text/plain")
			if err != nil {
				t.Errorf("Put failed: %v", err)
				return
			}

			// Has
			if !registry.Has(cid) {
				t.Errorf("Has(%s) should be true", cid)
			}

			// Get
			gotContent, gotMime, err := registry.Get(cid)
			if err != nil {
				t.Errorf("Get failed: %v", err)
				return
			}
			if string(gotContent) != string(content) {
				t.Errorf("Content mismatch")
			}
			if gotMime != "text/plain" {
				t.Errorf("MIME mismatch")
			}

			// Meta
			_, _, err = registry.Meta(cid)
			if err != nil {
				t.Errorf("Meta failed: %v", err)
			}
		}(i)
	}
	wg.Wait()
	// If we get here without race detector complaining, thread safety is working
}
