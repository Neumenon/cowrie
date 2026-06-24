package cowrie

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goldenVector mirrors one entry of testdata/v1_golden.json: the canonical
// framed bytes for a value (plus fingerprints we ignore here).
type goldenVector struct {
	CanonicalHex string `json:"canonical_hex"`
}

// TestGoldenScatterRecode gates the EncodeToWriter scatter-gather path (which the
// cross-language conformance gate, driven through the contiguous Encode, does not
// exercise): for every golden vector, strict-decoding the canonical bytes and
// re-encoding them via EncodeToWriter must be byte-identical to the original
// canonical bytes. This catches scatter-path drift such as a raw-byte (vs svarint)
// Decimal128 scale.
func TestGoldenScatterRecode(t *testing.T) {
	path := filepath.Join("..", "testdata", "v1_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	// The oracle emits bare NaN/Infinity literals in the (unused-here) "value"
	// field; Go's encoding/json rejects those. We only consume canonical_hex, so
	// quote those literals to make the document strict-JSON parseable.
	clean := raw
	for _, lit := range []string{"-Infinity", "Infinity", "NaN"} {
		clean = []byte(strings.ReplaceAll(string(clean), ": "+lit+",", ": \""+lit+"\","))
	}
	var vectors map[string]goldenVector
	if err := json.Unmarshal(clean, &vectors); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(vectors) == 0 {
		t.Fatal("no golden vectors loaded")
	}

	for name, vec := range vectors {
		if vec.CanonicalHex == "" {
			continue
		}
		name, vec := name, vec
		t.Run(name, func(t *testing.T) {
			want, err := hex.DecodeString(vec.CanonicalHex)
			if err != nil {
				t.Fatalf("bad canonical_hex: %v", err)
			}
			// Strict decode so we only round-trip canonical inputs.
			val, err := DecodeFramedStrict(want)
			if err != nil {
				t.Fatalf("DecodeFramedStrict: %v", err)
			}
			var buf bytes.Buffer
			if err := EncodeToWriter(&buf, val); err != nil {
				t.Fatalf("EncodeToWriter: %v", err)
			}
			if got := buf.Bytes(); !bytes.Equal(got, want) {
				t.Fatalf("scatter recode mismatch\n got: %x\nwant: %x", got, want)
			}
		})
	}
}
