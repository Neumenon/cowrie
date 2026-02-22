package cowrie

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type fixtureManifest struct {
	Cases []fixtureCase `json:"cases"`
}

type fixtureCase struct {
	ID     string        `json:"id"`
	Gen    int           `json:"gen"`
	Kind   string        `json:"kind"`
	Input  string        `json:"input"`
	Expect fixtureExpect `json:"expect"`
}

type fixtureExpect struct {
	OK    bool   `json:"ok"`
	JSON  string `json:"json"`
	Error string `json:"error"`
}

func TestFixturesCore(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join(".."))
	manifestPath := filepath.Join(repoRoot, "testdata", "fixtures", "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var manifest fixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}

	for _, c := range manifest.Cases {
		if c.Gen != 2 || c.Kind != "decode" {
			continue
		}

		casePath := filepath.Join(repoRoot, "testdata", "fixtures", c.Input)
		data, err := os.ReadFile(casePath)
		if err != nil {
			t.Fatalf("%s: read input: %v", c.ID, err)
		}

		val, err := Decode(data)
		if c.Expect.OK {
			if err != nil {
				t.Fatalf("%s: decode failed: %v", c.ID, err)
			}
			if c.Expect.JSON == "" {
				continue
			}

			expectedPath := filepath.Join(repoRoot, "testdata", "fixtures", c.Expect.JSON)
			expectedBytes, err := os.ReadFile(expectedPath)
			if err != nil {
				t.Fatalf("%s: read expected json: %v", c.ID, err)
			}

			var expectedAny any
			if err := json.Unmarshal(expectedBytes, &expectedAny); err != nil {
				t.Fatalf("%s: parse expected json: %v", c.ID, err)
			}

			actualJSON, err := ToJSON(val)
			if err != nil {
				t.Fatalf("%s: ToJSON failed: %v", c.ID, err)
			}
			var actualAny any
			if err := json.Unmarshal(actualJSON, &actualAny); err != nil {
				t.Fatalf("%s: parse actual json: %v", c.ID, err)
			}

			if !reflect.DeepEqual(actualAny, expectedAny) {
				t.Fatalf("%s: mismatch\nexpected: %s\nactual:   %s", c.ID, string(expectedBytes), string(actualJSON))
			}
			continue
		}

		if err == nil {
			t.Fatalf("%s: expected error but got nil", c.ID)
		}
		if code := mapErrorCode(err); code != c.Expect.Error {
			t.Fatalf("%s: expected error code %s, got %s (%v)", c.ID, c.Expect.Error, code, err)
		}
	}
}

func mapErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var tagErr *TagError
	if errors.As(err, &tagErr) {
		return "ERR_INVALID_TAG"
	}
	switch {
	case errors.Is(err, ErrInvalidMagic):
		return "ERR_INVALID_MAGIC"
	case errors.Is(err, ErrInvalidVersion):
		return "ERR_INVALID_VERSION"
	case errors.Is(err, ErrUnexpectedEOF):
		return "ERR_TRUNCATED"
	case errors.Is(err, ErrInvalidVarint):
		return "ERR_INVALID_VARINT"
	case errors.Is(err, ErrInvalidFieldID):
		return "ERR_INVALID_FIELD_ID"
	case errors.Is(err, ErrMalformedLength):
		return "ERR_TOO_LARGE"
	case errors.Is(err, ErrDictTooLarge):
		return "ERR_DICT_TOO_LARGE"
	case errors.Is(err, ErrStringTooLarge):
		return "ERR_STRING_TOO_LARGE"
	case errors.Is(err, ErrBytesTooLarge):
		return "ERR_BYTES_TOO_LARGE"
	case errors.Is(err, ErrExtTooLarge):
		return "ERR_EXT_TOO_LARGE"
	case errors.Is(err, ErrDepthExceeded):
		return "ERR_TOO_DEEP"
	case errors.Is(err, ErrArrayTooLarge), errors.Is(err, ErrObjectTooLarge):
		return "ERR_TOO_LARGE"
	default:
		return ""
	}
}
