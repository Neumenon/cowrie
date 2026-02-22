package glyph

import (
	"fmt"
	"strings"
)

// ============================================================
// LYPH v2 Header Parsing
// ============================================================
//
// Header format:
//   @lyph v2 @schema#abc123 @mode=auto @keys=wire
//
// Components:
//   @lyph v2       - Format identifier and version
//   @schema#hash   - Optional schema hash for validation
//   @mode=X        - Encoding mode: auto, struct, packed, tabular, patch
//   @keys=X        - Key format: wire, name, fid

// Header represents parsed LYPH v2 header information.
type Header struct {
	Version         string  // "v2"
	SchemaID        string  // Schema hash (optional)
	Mode            Mode    // Encoding mode
	KeyMode         KeyMode // Key format
	Target          RefID   // For patch mode: target document
	BaseFingerprint string  // For patch mode: base state fingerprint (v2.4.0)
	Raw             string  // Original header text
}

// ParseHeader parses a LYPH v2 header line.
// Returns nil if the input is not a v2 header.
func ParseHeader(input string) (*Header, error) {
	input = strings.TrimSpace(input)

	// Must start with @lyph or @glyph
	if !strings.HasPrefix(input, "@lyph") && !strings.HasPrefix(input, "@glyph") {
		return nil, nil // Not a v2 header
	}

	h := &Header{
		Version: "v2",
		Mode:    ModeAuto,
		KeyMode: KeyModeWire,
		Raw:     input,
	}

	// Tokenize by spaces
	tokens := tokenizeHeader(input)

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]

		switch {
		case tok == "@lyph" || tok == "@glyph":
			// Check for version
			if i+1 < len(tokens) && !strings.HasPrefix(tokens[i+1], "@") {
				h.Version = tokens[i+1]
				i++
			}

		case strings.HasPrefix(tok, "@schema#"):
			h.SchemaID = tok[8:] // Remove "@schema#"

		case strings.HasPrefix(tok, "@mode="):
			mode := tok[6:]
			switch mode {
			case "auto":
				h.Mode = ModeAuto
			case "struct":
				h.Mode = ModeStruct
			case "packed":
				h.Mode = ModePacked
			case "tabular", "tab":
				h.Mode = ModeTabular
			case "patch":
				h.Mode = ModePatch
			default:
				return nil, fmt.Errorf("unknown mode: %s", mode)
			}

		case strings.HasPrefix(tok, "@keys="):
			keys := tok[6:]
			switch keys {
			case "wire":
				h.KeyMode = KeyModeWire
			case "name":
				h.KeyMode = KeyModeName
			case "fid":
				h.KeyMode = KeyModeFID
			default:
				return nil, fmt.Errorf("unknown key mode: %s", keys)
			}

		case strings.HasPrefix(tok, "@target="):
			target := tok[8:]
			h.Target = parseRefIDFromTarget(target)

		case tok == "@patch":
			h.Mode = ModePatch

		case tok == "@tab":
			h.Mode = ModeTabular
		}
	}

	return h, nil
}

// tokenizeHeader splits header by spaces, respecting quoted strings.
func tokenizeHeader(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(input); i++ {
		c := input[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			current.WriteByte(c)
		case c == ' ' && !inQuote:
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

// parseRefIDFromTarget parses a target reference (without ^ prefix).
func parseRefIDFromTarget(s string) RefID {
	if strings.Contains(s, ":") {
		parts := strings.SplitN(s, ":", 2)
		return RefID{Prefix: parts[0], Value: parts[1]}
	}
	return RefID{Value: s}
}

// EmitHeader generates a LYPH v2 header string.
func EmitHeader(h *Header) string {
	var b strings.Builder

	b.WriteString("@lyph ")
	b.WriteString(h.Version)

	if h.SchemaID != "" {
		b.WriteString(" @schema#")
		b.WriteString(h.SchemaID)
	}

	if h.Mode != ModeAuto {
		b.WriteString(" @mode=")
		b.WriteString(h.Mode.String())
	}

	if h.KeyMode != KeyModeWire {
		b.WriteString(" @keys=")
		switch h.KeyMode {
		case KeyModeName:
			b.WriteString("name")
		case KeyModeFID:
			b.WriteString("fid")
		}
	}

	if h.Target.Value != "" {
		b.WriteString(" @target=")
		if h.Target.Prefix != "" {
			b.WriteString(h.Target.Prefix)
			b.WriteByte(':')
		}
		b.WriteString(h.Target.Value)
	}

	return b.String()
}

// ============================================================
// Document Structure
// ============================================================

// Document represents a complete LYPH v2 document.
type Document struct {
	Header *Header // Parsed header
	Body   *GValue // Main content
	Patch  *Patch  // For patch mode
	Errors []error // Parse errors (tolerant mode)
}

// DetectMode examines input to determine the document mode.
func DetectMode(input string) Mode {
	trimmed := strings.TrimSpace(input)

	// Check for explicit headers
	if strings.HasPrefix(trimmed, "@lyph") || strings.HasPrefix(trimmed, "@glyph") {
		h, _ := ParseHeader(trimmed)
		if h != nil {
			return h.Mode
		}
	}

	// Check for patch header
	if strings.HasPrefix(trimmed, "@patch") {
		return ModePatch
	}

	// Check for tabular
	if strings.HasPrefix(trimmed, "@tab") {
		return ModeTabular
	}

	// Check for packed syntax: Type@(...)
	if strings.Contains(trimmed, "@(") || strings.Contains(trimmed, "@{bm=") {
		return ModePacked
	}

	// Default to struct mode
	return ModeStruct
}

// ============================================================
// V2 Encoder Options
// ============================================================

// V2Options configures LYPH v2 encoding.
type V2Options struct {
	Schema        *Schema
	Mode          Mode    // Preferred mode (ModeAuto for automatic)
	KeyMode       KeyMode // Key format
	TabThreshold  int     // Minimum list length for tabular (default 3)
	IncludeHeader bool    // Whether to include @lyph header
	UseBitmap     bool    // Use bitmap for sparse optionals
	IndentPrefix  string  // Indentation for nested content
}

// DefaultV2Options returns sensible defaults for LYPH v2 encoding.
func DefaultV2Options(schema *Schema) V2Options {
	return V2Options{
		Schema:        schema,
		Mode:          ModeAuto,
		KeyMode:       KeyModeWire,
		TabThreshold:  3,
		IncludeHeader: true,
		UseBitmap:     true,
		IndentPrefix:  "",
	}
}

// ============================================================
// Top-level V2 Encoder
// ============================================================

// EmitV2 encodes a value in LYPH v2 format with automatic mode selection.
func EmitV2(v *GValue, opts V2Options) (string, error) {
	if v == nil {
		return canonNull(), nil
	}

	// Select mode
	mode := opts.Mode
	if mode == ModeAuto {
		mode = SelectMode(v, opts.Schema, opts.TabThreshold)
	}

	var body string
	var err error

	switch mode {
	case ModePacked:
		packOpts := PackedOptions{
			Schema:    opts.Schema,
			UseBitmap: opts.UseBitmap,
			KeyMode:   opts.KeyMode,
		}
		body, err = EmitPackedWithOptions(v, packOpts)

	case ModeTabular:
		tabOpts := TabularOptions{
			Schema:       opts.Schema,
			Threshold:    opts.TabThreshold,
			KeyMode:      opts.KeyMode,
			UseBitmap:    opts.UseBitmap,
			IndentPrefix: opts.IndentPrefix,
		}
		body, err = EmitTabularWithOptions(v, tabOpts)

	default:
		// Struct mode (v1 compatible)
		body = Emit(v)
	}

	if err != nil {
		return "", err
	}

	// Add header if requested
	if opts.IncludeHeader && opts.Schema != nil {
		header := &Header{
			Version:  "v2",
			SchemaID: opts.Schema.Hash,
			Mode:     mode,
			KeyMode:  opts.KeyMode,
		}
		return EmitHeader(header) + "\n" + body, nil
	}

	return body, nil
}

// EmitV2Patch encodes a patch in LYPH v2 format.
func EmitV2Patch(p *Patch, opts V2Options) (string, error) {
	patchOpts := PatchOptions{
		Schema:       opts.Schema,
		KeyMode:      opts.KeyMode,
		SortOps:      true,
		IndentPrefix: opts.IndentPrefix,
	}
	return EmitPatchWithOptions(p, patchOpts)
}
