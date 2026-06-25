// Command cowrie provides a CLI tool for encoding/decoding Cowrie data.
//
// Usage:
//
//	cowrie encode [--gen2] [--compress=none|gzip|zstd] < input.json > output.cowrie
//	cowrie decode [--gen2] < input.cowrie > output.json
//	cowrie info < input.cowrie
//
// Examples:
//
//	# Encode JSON to Cowrie Gen2
//	echo '{"name":"Alice","age":30}' | cowrie encode --gen2 > data.cowrie
//
//	# Decode Cowrie to JSON
//	cowrie decode < data.cowrie
//
//	# Encode with compression
//	cat large.json | cowrie encode --gen2 --compress=zstd > data.cowrie.zst
//
//	# Get info about Cowrie file
//	cowrie info < data.cowrie
package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	cowrie "github.com/Neumenon/cowrie/go"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "encode":
		encodeCmd(os.Args[2:])
	case "decode":
		decodeCmd(os.Args[2:])
	case "recode":
		recodeCmd(os.Args[2:])
	case "info":
		infoCmd(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`cowrie - Binary JSON codec CLI

Usage:
  cowrie encode [--gen2] [--compress=none|gzip|zstd] < input.json > output.cowrie
  cowrie decode [--gen2] < input.cowrie > output.json
  cowrie info < input.cowrie

Commands:
  encode    Encode JSON to Cowrie binary format
  decode    Decode Cowrie binary to JSON
  info      Display information about an Cowrie file

Flags:
  --gen2        Use Gen2 codec (full Cowrie v2 with ML extensions) [default]
  --compress    Compression: none, gzip, zstd (Gen2 only) [default: none]
  --pretty      Pretty-print JSON output (decode only)

Examples:
  echo '{"name":"Alice"}' | cowrie encode --gen2 > data.cowrie
  cowrie decode < data.cowrie
  cat data.json | cowrie encode --gen2 --compress=zstd | cowrie decode`)
}

func encodeCmd(args []string) {
	fs := flag.NewFlagSet("encode", flag.ExitOnError)
	useGen2 := fs.Bool("gen2", false, "Use Gen2 codec (default)")
	compress := fs.String("compress", "none", "Compression: none, gzip, zstd")
	fs.Parse(args)

	// Read input
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	var output []byte

	// Gen2 encoding (the only v1 codec; --gen2 is accepted for compatibility)
	_ = useGen2 // Silence unused warning
	val, err := cowrie.FromJSON(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// Apply compression if requested
	switch *compress {
	case "none":
		output, err = cowrie.Encode(val)
	case "gzip":
		output, err = cowrie.EncodeFramed(val, cowrie.CompressionGzip)
	case "zstd":
		output, err = cowrie.EncodeFramed(val, cowrie.CompressionZstd)
	default:
		fmt.Fprintf(os.Stderr, "Unknown compression: %s\n", *compress)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding: %v\n", err)
		os.Exit(1)
	}

	os.Stdout.Write(output)
}

func decodeCmd(args []string) {
	fs := flag.NewFlagSet("decode", flag.ExitOnError)
	useGen2 := fs.Bool("gen2", false, "Use Gen2 codec (default)")
	pretty := fs.Bool("pretty", false, "Pretty-print JSON output")
	fs.Parse(args)

	// Read input
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	var output []byte

	// Gen2 decoding (the only v1 codec; --gen2 is accepted for compatibility)
	_ = useGen2 // Silence unused warning

	// Try framed decode first (handles compression)
	val, err := cowrie.DecodeFramed(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding: %v\n", err)
		os.Exit(1)
	}

	if *pretty {
		output, err = cowrie.ToJSONIndent(val, "  ")
	} else {
		output, err = cowrie.ToJSON(val)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	os.Stdout.Write(output)
	os.Stdout.Write([]byte("\n"))
}

// specErrorCode maps an internal decode error to its canonical SPEC-v1 §2.6
// ERR_* code. The cross-language conformance gate extracts the first
// /ERR_[A-Z_]+/ token from stderr, so 'recode' prints "<CODE>: <detail>".
// Mirrors tools/cowrie_ref/errors.py + decode.py (the oracle for which input
// maps to which code).
func specErrorCode(err error) string {
	switch {
	case errors.Is(err, cowrie.ErrInvalidMagic):
		return "ERR_INVALID_MAGIC"
	case errors.Is(err, cowrie.ErrInvalidVersion):
		return "ERR_INVALID_VERSION"
	case errors.Is(err, cowrie.ErrNonCanonical):
		return "ERR_NON_CANONICAL"
	case errors.Is(err, cowrie.ErrTrailingData):
		return "ERR_TRAILING_DATA"
	case errors.Is(err, cowrie.ErrInvalidVarint):
		return "ERR_INVALID_VARINT"
	case errors.Is(err, cowrie.ErrInvalidFieldID):
		// dict index out of range / non-ascending field id (§2.6)
		return "ERR_INVALID_FIELD_ID"
	case errors.Is(err, cowrie.ErrDuplicateKey):
		return "ERR_DUPLICATE_KEY"
	case errors.Is(err, cowrie.ErrTooLarge):
		// §2.7 size limit (e.g. tensor rank > MAX_RANK)
		return "ERR_TOO_LARGE"
	case errors.Is(err, cowrie.ErrDepthExceeded), errors.Is(err, cowrie.ErrTooDeep):
		// §2.7 nesting depth > MAX_DEPTH
		return "ERR_TOO_DEEP"
	case errors.Is(err, cowrie.ErrUnexpectedEOF), errors.Is(err, cowrie.ErrMalformedLength):
		// truncation / premature EOF / declared length past end of buffer
		return "ERR_TRUNCATED"
	}
	// Reserved or unknown type tag (§2.3): the decoder returns a *TagError for
	// any tag it does not handle (including reserved tag 0x0F and any value
	// outside the defined ranges). The oracle classifies these as RESERVED_TAG.
	var te *cowrie.TagError
	if errors.As(err, &te) {
		return "ERR_RESERVED_TAG"
	}
	switch {
	case errors.Is(err, cowrie.ErrInvalidTag), errors.Is(err, cowrie.ErrInvalidDType):
		return "ERR_RESERVED_TAG"
	}
	return ""
}

// failDecode prints the canonical ERR_* code (when known) followed by detail to
// stderr and exits non-zero. When no spec code maps, it prints the raw error so
// the failure is still visible (and non-canonical/rejection still fails).
func failDecode(err error) {
	if code := specErrorCode(err); code != "" {
		fmt.Fprintf(os.Stderr, "%s: %v\n", code, err)
	} else {
		fmt.Fprintf(os.Stderr, "Error decoding: %v\n", err)
	}
	os.Exit(1)
}

// recodeCmd decodes framed Gen2 from stdin and re-encodes RAW canonical bytes to stdout.
// Used by the cross-language identity gate: encode(decode(wire)) must be byte-identical
// across Go/Rust/Python/TS. JSON-projection-independent — tests identity, not display.
func recodeCmd(args []string) {
	fs := flag.NewFlagSet("recode", flag.ExitOnError)
	strict := fs.Bool("strict", false, "Strict-decode mode (§5.3): reject non-canonical input")
	addr := fs.Bool("addr", false, "Print the content-address (§3) multihash SHA-256 hex of the canonical bytes")
	fileID := fs.Bool("file-id", false, "Read a Cowrie FILE (§7) on stdin, decode+verify, print merkle_root as lowercase hex")
	fileRecode := fs.Bool("file-recode", false, "Read a Cowrie FILE (§7) on stdin, decode, re-encode, write raw bytes to stdout")
	tensorSpans := fs.Bool("tensor-spans", false, "Read a canonical Cowrie message on stdin, decode, print one line per tensor in document order: \"<data_offset> <data_len> <dtype> <shape>\"")
	fingerprint := fs.Bool("fingerprint", false, "Read a canonical Cowrie message on stdin, decode, print the §4 fingerprint64 as 16 lowercase hex chars")
	datasetRoot := fs.Bool("dataset-root", false, "Read uvarint(n) then n repetitions of (uvarint(len) || blob) on stdin; print the §7 merkle_root over that ORDERED blob list as 68 lowercase hex chars")
	fs.Parse(args)

	if *datasetRoot {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		// Dataset/stream layer (§7 reuse): the dataset_root is the SAME merkle_root
		// construction, with the ordered list of 34-byte shard file-roots as the
		// leaf blobs. Oracle: tools/cowrie_ref/profiles.py dataset_root.
		// stdin framing: uvarint(n) || n * (uvarint(len) || blob_bytes).
		n, w := binary.Uvarint(input)
		if w <= 0 {
			fmt.Fprintf(os.Stderr, "Error: bad uvarint count\n")
			os.Exit(1)
		}
		pos := w
		blobs := make([][]byte, 0, n)
		for i := uint64(0); i < n; i++ {
			l, lw := binary.Uvarint(input[pos:])
			if lw <= 0 {
				fmt.Fprintf(os.Stderr, "Error: bad uvarint length at blob %d\n", i)
				os.Exit(1)
			}
			pos += lw
			if uint64(pos)+l > uint64(len(input)) {
				fmt.Fprintf(os.Stderr, "Error: blob %d length past end of input\n", i)
				os.Exit(1)
			}
			blobs = append(blobs, input[pos:pos+int(l)])
			pos += int(l)
		}
		// Reuse the gated §7 merkle (cowrie.MerkleRoot) — do not reimplement.
		fmt.Println(hex.EncodeToString(cowrie.MerkleRoot(blobs)))
		return
	}

	if *fingerprint {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		val, err := cowrie.DecodeFramed(input)
		if err != nil {
			failDecode(err)
		}
		// §4: print fingerprint64 as 16 lowercase hex chars (zero-padded).
		fmt.Printf("%016x\n", cowrie.SchemaFingerprint64(val))
		return
	}

	if *tensorSpans {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		spans, err := cowrie.TensorSpans(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding: %v\n", err)
			os.Exit(1)
		}
		for _, s := range spans {
			shape := "-"
			if len(s.Shape) > 0 {
				parts := make([]string, len(s.Shape))
				for i, d := range s.Shape {
					parts[i] = strconv.FormatUint(d, 10)
				}
				shape = strings.Join(parts, ",")
			}
			fmt.Printf("%d %d %d %s\n", s.DataOffset, s.DataLen, uint8(s.DType), shape)
		}
		return
	}

	if *fileID || *fileRecode {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		if *fileID {
			id, err := cowrie.FileIdentityHex(input)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(id)
			return
		}
		// fileRecode
		out, err := cowrie.RecodeFile(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Stdout.Write(out)
		return
	}

	// STRICT=1 env also enables strict mode (parity with cross-language harness).
	if v := os.Getenv("STRICT"); v == "1" || v == "true" {
		*strict = true
	}

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
	var val *cowrie.Value
	if *strict {
		val, err = cowrie.DecodeFramedStrict(input)
	} else {
		val, err = cowrie.DecodeFramed(input)
	}
	if err != nil {
		failDecode(err)
	}
	output, err := cowrie.Encode(val)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding: %v\n", err)
		os.Exit(1)
	}
	if *addr {
		// Content address (§3): multihash SHA-256 of the canonical bytes.
		fmt.Println(cowrie.AddressOfBytesHex(output))
		return
	}
	os.Stdout.Write(output)
}

func infoCmd(args []string) {
	// Read input
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	if len(input) < 4 {
		fmt.Fprintf(os.Stderr, "Error: input too short\n")
		os.Exit(1)
	}

	// Check magic bytes
	magic := string(input[0:2])
	if magic == "SJ" {
		// Gen2 format
		version := input[2]
		flags := input[3]

		fmt.Printf("Format: Cowrie Gen2 (v%d)\n", version)
		fmt.Printf("Size: %d bytes\n", len(input))

		if flags&0x01 != 0 {
			compType := (flags >> 1) & 0x03
			switch compType {
			case 1:
				fmt.Println("Compression: gzip")
			case 2:
				fmt.Println("Compression: zstd")
			default:
				fmt.Println("Compression: unknown")
			}
		} else {
			fmt.Println("Compression: none")
		}

		// Try to decode and get schema info
		val, err := cowrie.DecodeFramed(input)
		if err == nil {
			fmt.Printf("Root type: %s\n", val.Type())
			fmt.Printf("Schema fingerprint: 0x%016x\n", cowrie.SchemaFingerprint64(val))
			fmt.Printf("Schema descriptor: %s\n", cowrie.SchemaDescriptor(val))
		}
	} else {
		fmt.Println("Format: Unknown")
		fmt.Printf("Size: %d bytes\n", len(input))
		fmt.Printf("First bytes: %02x %02x %02x %02x\n", input[0], input[1], input[2], input[3])
	}
}
