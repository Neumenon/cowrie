// Command cowrie provides a CLI tool for encoding/decoding Cowrie data.
//
// Usage:
//
//	cowrie encode [--gen1|--gen2] [--compress=none|gzip|zstd] < input.json > output.cowrie
//	cowrie decode [--gen1|--gen2] < input.cowrie > output.json
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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Neumenon/cowrie/gen1"
	cowrie "github.com/Neumenon/cowrie"
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
  cowrie encode [--gen1|--gen2] [--compress=none|gzip|zstd] < input.json > output.cowrie
  cowrie decode [--gen1|--gen2] < input.cowrie > output.json
  cowrie info < input.cowrie

Commands:
  encode    Encode JSON to Cowrie binary format
  decode    Decode Cowrie binary to JSON
  info      Display information about an Cowrie file

Flags:
  --gen1        Use Gen1 codec (lightweight, stdlib only)
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
	useGen1 := fs.Bool("gen1", false, "Use Gen1 codec")
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

	if *useGen1 {
		// Gen1 encoding
		var data any
		if err := json.Unmarshal(input, &data); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
			os.Exit(1)
		}
		output, err = gen1.Encode(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Gen2 encoding (default)
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
	}

	os.Stdout.Write(output)
}

func decodeCmd(args []string) {
	fs := flag.NewFlagSet("decode", flag.ExitOnError)
	useGen1 := fs.Bool("gen1", false, "Use Gen1 codec")
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

	if *useGen1 {
		// Gen1 decoding
		data, err := gen1.Decode(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding: %v\n", err)
			os.Exit(1)
		}
		if *pretty {
			output, err = json.MarshalIndent(data, "", "  ")
		} else {
			output, err = json.Marshal(data)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Gen2 decoding (default)
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
	}

	os.Stdout.Write(output)
	os.Stdout.Write([]byte("\n"))
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
	} else if input[0] == 0x00 || input[0] <= 0x15 {
		// Likely Gen1 format (starts with type tag)
		fmt.Println("Format: Cowrie Gen1 (likely)")
		fmt.Printf("Size: %d bytes\n", len(input))

		// Try to decode
		data, err := gen1.Decode(input)
		if err == nil {
			fmt.Printf("Root type: %T\n", data)
		}
	} else {
		fmt.Println("Format: Unknown")
		fmt.Printf("Size: %d bytes\n", len(input))
		fmt.Printf("First bytes: %02x %02x %02x %02x\n", input[0], input[1], input[2], input[3])
	}
}
