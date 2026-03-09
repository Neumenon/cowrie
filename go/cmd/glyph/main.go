// glyph - GLYPH codec CLI tool
//
// Usage:
//
//	glyph fmt-loose [--no-tabular] [file]  Format JSON as canonical GLYPH-Loose
//	glyph to-json [file]                   Convert GLYPH-Loose canonical to JSON
//	glyph from-json [file]                 Parse JSON to GLYPH-Loose canonical
//	glyph stream decode [file]             Decode GS1-T frames and print
//	glyph stream demo                      Run the Agent Cockpit streaming demo
//	glyph version                          Print version info
//
// Smart auto-tabular is ON by default: lists of 3+ objects become @tab blocks.
// Use --no-tabular to disable.
//
// If no file is given, reads from stdin.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Neumenon/cowrie/go/glyph"
	"github.com/Neumenon/cowrie/go/glyph/stream"
)

const (
	libVersion  = "0.4.0"
	specVersion = "2.4.0-gs1"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	var input io.Reader = os.Stdin

	// Handle stream subcommands
	if cmd == "stream" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "glyph stream: missing subcommand (decode, demo)")
			os.Exit(1)
		}
		subcmd := os.Args[2]
		switch subcmd {
		case "decode":
			if len(os.Args) > 3 && os.Args[3] != "-" {
				f, err := os.Open(os.Args[3])
				if err != nil {
					fatal("open file: %v", err)
				}
				defer f.Close()
				input = f
			}
			cmdStreamDecode(input)
		case "demo":
			cmdStreamDemo()
		default:
			fmt.Fprintf(os.Stderr, "glyph stream: unknown subcommand: %s\n", subcmd)
			os.Exit(1)
		}
		return
	}

	// Parse flags and file argument for non-stream commands
	noTabular := false
	llmMode := false
	compactMode := false
	autoPool := false
	minOccurs := 2
	minLength := 20
	fileArg := ""
	for _, arg := range os.Args[2:] {
		switch {
		case arg == "--no-tabular":
			noTabular = true
		case arg == "--llm":
			llmMode = true
		case arg == "--compact":
			compactMode = true
		case arg == "--auto-pool":
			autoPool = true
		case arg == "--auto-tabular":
			// For backward compat (tabular is already default)
		case strings.HasPrefix(arg, "--min-occurs="):
			if n, err := parseIntArg(arg, "--min-occurs="); err == nil {
				minOccurs = n
			}
		case strings.HasPrefix(arg, "--min-length="):
			if n, err := parseIntArg(arg, "--min-length="); err == nil {
				minLength = n
			}
		default:
			if !strings.HasPrefix(arg, "-") && arg != "-" {
				fileArg = arg
			}
		}
	}

	// If a file argument is provided, use it
	if fileArg != "" {
		f, err := os.Open(fileArg)
		if err != nil {
			fatal("open file: %v", err)
		}
		defer f.Close()
		input = f
	}

	switch cmd {
	case "fmt-loose", "fmt":
		if autoPool {
			cmdFmtLooseWithPool(input, llmMode, minOccurs, minLength)
		} else {
			cmdFmtLoose(input, noTabular, llmMode, compactMode)
		}
	case "to-json":
		cmdToJSON(input)
	case "from-json":
		cmdFromJSON(input)
	case "version", "-v", "--version":
		fmt.Printf("glyph %s (spec %s)\n", libVersion, specVersion)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `glyph - GLYPH codec CLI tool (v2.4.0)

Usage:
  glyph fmt-loose [options] [file]       Format JSON as canonical GLYPH-Loose
  glyph to-json [file]                   Convert GLYPH canonical to JSON  
  glyph from-json [file]                 Parse JSON to GLYPH-Loose canonical
  glyph stream decode [file]             Decode GS1-T frames and print
  glyph stream demo                      Run the Agent Cockpit streaming demo
  glyph version                          Print version info

Options:
  --no-tabular        Disable auto-tabular (it's ON by default for 35-65% token savings)
  --llm               Use LLM-friendly mode (ASCII _ for null)
  --compact           Use schema header + compact keys (#0, #1, etc.) for max compression
  --auto-pool         Enable automatic string pooling for deduplication
  --min-occurs=N      Minimum occurrences to pool a string (default: 2)
  --min-length=N      Minimum string length to consider for pooling (default: 20)

Smart auto-tabular: lists of 3+ homogeneous objects become compact @tab blocks.
Non-eligible data (primitives, mixed lists, <3 items) uses standard format.

If no file is given, reads from stdin.

Examples:
  echo '{"b":1,"a":2}' | glyph fmt-loose
  # Output: {a=2 b=1}

  # Lists of 3+ objects auto-tabularize (DEFAULT)
  echo '[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"}]' | glyph fmt-loose
  # Output:
  # @tab _ [id name]
  # |1|a|
  # |2|b|
  # |3|c|
  # @end

  # LLM mode (uses _ for null)
  echo '{"a":null,"b":42}' | glyph fmt-loose --llm
  # Output: {a=_ b=42}

  # Compact mode with schema header
  echo '{"action":"search","query":"test"}' | glyph fmt-loose --compact
  # Output:
  # @schema#<hash> keys=[action query]
  # {#0=search #1=test}

  # Disable tabular for v2.2.x compatible output
  echo '[{"id":1},{"id":2},{"id":3}]' | glyph fmt-loose --no-tabular
  # Output: [{id=1} {id=2} {id=3}]

  cat data.json | glyph fmt-loose > data.glyph
  glyph to-json data.glyph > data.json
`)
}

// cmdFmtLoose: JSON -> canonical GLYPH-Loose
func cmdFmtLoose(r io.Reader, noTabular, llmMode, compactMode bool) {
	data, err := io.ReadAll(r)
	if err != nil {
		fatal("read input: %v", err)
	}

	gv, err := glyph.FromJSONLoose(data)
	if err != nil {
		fatal("parse JSON: %v", err)
	}

	var opts glyph.LooseCanonOpts
	if llmMode {
		opts = glyph.LLMLooseCanonOpts()
	} else {
		opts = glyph.DefaultLooseCanonOpts()
	}

	if noTabular {
		opts.AutoTabular = false
	}

	var canonical string
	if compactMode {
		// Build key dictionary and emit with schema header + compact keys
		keyDict := glyph.BuildKeyDictFromValue(gv)
		hash := stream.StateHashLoose(gv)
		schemaRef := stream.HashToHex(hash)[:16] // Use first 16 chars of hash
		opts.SchemaRef = schemaRef
		opts.KeyDict = keyDict
		opts.UseCompactKeys = true
		canonical = glyph.CanonicalizeLooseWithSchema(gv, opts)
	} else {
		canonical = glyph.CanonicalizeLooseWithOpts(gv, opts)
	}
	fmt.Println(canonical)
}

// cmdToJSON: GLYPH-Loose canonical -> JSON
// Parses GLYPH canonical form (with optional @schema, @pool, @tab directives) and outputs JSON.
func cmdToJSON(r io.Reader) {
	data, err := io.ReadAll(r)
	if err != nil {
		fatal("read input: %v", err)
	}

	input := strings.TrimSpace(string(data))

	// Parse using the library's high-level ParseDocument API
	gv, err := glyph.ParseDocument(input)
	if err != nil {
		// Fallback: try as JSON
		gv, err = glyph.FromJSONLoose(data)
		if err != nil {
			fatal("parse input (neither GLYPH nor JSON): %v", err)
		}
	}

	jsonData, err := glyph.ToJSONLoose(gv)
	if err != nil {
		fatal("convert to JSON: %v", err)
	}

	// Pretty-print JSON
	var pretty interface{}
	json.Unmarshal(jsonData, &pretty)
	out, _ := json.MarshalIndent(pretty, "", "  ")
	fmt.Println(string(out))
}

// cmdFromJSON: JSON -> GLYPH-Loose canonical (same as fmt-loose)
func cmdFromJSON(r io.Reader) {
	cmdFmtLoose(r, false, false, false)
}

// cmdStreamDecode: Decode GS1-T frames and print them
func cmdStreamDecode(r io.Reader) {
	reader := stream.NewReader(r)
	frameNum := 0

	for {
		frame, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "frame %d: error: %v\n", frameNum, err)
			continue
		}

		frameNum++
		printFrame(frameNum, frame)
	}

	fmt.Fprintf(os.Stderr, "\n--- %d frames decoded ---\n", frameNum)
}

func printFrame(n int, f *stream.Frame) {
	fmt.Printf("--- Frame %d ---\n", n)
	fmt.Printf("  sid=%d seq=%d kind=%s len=%d\n", f.SID, f.Seq, f.Kind, len(f.Payload))

	if f.CRC != nil {
		fmt.Printf("  crc=%08x\n", *f.CRC)
	}
	if f.Base != nil {
		fmt.Printf("  base=%s\n", stream.HashToHex(*f.Base))
	}
	if f.Final {
		fmt.Printf("  final=true\n")
	}

	// Print payload (truncated if long)
	payload := string(f.Payload)
	if len(payload) > 200 {
		payload = payload[:200] + "..."
	}
	if len(payload) > 0 {
		fmt.Printf("  payload: %s\n", payload)
	}
}

// cmdStreamDemo: Run the Agent Cockpit streaming demo
func cmdStreamDemo() {
	w := stream.NewWriterWithCRC(os.Stdout)
	bw := bufio.NewWriter(os.Stdout)

	sid := uint64(1)
	seq := uint64(0)

	// Helper to write and flush
	writeFrame := func(f *stream.Frame) {
		if err := w.WriteFrame(f); err != nil {
			fatal("write frame: %v", err)
		}
		bw.Flush()
		os.Stdout.Sync()
	}

	// 1. Initial doc snapshot
	initialState := glyph.Struct("AgentState",
		glyph.MapEntry{Key: "task", Value: glyph.Str("process_data")},
		glyph.MapEntry{Key: "step", Value: glyph.Int(0)},
		glyph.MapEntry{Key: "total_steps", Value: glyph.Int(10)},
		glyph.MapEntry{Key: "items", Value: glyph.List()},
	)

	currentState := initialState
	stateHash := stream.StateHashLoose(currentState)

	seq++
	writeFrame(&stream.Frame{
		Version: stream.Version,
		SID:     sid,
		Seq:     seq,
		Kind:    stream.KindDoc,
		Payload: []byte(glyph.Emit(currentState)),
	})

	fmt.Fprintln(os.Stderr, "[demo] Sent initial state")
	time.Sleep(500 * time.Millisecond)

	// 2. Progress through steps with UI events and patches
	for step := 1; step <= 10; step++ {
		// UI: Progress event
		seq++
		writeFrame(&stream.Frame{
			Version: stream.Version,
			SID:     sid,
			Seq:     seq,
			Kind:    stream.KindUI,
			Payload: stream.EmitProgress(float64(step)/10.0, fmt.Sprintf("Processing step %d of 10", step)),
		})

		time.Sleep(200 * time.Millisecond)

		// UI: Log event
		seq++
		writeFrame(&stream.Frame{
			Version: stream.Version,
			SID:     sid,
			Seq:     seq,
			Kind:    stream.KindUI,
			Payload: stream.EmitLog("info", fmt.Sprintf("Step %d: generated item_%d", step, step)),
		})

		// Patch: Update state
		patchPayload := fmt.Sprintf(`@patch
= .step %d
+ .items {id=%d name="item_%d"}
@end`, step, step, step)

		seq++
		writeFrame(&stream.Frame{
			Version: stream.Version,
			SID:     sid,
			Seq:     seq,
			Kind:    stream.KindPatch,
			Payload: []byte(patchPayload),
			Base:    &stateHash,
		})

		// Apply the patch to update state (real parse + apply, not simulated)
		parsed, err := glyph.ParsePatch(patchPayload, nil)
		if err != nil {
			fatal("parse patch: %v", err)
		}
		currentState, err = glyph.ApplyPatch(currentState, parsed)
		if err != nil {
			fatal("apply patch: %v", err)
		}
		stateHash = stream.StateHashLoose(currentState)

		// UI: Metric every 3 steps
		if step%3 == 0 {
			seq++
			writeFrame(&stream.Frame{
				Version: stream.Version,
				SID:     sid,
				Seq:     seq,
				Kind:    stream.KindUI,
				Payload: stream.EmitMetric("items_processed", float64(step), "count"),
			})
		}

		time.Sleep(300 * time.Millisecond)
	}

	// 3. Final: Artifact reference
	seq++
	writeFrame(&stream.Frame{
		Version: stream.Version,
		SID:     sid,
		Seq:     seq,
		Kind:    stream.KindUI,
		Payload: stream.EmitArtifact("application/json", "blob:sha256:abc123...", "results.json"),
	})

	// 4. Completion log
	seq++
	writeFrame(&stream.Frame{
		Version: stream.Version,
		SID:     sid,
		Seq:     seq,
		Kind:    stream.KindUI,
		Payload: stream.EmitLog("info", "Task completed successfully"),
	})

	// 5. Final doc snapshot with final flag
	finalState := glyph.Struct("AgentState",
		glyph.MapEntry{Key: "task", Value: glyph.Str("process_data")},
		glyph.MapEntry{Key: "step", Value: glyph.Int(10)},
		glyph.MapEntry{Key: "total_steps", Value: glyph.Int(10)},
		glyph.MapEntry{Key: "status", Value: glyph.Str("completed")},
	)

	seq++
	writeFrame(&stream.Frame{
		Version: stream.Version,
		SID:     sid,
		Seq:     seq,
		Kind:    stream.KindDoc,
		Payload: []byte(glyph.Emit(finalState)),
		Final:   true,
	})

	fmt.Fprintln(os.Stderr, "[demo] Stream complete")
	fmt.Fprintf(os.Stderr, "[demo] Sent %d frames\n", seq)
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "glyph: "+format+"\n", args...)
	os.Exit(1)
}

// parseIntArg extracts an integer from a flag like "--min-occurs=2"
func parseIntArg(arg, prefix string) (int, error) {
	val := strings.TrimPrefix(arg, prefix)
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// cmdFmtLooseWithPool: JSON -> canonical GLYPH-Loose with automatic string pooling
func cmdFmtLooseWithPool(r io.Reader, llmMode bool, minOccurs, minLength int) {
	data, err := io.ReadAll(r)
	if err != nil {
		fatal("read input: %v", err)
	}

	opts := glyph.DefaultAutoPoolOpts()
	opts.MinOccurs = minOccurs
	opts.MinLength = minLength
	if llmMode {
		opts.LooseOpts = glyph.LLMLooseCanonOpts()
	}

	result, err := glyph.AutoPoolEncodeJSON(data, opts)
	if err != nil {
		fatal("encode with pool: %v", err)
	}

	// Print output
	fmt.Print(result.Output)

	// Print stats to stderr
	if result.Stats.PoolEntries > 0 {
		fmt.Fprintf(os.Stderr, "\n--- Pool Stats ---\n")
		fmt.Fprintf(os.Stderr, "Strings pooled: %d\n", result.Stats.PooledStrings)
		fmt.Fprintf(os.Stderr, "Pool entries: %d\n", result.Stats.PoolEntries)
		fmt.Fprintf(os.Stderr, "Original size: %d bytes\n", result.Stats.OriginalBytes)
		fmt.Fprintf(os.Stderr, "Pooled size: %d bytes\n", result.Stats.PooledBytes)
		fmt.Fprintf(os.Stderr, "Savings: %d bytes (%.1f%%)\n", result.Stats.BytesSaved, result.Stats.SavingsPercent)
	}
}
