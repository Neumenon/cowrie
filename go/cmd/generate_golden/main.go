// Generate golden test files for cross-language parity testing
package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/phenomenon0/cowrie-final/go/gen2"
)

func main() {
	outDir := filepath.Join("..", "..", "testdata", "gen2")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		panic(err)
	}

	// Generate primitives.cowrie
	generatePrimitives(outDir)

	// Generate nested.cowrie
	generateNested(outDir)

	// Generate empty.cowrie
	generateEmpty(outDir)

	// Generate integers.cowrie
	generateIntegers(outDir)

	// Generate floats.cowrie
	generateFloats(outDir)

	// Generate mixed_array.cowrie
	generateMixedArray(outDir)

	// Generate deterministic.cowrie
	generateDeterministic(outDir)

	// Generate schema1.cowrie and schema2.cowrie
	generateSchemas(outDir)

	fmt.Println("All golden files generated successfully!")
}

func writeFile(outDir, name string, data []byte) {
	path := filepath.Join(outDir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		panic(fmt.Sprintf("failed to write %s: %v", name, err))
	}
	fmt.Printf("Generated %s (%d bytes)\n", name, len(data))
}

func writeFingerprint(outDir, name string, fp uint32) {
	path := filepath.Join(outDir, name+".fingerprint")
	content := fmt.Sprintf("%08x\n", fp)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		panic(fmt.Sprintf("failed to write %s.fingerprint: %v", name, err))
	}
	fmt.Printf("Generated %s.fingerprint (0x%08x)\n", name, fp)
}

func generatePrimitives(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"null_val":       gen2.Null(),
		"bool_true":      gen2.Bool(true),
		"bool_false":     gen2.Bool(false),
		"int_positive":   gen2.Int64(42),
		"int_negative":   gen2.Int64(-42),
		"string_val":     gen2.String("hello, world!"),
		"string_unicode": gen2.String("你好世界 🌍"),
	})

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "primitives.cowrie", data)
}

func generateNested(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"user": gen2.ObjectFromMap(map[string]*gen2.Value{
			"name": gen2.String("Alice"),
			"age":  gen2.Int64(30),
			"emails": gen2.Array(
				gen2.String("alice@example.com"),
				gen2.String("alice.work@company.com"),
			),
		}),
	})

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "nested.cowrie", data)
}

func generateEmpty(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"empty_array":  gen2.Array(),
		"empty_object": gen2.Object(),
		"empty_string": gen2.String(""),
	})

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "empty.cowrie", data)
}

func generateIntegers(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"zero":      gen2.Int64(0),
		"one":       gen2.Int64(1),
		"minus_one": gen2.Int64(-1),
		"int_min":   gen2.Int64(math.MinInt64),
		"int_max":   gen2.Int64(math.MaxInt64),
		"uint_max":  gen2.Uint64(math.MaxUint64),
	})

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "integers.cowrie", data)
}

func generateFloats(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"zero": gen2.Float64(0.0),
		"pi":   gen2.Float64(math.Pi),
		"e":    gen2.Float64(math.E),
	})

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "floats.cowrie", data)
}

func generateMixedArray(outDir string) {
	val := gen2.Array(
		gen2.Null(),
		gen2.Bool(true),
		gen2.Int64(42),
		gen2.Float64(3.14),
		gen2.String("hello"),
		gen2.Array(gen2.Int64(1), gen2.Int64(2)),
		gen2.ObjectFromMap(map[string]*gen2.Value{"key": gen2.String("value")}),
	)

	data, err := gen2.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "mixed_array.cowrie", data)
}

func generateDeterministic(outDir string) {
	val := gen2.ObjectFromMap(map[string]*gen2.Value{
		"zebra": gen2.Int64(1),
		"apple": gen2.Int64(2),
		"mango": gen2.Int64(3),
		"banana": gen2.ObjectFromMap(map[string]*gen2.Value{
			"z_inner": gen2.String("z"),
			"a_inner": gen2.String("a"),
		}),
	})

	data, err := gen2.EncodeWithOptions(val, gen2.EncodeOptions{Deterministic: true})
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "deterministic.cowrie", data)

	// Generate fingerprint
	fp := gen2.SchemaFingerprint32(val)
	writeFingerprint(outDir, "deterministic.cowrie", fp)
}

func generateSchemas(outDir string) {
	// schema1 and schema2 have the same structure but different values
	schema1 := gen2.ObjectFromMap(map[string]*gen2.Value{
		"name":  gen2.String("Alice"),
		"age":   gen2.Int64(30),
		"score": gen2.Float64(95.5),
	})

	schema2 := gen2.ObjectFromMap(map[string]*gen2.Value{
		"name":  gen2.String("Bob"),
		"age":   gen2.Int64(25),
		"score": gen2.Float64(88.0),
	})

	data1, err := gen2.Encode(schema1)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "schema1.cowrie", data1)

	data2, err := gen2.Encode(schema2)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "schema2.cowrie", data2)

	// Generate fingerprints
	fp1 := gen2.SchemaFingerprint32(schema1)
	fp2 := gen2.SchemaFingerprint32(schema2)

	writeFingerprint(outDir, "schema1.cowrie", fp1)
	writeFingerprint(outDir, "schema2.cowrie", fp2)

	if fp1 != fp2 {
		fmt.Printf("WARNING: schema1 and schema2 have different fingerprints! (0x%08x vs 0x%08x)\n", fp1, fp2)
	}
}
