// Generate golden test files for cross-language parity testing
package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/Neumenon/cowrie"
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
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"null_val":       cowrie.Null(),
		"bool_true":      cowrie.Bool(true),
		"bool_false":     cowrie.Bool(false),
		"int_positive":   cowrie.Int64(42),
		"int_negative":   cowrie.Int64(-42),
		"string_val":     cowrie.String("hello, world!"),
		"string_unicode": cowrie.String("你好世界 🌍"),
	})

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "primitives.cowrie", data)
}

func generateNested(outDir string) {
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"user": cowrie.ObjectFromMap(map[string]*cowrie.Value{
			"name": cowrie.String("Alice"),
			"age":  cowrie.Int64(30),
			"emails": cowrie.Array(
				cowrie.String("alice@example.com"),
				cowrie.String("alice.work@company.com"),
			),
		}),
	})

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "nested.cowrie", data)
}

func generateEmpty(outDir string) {
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"empty_array":  cowrie.Array(),
		"empty_object": cowrie.Object(),
		"empty_string": cowrie.String(""),
	})

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "empty.cowrie", data)
}

func generateIntegers(outDir string) {
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"zero":      cowrie.Int64(0),
		"one":       cowrie.Int64(1),
		"minus_one": cowrie.Int64(-1),
		"int_min":   cowrie.Int64(math.MinInt64),
		"int_max":   cowrie.Int64(math.MaxInt64),
		"uint_max":  cowrie.Uint64(math.MaxUint64),
	})

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "integers.cowrie", data)
}

func generateFloats(outDir string) {
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"zero": cowrie.Float64(0.0),
		"pi":   cowrie.Float64(math.Pi),
		"e":    cowrie.Float64(math.E),
	})

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "floats.cowrie", data)
}

func generateMixedArray(outDir string) {
	val := cowrie.Array(
		cowrie.Null(),
		cowrie.Bool(true),
		cowrie.Int64(42),
		cowrie.Float64(3.14),
		cowrie.String("hello"),
		cowrie.Array(cowrie.Int64(1), cowrie.Int64(2)),
		cowrie.ObjectFromMap(map[string]*cowrie.Value{"key": cowrie.String("value")}),
	)

	data, err := cowrie.Encode(val)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "mixed_array.cowrie", data)
}

func generateDeterministic(outDir string) {
	val := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"zebra": cowrie.Int64(1),
		"apple": cowrie.Int64(2),
		"mango": cowrie.Int64(3),
		"banana": cowrie.ObjectFromMap(map[string]*cowrie.Value{
			"z_inner": cowrie.String("z"),
			"a_inner": cowrie.String("a"),
		}),
	})

	data, err := cowrie.EncodeWithOptions(val, cowrie.EncodeOptions{Deterministic: true})
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "deterministic.cowrie", data)

	// Generate fingerprint
	fp := cowrie.SchemaFingerprint32(val)
	writeFingerprint(outDir, "deterministic.cowrie", fp)
}

func generateSchemas(outDir string) {
	// schema1 and schema2 have the same structure but different values
	schema1 := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"name":  cowrie.String("Alice"),
		"age":   cowrie.Int64(30),
		"score": cowrie.Float64(95.5),
	})

	schema2 := cowrie.ObjectFromMap(map[string]*cowrie.Value{
		"name":  cowrie.String("Bob"),
		"age":   cowrie.Int64(25),
		"score": cowrie.Float64(88.0),
	})

	data1, err := cowrie.Encode(schema1)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "schema1.cowrie", data1)

	data2, err := cowrie.Encode(schema2)
	if err != nil {
		panic(err)
	}
	writeFile(outDir, "schema2.cowrie", data2)

	// Generate fingerprints
	fp1 := cowrie.SchemaFingerprint32(schema1)
	fp2 := cowrie.SchemaFingerprint32(schema2)

	writeFingerprint(outDir, "schema1.cowrie", fp1)
	writeFingerprint(outDir, "schema2.cowrie", fp2)

	if fp1 != fp2 {
		fmt.Printf("WARNING: schema1 and schema2 have different fingerprints! (0x%08x vs 0x%08x)\n", fp1, fp2)
	}
}
