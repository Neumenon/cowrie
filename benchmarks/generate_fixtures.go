//go:build ignore

package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	cowrie "github.com/Neumenon/cowrie"
	"github.com/Neumenon/cowrie/gen1"
)

func main() {
	dir := filepath.Join("benchmarks", "fixtures")
	os.MkdirAll(dir, 0o755)

	// Small: 3-field object
	small := map[string]any{"name": "Alice", "age": int64(30), "score": 3.14159}
	smallV2 := cowrie.Object(
		cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
		cowrie.Member{Key: "age", Value: cowrie.Int64(30)},
		cowrie.Member{Key: "score", Value: cowrie.Float64(3.14159)},
	)

	// Medium: 20-field object with nested sub-object
	medFields := make(map[string]any)
	medMembers := make([]cowrie.Member, 0, 21)
	for i := 0; i < 20; i++ {
		k := string(rune('a' + i))
		medFields[k] = int64(i * 100)
		medMembers = append(medMembers, cowrie.Member{Key: k, Value: cowrie.Int64(int64(i * 100))})
	}
	medFields["nested"] = map[string]any{"x": 1.0, "y": 2.0, "z": 3.0}
	medMembers = append(medMembers, cowrie.Member{Key: "nested", Value: cowrie.Object(
		cowrie.Member{Key: "x", Value: cowrie.Float64(1.0)},
		cowrie.Member{Key: "y", Value: cowrie.Float64(2.0)},
		cowrie.Member{Key: "z", Value: cowrie.Float64(3.0)},
	)})
	mediumV2 := cowrie.Object(medMembers...)

	// Large: 1000 objects with repeated {id, name, value}
	largeArr := make([]any, 1000)
	largeV2Items := make([]*cowrie.Value, 1000)
	for i := 0; i < 1000; i++ {
		largeArr[i] = map[string]any{"id": int64(i), "name": "item", "value": float64(i) * 0.1}
		largeV2Items[i] = cowrie.Object(
			cowrie.Member{Key: "id", Value: cowrie.Int64(int64(i))},
			cowrie.Member{Key: "name", Value: cowrie.String("item")},
			cowrie.Member{Key: "value", Value: cowrie.Float64(float64(i) * 0.1)},
		)
	}
	largeV2 := cowrie.Array(largeV2Items...)

	// Floats: 10000 float64 values (as gen1 float64 array, gen2 tensor)
	floats := make([]float64, 10000)
	for i := range floats {
		floats[i] = float64(i) * 0.001
	}
	// Gen2 tensor
	floatData := make([]byte, 8*len(floats))
	for i, f := range floats {
		bits := math.Float64bits(f)
		floatData[i*8] = byte(bits)
		floatData[i*8+1] = byte(bits >> 8)
		floatData[i*8+2] = byte(bits >> 16)
		floatData[i*8+3] = byte(bits >> 24)
		floatData[i*8+4] = byte(bits >> 32)
		floatData[i*8+5] = byte(bits >> 40)
		floatData[i*8+6] = byte(bits >> 48)
		floatData[i*8+7] = byte(bits >> 56)
	}

	// Gen1 encode
	writeFixture(dir, "small.gen1", func() ([]byte, error) { return gen1.Encode(small) })
	writeFixture(dir, "medium.gen1", func() ([]byte, error) { return gen1.Encode(medFields) })
	writeFixture(dir, "large.gen1", func() ([]byte, error) { return gen1.Encode(largeArr) })
	writeFixture(dir, "floats.gen1", func() ([]byte, error) { return gen1.Encode(floats) })

	// Gen2 encode
	writeFixture(dir, "small.gen2", func() ([]byte, error) { return cowrie.Encode(smallV2) })
	writeFixture(dir, "medium.gen2", func() ([]byte, error) { return cowrie.Encode(mediumV2) })
	writeFixture(dir, "large.gen2", func() ([]byte, error) { return cowrie.Encode(largeV2) })
	// Gen2 floats - encode as float64 array via the any API
	writeFixture(dir, "floats.gen2", func() ([]byte, error) { return cowrie.EncodeAny(floats) })

	fmt.Println("All fixtures generated.")
}

func writeFixture(dir, name string, fn func() ([]byte, error)) {
	data, err := fn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR encoding %s: %v\n", name, err)
		os.Exit(1)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR writing %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Printf("  %s: %d bytes\n", name, len(data))
}
