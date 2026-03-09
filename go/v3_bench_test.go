package cowrie

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
)

// TestV3WireSizeSavings measures wire size improvements from v3 inline types.
func TestV3WireSizeSavings(t *testing.T) {
	fmt.Println("\n=== V3 Wire Size Impact ===")
	fmt.Printf("%-40s %8s %8s %8s\n", "Payload", "V2 (est)", "V3", "Savings")

	// 1. Small ints (0-127) → fixint saves 1-2 bytes per int
	// In v2: TagInt64(1) + zigzag varint(1+) = 2+ bytes
	// In v3: fixint = 1 byte
	testCases := []struct {
		name string
		val  *Value
	}{
		{"Single int 42", Int64(42)},
		{"Single int 0", Int64(0)},
		{"Single int 127", Int64(127)},
		{"Single int 128 (no fixint)", Int64(128)},
		{"Single int -1", Int64(-1)},
		{"Single int -16", Int64(-16)},
		{"Single int -17 (no fixneg)", Int64(-17)},
		{"Array [1,2,3]", Array(Int64(1), Int64(2), Int64(3))},
		{"Array of 15 small ints", func() *Value {
			items := make([]*Value, 15)
			for i := range items {
				items[i] = Int64(int64(i))
			}
			return Array(items...)
		}()},
		{"Array of 16 small ints (fallback)", func() *Value {
			items := make([]*Value, 16)
			for i := range items {
				items[i] = Int64(int64(i))
			}
			return Array(items...)
		}()},
		{"Object {a:1}", Object(Member{Key: "a", Value: Int64(1)})},
		{"Object 3 fields", Object(
			Member{Key: "x", Value: Int64(1)},
			Member{Key: "y", Value: Int64(2)},
			Member{Key: "z", Value: Int64(3)},
		)},
		{"1000 objects {id,name,val}", func() *Value {
			items := make([]*Value, 1000)
			for i := 0; i < 1000; i++ {
				items[i] = Object(
					Member{Key: "id", Value: Int64(int64(i))},
					Member{Key: "name", Value: String("item")},
					Member{Key: "value", Value: Float64(float64(i) * 0.1)},
				)
			}
			return Array(items...)
		}()},
		{"Telemetry 200 records", func() *Value {
			items := make([]*Value, 200)
			for i := 0; i < 200; i++ {
				items[i] = Object(
					Member{Key: "ts", Value: Datetime64(int64(i * 1000000))},
					Member{Key: "cpu", Value: Float64(float64(i%100) / 100.0)},
					Member{Key: "mem", Value: Float64(float64((i+50)%100) / 100.0)},
					Member{Key: "disk_read", Value: Int64(int64(i * 1024))},
					Member{Key: "disk_write", Value: Int64(int64(i * 512))},
					Member{Key: "net_in", Value: Int64(int64(i * 256))},
					Member{Key: "net_out", Value: Int64(int64(i * 128))},
				)
			}
			return Array(items...)
		}()},
		{"Bitmask 2048 bits", Bitmask(2048, func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = 0xAA // alternating pattern
			}
			return b
		}())},
		{"Bool array 2048 (alt to bitmask)", func() *Value {
			items := make([]*Value, 2048)
			for i := range items {
				items[i] = Bool(i%2 == 0)
			}
			return Array(items...)
		}()},
	}

	for _, tc := range testCases {
		encoded, err := Encode(tc.val)
		if err != nil {
			t.Fatalf("encode %s: %v", tc.name, err)
		}
		fmt.Printf("%-40s %8s %8d bytes\n", tc.name, "—", len(encoded))
	}

	// Specific comparison: bitmask vs bool array for ML masking
	fmt.Println("\n--- ML Masking Comparison ---")
	bitmaskVal := Bitmask(2048, func() []byte {
		b := make([]byte, 256)
		for i := range b {
			b[i] = 0xAA
		}
		return b
	}())
	boolArr := func() *Value {
		items := make([]*Value, 2048)
		for i := range items {
			items[i] = Bool(i%2 == 0)
		}
		return Array(items...)
	}()
	intArr := func() *Value {
		items := make([]*Value, 2048)
		for i := range items {
			if i%2 == 0 {
				items[i] = Int64(1)
			} else {
				items[i] = Int64(0)
			}
		}
		return Array(items...)
	}()

	bitmaskEnc, _ := Encode(bitmaskVal)
	boolEnc, _ := Encode(boolArr)
	intEnc, _ := Encode(intArr)

	fmt.Printf("%-30s %8d bytes\n", "Bitmask(2048)", len(bitmaskEnc))
	fmt.Printf("%-30s %8d bytes\n", "Bool array(2048)", len(boolEnc))
	fmt.Printf("%-30s %8d bytes\n", "Int array(2048)", len(intEnc))
	fmt.Printf("Bitmask vs bool array: %.1fx smaller\n", float64(len(boolEnc))/float64(len(bitmaskEnc)))
	fmt.Printf("Bitmask vs int array:  %.1fx smaller\n", float64(len(intEnc))/float64(len(bitmaskEnc)))
}

// BenchmarkV3FixintEncode measures encode speed improvement from fixint.
func BenchmarkV3FixintEncode(b *testing.B) {
	// 1000 objects with small int IDs (0-127 range)
	items := make([]*Value, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = Object(
			Member{Key: "id", Value: Int64(int64(i % 128))},
			Member{Key: "name", Value: String("item")},
			Member{Key: "value", Value: Float64(float64(i) * 0.1)},
		)
	}
	v := Array(items...)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(v)
	}
}

// BenchmarkV3FixintDecode measures decode speed with fixint.
func BenchmarkV3FixintDecode(b *testing.B) {
	items := make([]*Value, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = Object(
			Member{Key: "id", Value: Int64(int64(i % 128))},
			Member{Key: "name", Value: String("item")},
			Member{Key: "value", Value: Float64(float64(i) * 0.1)},
		)
	}
	v := Array(items...)
	data, _ := Encode(v)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}

// BenchmarkV3UnsafeStringsDecode measures decode speed with zero-copy strings.
func BenchmarkV3UnsafeStringsDecode(b *testing.B) {
	// String-heavy payload
	items := make([]*Value, 500)
	for i := 0; i < 500; i++ {
		items[i] = Object(
			Member{Key: "name", Value: String(fmt.Sprintf("user_%d", i))},
			Member{Key: "email", Value: String(fmt.Sprintf("user%d@example.com", i))},
			Member{Key: "city", Value: String("San Francisco")},
			Member{Key: "status", Value: String("active")},
		)
	}
	v := Array(items...)
	data, _ := Encode(v)

	b.Run("SafeStrings", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Decode(data)
		}
	})

	b.Run("UnsafeStrings", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			DecodeWithOptions(data, DecodeOptions{UnsafeStrings: true})
		}
	})
}

// BenchmarkV3ScatterGather measures scatter-gather vs normal encode for tensors.
func BenchmarkV3ScatterGather(b *testing.B) {
	// Create a tensor value
	tensorData := make([]byte, 1024*1024) // 1MB tensor
	for i := range tensorData {
		tensorData[i] = byte(i)
	}
	v := Object(
		Member{Key: "model", Value: String("resnet50")},
		Member{Key: "layer", Value: String("conv1")},
		Member{Key: "weights", Value: Tensor(DTypeFloat32, []uint64{256, 1024}, tensorData)},
	)

	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(v)
		}
	})

	b.Run("EncodeToWriter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			buf.Grow(1024*1024 + 256)
			EncodeToWriter(&buf, v)
		}
	})
}

// BenchmarkV3BitmaskEncode measures bitmask vs bool array encode.
func BenchmarkV3BitmaskEncode(b *testing.B) {
	bitmaskVal := Bitmask(2048, func() []byte {
		bits := make([]byte, 256)
		for i := range bits {
			bits[i] = 0xAA
		}
		return bits
	}())

	boolArr := func() *Value {
		items := make([]*Value, 2048)
		for i := range items {
			items[i] = Bool(i%2 == 0)
		}
		return Array(items...)
	}()

	b.Run("Bitmask", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(bitmaskVal)
		}
	})

	b.Run("BoolArray", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(boolArr)
		}
	})
}

// BenchmarkV3BitmaskDecode measures bitmask vs bool array decode.
func BenchmarkV3BitmaskDecode(b *testing.B) {
	bitmaskVal := Bitmask(2048, func() []byte {
		bits := make([]byte, 256)
		for i := range bits {
			bits[i] = 0xAA
		}
		return bits
	}())
	bitmaskData, _ := Encode(bitmaskVal)

	boolArr := func() *Value {
		items := make([]*Value, 2048)
		for i := range items {
			items[i] = Bool(i%2 == 0)
		}
		return Array(items...)
	}()
	boolData, _ := Encode(boolArr)

	b.Run("Bitmask", func(b *testing.B) {
		b.SetBytes(int64(len(bitmaskData)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Decode(bitmaskData)
		}
	})

	b.Run("BoolArray", func(b *testing.B) {
		b.SetBytes(int64(len(boolData)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Decode(boolData)
		}
	})
}

// BenchmarkV3TensorSink measures streaming tensor decode vs standard decode.
func BenchmarkV3TensorSink(b *testing.B) {
	// 1MB tensor
	tensorData := make([]byte, 1024*1024)
	for i := range tensorData {
		tensorData[i] = byte(i)
	}
	v := Tensor(DTypeFloat32, []uint64{256, 1024}, tensorData)
	data, _ := Encode(v)

	b.Run("StandardDecode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Decode(data)
		}
	})

	b.Run("TensorSinkDecode", func(b *testing.B) {
		sink := &discardSink{}
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			DecodeWithOptions(data, DecodeOptions{TensorSink: sink})
		}
	})
}

// BenchmarkV3LargePayload benchmarks the full 1000-object payload encode+decode.
func BenchmarkV3LargePayload(b *testing.B) {
	items := make([]*Value, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = Object(
			Member{Key: "id", Value: Int64(int64(i))},
			Member{Key: "name", Value: String("item")},
			Member{Key: "value", Value: Float64(float64(i) * 0.1)},
		)
	}
	v := Array(items...)
	data, _ := Encode(v)

	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(v)
		}
	})

	b.Run("Decode", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Decode(data)
		}
	})

	b.Run("DecodeUnsafe", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			DecodeWithOptions(data, DecodeOptions{UnsafeStrings: true})
		}
	})
}

// discardSink reads tensor data without allocating storage.
type discardSink struct{}

func (d *discardSink) AcceptTensor(dtype DType, dims []uint64, data io.Reader) error {
	buf := make([]byte, 4096)
	for {
		_, err := data.Read(buf)
		if err != nil {
			break
		}
	}
	return nil
}

// Helper to compute wire size of a float64 tensor vs float64 array
func TestV3TensorVsArraySize(t *testing.T) {
	fmt.Println("\n=== Tensor vs Float64 Array ===")
	sizes := []int{100, 1000, 10000}
	for _, n := range sizes {
		// Float64 array
		items := make([]*Value, n)
		for i := range items {
			items[i] = Float64(float64(i) * 0.001)
		}
		arrVal := Array(items...)
		arrData, _ := Encode(arrVal)

		// Tensor
		tensorBytes := make([]byte, 8*n)
		for i := 0; i < n; i++ {
			bits := math.Float64bits(float64(i) * 0.001)
			tensorBytes[i*8] = byte(bits)
			tensorBytes[i*8+1] = byte(bits >> 8)
			tensorBytes[i*8+2] = byte(bits >> 16)
			tensorBytes[i*8+3] = byte(bits >> 24)
			tensorBytes[i*8+4] = byte(bits >> 32)
			tensorBytes[i*8+5] = byte(bits >> 40)
			tensorBytes[i*8+6] = byte(bits >> 48)
			tensorBytes[i*8+7] = byte(bits >> 56)
		}
		tVal := Tensor(DTypeFloat64, []uint64{uint64(n)}, tensorBytes)
		tData, _ := Encode(tVal)

		ratio := float64(len(arrData)) / float64(len(tData))
		fmt.Printf("n=%5d: array=%7d bytes, tensor=%7d bytes (tensor %.1fx smaller)\n",
			n, len(arrData), len(tData), ratio)
	}
}
