package gen1

import (
	"encoding/json"
	"testing"
)

func TestEncodeTypedSlices(t *testing.T) {
	// []string - exercises tagArrayString and readArrayString
	t.Run("string_slice", func(t *testing.T) {
		data, err := Encode([]string{"hello", "world", "test"})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		arr, ok := result.([]string)
		if !ok {
			t.Fatalf("expected []string, got %T", result)
		}
		if len(arr) != 3 || arr[0] != "hello" {
			t.Error("mismatch")
		}
	})

	// []float64 - exercises appendFloat64Slice
	t.Run("float64_slice", func(t *testing.T) {
		data, err := Encode([]float64{1.1, 2.2, 3.3})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// []float32 - exercises tagArrayFloat32
	t.Run("float32_slice", func(t *testing.T) {
		data, err := Encode([]float32{1.5, 2.5, 3.5})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// []int - exercises int slice conversion
	t.Run("int_slice", func(t *testing.T) {
		data, err := Encode([]int{10, 20, 30})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// []int64 - exercises appendInt64Slice
	t.Run("int64_slice", func(t *testing.T) {
		data, err := Encode([]int64{100, 200, 300})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// [][]float64 - exercises matrix encoding
	t.Run("float64_matrix", func(t *testing.T) {
		data, err := Encode([][]float64{{1.0, 2.0}, {3.0, 4.0}})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// []float64 with high precision
	t.Run("float64_slice_highprec", func(t *testing.T) {
		opts := EncodeOptions{HighPrecision: true}
		data, err := EncodeWithOptions([]float64{3.141592653589793, 2.718281828459045}, opts)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	// [][]float64 with high precision
	t.Run("float64_matrix_highprec", func(t *testing.T) {
		opts := EncodeOptions{HighPrecision: true}
		data, err := EncodeWithOptions([][]float64{{1.1, 2.2}, {3.3, 4.4}}, opts)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})
}

func TestEncodeAppendValue_TypedSlices(t *testing.T) {
	// Directly call appendValue (not WithOpts) to cover the wrapper
	t.Run("append_value_nil", func(t *testing.T) {
		buf, err := appendValue(nil, "hello")
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) == 0 {
			t.Error("empty")
		}
	})

	// appendArray directly
	t.Run("append_array", func(t *testing.T) {
		buf, err := appendArray(nil, []any{1, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) == 0 {
			t.Error("empty")
		}
	})
}

func TestEncodeNumericArrayPromotion(t *testing.T) {
	// Large numeric array (>= NumericArrayMin) triggers asFloatSlice/asIntSlice
	t.Run("large_float_array", func(t *testing.T) {
		arr := make([]any, 20)
		for i := range arr {
			arr[i] = float64(i) * 1.1
		}
		data, err := Encode(arr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Decode(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	// Large int array triggers asIntSlice
	t.Run("large_int_array", func(t *testing.T) {
		arr := make([]any, 20)
		for i := range arr {
			arr[i] = int64(i * 10)
		}
		data, err := Encode(arr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Decode(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	// Large mixed int types for asIntSlice
	t.Run("large_mixed_int_array", func(t *testing.T) {
		arr := make([]any, 20)
		for i := range arr {
			switch i % 4 {
			case 0:
				arr[i] = int(i)
			case 1:
				arr[i] = int64(i)
			case 2:
				arr[i] = int32(i)
			case 3:
				arr[i] = json.Number("42")
			}
		}
		data, err := Encode(arr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Decode(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	// Large float array with json.Number
	t.Run("large_float_json_number", func(t *testing.T) {
		arr := make([]any, 20)
		for i := range arr {
			if i%2 == 0 {
				arr[i] = float64(i)
			} else {
				arr[i] = json.Number("3.14")
			}
		}
		data, err := Encode(arr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Decode(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	// Large compact float array
	t.Run("large_compact_float_array", func(t *testing.T) {
		arr := make([]any, 20)
		for i := range arr {
			arr[i] = float64(i)
		}
		opts := EncodeOptions{CompactFloats: true}
		data, err := EncodeWithOptions(arr, opts)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Decode(data)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestEncodeUnknownTypeViaJSON(t *testing.T) {
	// A struct not directly handled — triggers default JSON round-trip
	type Custom struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	data, err := Encode(Custom{Name: "Alice", Age: 30})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["name"] != "Alice" {
		t.Error("name mismatch")
	}
}

func TestEncodeUintTypes(t *testing.T) {
	tests := []struct {
		name string
		v    any
	}{
		{"uint", uint(42)},
		{"uint8", uint8(42)},
		{"uint16", uint16(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.v)
			if err != nil {
				t.Fatal(err)
			}
			result, err := Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			_ = result
		})
	}
}

func TestEncodeCompactFloats_Float32(t *testing.T) {
	// Float32 with CompactFloats
	opts := EncodeOptions{CompactFloats: true}
	data, err := EncodeWithOptions(float32(1.5), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	_ = result

	// Float32 without CompactFloats
	opts2 := EncodeOptions{CompactFloats: false}
	data2, err := EncodeWithOptions(float32(1.5), opts2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Decode(data2)
	if err != nil {
		t.Fatal(err)
	}
}
