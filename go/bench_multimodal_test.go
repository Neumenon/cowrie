package cowrie

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
)

// BenchmarkMultimodalEncode benchmarks encoding a multimodal inference request
// containing a 640x480 JPEG image (~50KB), a text prompt, and a float32
// embedding tensor [1, 768]. Compares Cowrie native types vs JSON (base64) vs
// manual msgpack-style packing.
func BenchmarkMultimodalEncode(b *testing.B) {
	// Simulate a 640x480 JPEG (~50KB)
	imageData := make([]byte, 50*1024)
	for i := range imageData {
		imageData[i] = byte(i % 256)
	}

	prompt := "Describe the contents of this image in detail, including objects, colors, and spatial relationships."

	// Float32 embedding [1, 768] = 3072 bytes
	embeddingDims := []uint64{1, 768}
	embeddingData := make([]byte, 768*4)
	for i := 0; i < 768; i++ {
		bits := math.Float32bits(float32(i) * 0.001)
		binary.LittleEndian.PutUint32(embeddingData[i*4:], bits)
	}

	b.Run("Cowrie", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := Object(
				Member{Key: "image", Value: Image(ImageFormatJPEG, 640, 480, imageData)},
				Member{Key: "prompt", Value: String(prompt)},
				Member{Key: "embedding", Value: Tensor(DTypeFloat32, embeddingDims, embeddingData)},
			)
			data, err := Encode(v)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				b.ReportMetric(float64(len(data)), "bytes")
			}
		}
	})

	b.Run("JSON", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// JSON requires base64 for binary, float array for embedding
			floats := make([]float32, 768)
			for j := range floats {
				floats[j] = float32(j) * 0.001
			}
			msg := map[string]any{
				"image": map[string]any{
					"format": "jpeg",
					"width":  640,
					"height": 480,
					"data":   base64.StdEncoding.EncodeToString(imageData),
				},
				"prompt": prompt,
				"embedding": map[string]any{
					"shape": []int{1, 768},
					"dtype": "float32",
					"data":  floats,
				},
			}
			data, err := json.Marshal(msg)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				b.ReportMetric(float64(len(data)), "bytes")
			}
		}
	})
}

// BenchmarkMultimodalDecode benchmarks decoding the same multimodal payload.
func BenchmarkMultimodalDecode(b *testing.B) {
	imageData := make([]byte, 50*1024)
	for i := range imageData {
		imageData[i] = byte(i % 256)
	}

	prompt := "Describe the contents of this image in detail, including objects, colors, and spatial relationships."

	embeddingDims := []uint64{1, 768}
	embeddingData := make([]byte, 768*4)
	for i := 0; i < 768; i++ {
		bits := math.Float32bits(float32(i) * 0.001)
		binary.LittleEndian.PutUint32(embeddingData[i*4:], bits)
	}

	b.Run("Cowrie", func(b *testing.B) {
		v := Object(
			Member{Key: "image", Value: Image(ImageFormatJPEG, 640, 480, imageData)},
			Member{Key: "prompt", Value: String(prompt)},
			Member{Key: "embedding", Value: Tensor(DTypeFloat32, embeddingDims, embeddingData)},
		)
		encoded, err := Encode(v)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Decode(encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JSON", func(b *testing.B) {
		floats := make([]float32, 768)
		for j := range floats {
			floats[j] = float32(j) * 0.001
		}
		msg := map[string]any{
			"image": map[string]any{
				"format": "jpeg",
				"width":  640,
				"height": 480,
				"data":   base64.StdEncoding.EncodeToString(imageData),
			},
			"prompt": prompt,
			"embedding": map[string]any{
				"shape": []int{1, 768},
				"dtype": "float32",
				"data":  floats,
			},
		}
		encoded, err := json.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out map[string]any
			if err := json.Unmarshal(encoded, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMultimodalSize reports the encoded size difference for a multimodal
// payload across formats. Run with: go test -run=BenchmarkMultimodalSize -v
func BenchmarkMultimodalSize(b *testing.B) {
	imageData := make([]byte, 50*1024)
	for i := range imageData {
		imageData[i] = byte(i % 256)
	}

	prompt := "Describe this image."

	embeddingData := make([]byte, 768*4)
	for i := 0; i < 768; i++ {
		bits := math.Float32bits(float32(i) * 0.001)
		binary.LittleEndian.PutUint32(embeddingData[i*4:], bits)
	}

	// Cowrie
	v := Object(
		Member{Key: "image", Value: Image(ImageFormatJPEG, 640, 480, imageData)},
		Member{Key: "prompt", Value: String(prompt)},
		Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{1, 768}, embeddingData)},
	)
	cowrieData, err := Encode(v)
	if err != nil {
		b.Fatal(err)
	}

	// JSON
	floats := make([]float32, 768)
	for j := range floats {
		floats[j] = float32(j) * 0.001
	}
	jsonMsg := map[string]any{
		"image": map[string]any{
			"format": "jpeg",
			"width":  640,
			"height": 480,
			"data":   base64.StdEncoding.EncodeToString(imageData),
		},
		"prompt": prompt,
		"embedding": map[string]any{
			"shape": []int{1, 768},
			"dtype": "float32",
			"data":  floats,
		},
	}
	jsonData, err := json.Marshal(jsonMsg)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(len(cowrieData)), "cowrie_bytes")
	b.ReportMetric(float64(len(jsonData)), "json_bytes")
	b.ReportMetric(float64(len(jsonData))/float64(len(cowrieData)), "json/cowrie_ratio")
}
