package cowrie_test

// vLLM payload shape benchmarks — Go compiled encoder.
// Run: cd cowrie/go && go test -bench=Benchmark -benchmem -run='^$' -timeout=60s

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"

	cowrie "github.com/Neumenon/cowrie/go"
)

func randFloat32Bytes(dims int, rng *rand.Rand) []byte {
	buf := make([]byte, dims*4)
	for i := 0; i < dims; i++ {
		bits := math.Float32bits(rng.Float32())
		buf[i*4] = byte(bits)
		buf[i*4+1] = byte(bits >> 8)
		buf[i*4+2] = byte(bits >> 16)
		buf[i*4+3] = byte(bits >> 24)
	}
	return buf
}

// ── SamplingParams ──

func samplingDict() map[string]any {
	return map[string]any{
		"n": 1, "best_of": 1,
		"presence_penalty": 0.0, "frequency_penalty": 0.0,
		"repetition_penalty": 1.0, "temperature": 0.7,
		"top_p": 0.95, "top_k": 50, "min_p": 0.0,
		"use_beam_search": false, "length_penalty": 1.0,
		"early_stopping": false, "stop": []any{}, "stop_token_ids": []any{},
		"max_tokens": 2048, "min_tokens": 0,
		"skip_special_tokens": true, "spaces_between_special_tokens": true,
	}
}

func BenchmarkSamplingParams_JSON(b *testing.B) {
	p := samplingDict()
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(p)
		_ = data
	}
}

func BenchmarkSamplingParams_Cowrie(b *testing.B) {
	v := cowrie.FromAny(samplingDict())
	for i := 0; i < b.N; i++ {
		data, _ := cowrie.Encode(v)
		_ = data
	}
}

// ── Single embedding 1536-dim ──

func embedCowrie(dims int) *cowrie.Value {
	rng := rand.New(rand.NewSource(42))
	data := randFloat32Bytes(dims, rng)
	members := []cowrie.Member{
		{Key: "object", Value: cowrie.String("embedding")},
		{Key: "index", Value: cowrie.Int64(0)},
		{Key: "embedding", Value: cowrie.Tensor(cowrie.DTypeFloat32, []uint64{uint64(dims)}, data)},
	}
	return cowrie.Object(members...)
}

func embedJSON(dims int) map[string]any {
	rng := rand.New(rand.NewSource(42))
	emb := make([]float64, dims)
	for i := range emb {
		emb[i] = float64(rng.Float32())
	}
	return map[string]any{"object": "embedding", "index": 0, "embedding": emb}
}

func BenchmarkEmbed1536_JSON_Encode(b *testing.B) {
	p := embedJSON(1536)
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(p)
		_ = data
	}
}

func BenchmarkEmbed1536_Cowrie_Encode(b *testing.B) {
	v := embedCowrie(1536)
	for i := 0; i < b.N; i++ {
		data, _ := cowrie.Encode(v)
		_ = data
	}
}

func BenchmarkEmbed1536_Cowrie_Decode(b *testing.B) {
	data, _ := cowrie.Encode(embedCowrie(1536))
	for i := 0; i < b.N; i++ {
		_, _ = cowrie.Decode(data)
	}
}

func BenchmarkEmbed3072_JSON_Encode(b *testing.B) {
	p := embedJSON(3072)
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(p)
		_ = data
	}
}

func BenchmarkEmbed3072_Cowrie_Encode(b *testing.B) {
	v := embedCowrie(3072)
	for i := 0; i < b.N; i++ {
		data, _ := cowrie.Encode(v)
		_ = data
	}
}

// ── KV cache shard (8MB float32 tensor) ──

func kvCacheCowrie() *cowrie.Value {
	rng := rand.New(rand.NewSource(42))
	size := 32 * 8 * 128 * 64
	data := randFloat32Bytes(size, rng)
	members := []cowrie.Member{
		{Key: "layer_start", Value: cowrie.Int64(0)},
		{Key: "layer_end", Value: cowrie.Int64(32)},
		{Key: "seq_id", Value: cowrie.Int64(42)},
		{Key: "kv_cache", Value: cowrie.Tensor(cowrie.DTypeFloat32, []uint64{32, 8, 128, 64}, data)},
	}
	return cowrie.Object(members...)
}

func BenchmarkKVCache_Cowrie_Encode(b *testing.B) {
	v := kvCacheCowrie()
	for i := 0; i < b.N; i++ {
		data, _ := cowrie.Encode(v)
		_ = data
	}
}

func BenchmarkKVCache_Cowrie_Decode(b *testing.B) {
	data, _ := cowrie.Encode(kvCacheCowrie())
	for i := 0; i < b.N; i++ {
		_, _ = cowrie.Decode(data)
	}
}

// ── Size report (not a benchmark, just prints sizes) ──

func TestVLLMPayloadSizes(t *testing.T) {
	// SamplingParams
	jsonSP, _ := json.Marshal(samplingDict())
	cowrieSP, _ := cowrie.Encode(cowrie.FromAny(samplingDict()))
	t.Logf("SamplingParams:  JSON=%d  Cowrie=%d  ratio=%.2fx", len(jsonSP), len(cowrieSP), float64(len(cowrieSP))/float64(len(jsonSP)))

	// Embed 1536
	jsonE, _ := json.Marshal(embedJSON(1536))
	cowrieE, _ := cowrie.Encode(embedCowrie(1536))
	t.Logf("Embed 1536:      JSON=%d  Cowrie=%d  ratio=%.2fx", len(jsonE), len(cowrieE), float64(len(cowrieE))/float64(len(jsonE)))

	// Embed 3072
	jsonE3, _ := json.Marshal(embedJSON(3072))
	cowrieE3, _ := cowrie.Encode(embedCowrie(3072))
	t.Logf("Embed 3072:      JSON=%d  Cowrie=%d  ratio=%.2fx", len(jsonE3), len(cowrieE3), float64(len(cowrieE3))/float64(len(jsonE3)))

	// KV cache
	cowrieKV, _ := cowrie.Encode(kvCacheCowrie())
	t.Logf("KV cache (8MB):  Cowrie=%d bytes (JSON would be ~41MB)", len(cowrieKV))
}
