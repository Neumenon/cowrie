# vLLM Serialization Benchmark

Compares Cowrie Gen2 against JSON, pickle5, and msgpack+ExtType for real vLLM payload shapes.

**Context**: vLLM migrated from pickle to msgpack in [PR #12918](https://github.com/vllm-project/vllm/pull/12918). This benchmark measures the next step — native tensor encoding with zero-copy decode.

## Results

Measured on real payload shapes (SamplingParams, OpenAI embedding responses, KV cache shards):

| Payload | JSON | Pickle5 | MsgPack+ext | Cowrie | vs JSON |
|---------|------|---------|-------------|--------|---------|
| SamplingParams | 127 B | 611 B | 96 B | **89 B** | 0.70x |
| Embedding 1536-dim | 32 KB | 340 B | 6.1 KB | **6.1 KB** | 0.19x |
| Embedding 3072-dim | 64 KB | 594 B | 12.3 KB | **12.3 KB** | 0.19x |
| Batch 32×1536 | 1.56 MB | — | 820 KB | **410 KB** | 0.27x |
| KV cache 32L×8H×128D×64T | — | — | 8.0 MB | **8.0 MB** | zero-copy |

**Key advantages over msgpack+ExtType**:
- **Batch payloads**: Dictionary coding amortizes field names — 50% smaller than msgpack on batch responses
- **Zero-copy decode**: `TensorData.view_float32()` returns a numpy array backed by the wire buffer
- **Native dtype**: float32/float16/bfloat16/uint8 encoded in wire format — no guessing, no custom ExtType registration
- **SamplingParams**: 7% smaller than msgpack with simpler encoding (no extension types needed)

## Reproduce

```bash
pip install cowrie-py msgpack numpy
python benchmarks/vllm/benchmark.py
```

Optional: install `vllm` to benchmark against real `SamplingParams` objects (falls back to dict equivalent if not installed).

## Files

- `benchmark.py` — Standalone reproducer with all 6 payload types
- `notebook.py` — Detailed analysis with framed encoding, compression, and zero-copy demos

## References

- [vLLM #6241](https://github.com/vllm-project/vllm/issues/6241) — Original serialization perf proposal
- [vLLM #12918](https://github.com/vllm-project/vllm/pull/12918) — Pickle → msgpack migration
- [vLLM #16860](https://github.com/vllm-project/vllm/pull/16860) — bf16 zero-copy tensor fix
- [vLLM #21796](https://github.com/vllm-project/vllm/issues/21796) — Embedding optimization RFC (open)
