# SJSON Benchmarks

Performance benchmarks for SJSON codec implementations.

## Running Benchmarks

### Go

```bash
cd go
go test -bench=. -benchmem ../benchmarks/
```

### Python

```bash
cd python
python ../benchmarks/bench_python.py
```

### Rust

```bash
cd rust
cargo bench
```

## Benchmark Categories

1. **Encode/Decode Speed** - Operations per second for various payload sizes
2. **Payload Size** - Compressed size vs JSON
3. **Memory Usage** - Allocations during encode/decode
4. **Graph Types** - Performance of new v2.1 graph types

## Expected Results

### Throughput (approximate)

| Implementation | Gen1 Encode | Gen1 Decode | Gen2 Encode | Gen2 Decode |
|---------------|-------------|-------------|-------------|-------------|
| Go            | ~500 MB/s   | ~600 MB/s   | ~300 MB/s   | ~400 MB/s   |
| Rust          | ~450 MB/s   | ~550 MB/s   | ~250 MB/s   | ~350 MB/s   |
| Python        | ~15 MB/s    | ~20 MB/s    | ~8 MB/s     | ~12 MB/s    |

### Payload Size Savings

| Payload Type | JSON | Gen1 | Gen2 |
|--------------|------|------|------|
| Small object | 100% | ~90% | ~85% |
| Large array (repeated keys) | 100% | ~85% | ~60% |
| Float array | 100% | ~50% | ~50% |

Gen2 shows best savings with repeated object keys due to dictionary coding.
