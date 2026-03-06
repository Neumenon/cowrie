#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "========================================================================"
echo "Cowrie Gen1 vs Gen2 Decode Benchmark — All Languages"
echo "========================================================================"
echo "Hardware: $(uname -m) $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || echo 'unknown')"
echo "Date:     $(date -Iseconds)"
echo ""

# Check fixtures exist
if [ ! -f benchmarks/fixtures/small.gen1 ]; then
    echo "Generating fixtures..."
    (cd go && go run ../benchmarks/generate_fixtures.go)
fi

echo ""
echo "========================================================================"
echo "1. Go"
echo "========================================================================"
(cd benchmarks && go test -bench=BenchmarkDecode -benchmem -count=1 -timeout=120s)

echo ""
echo "========================================================================"
echo "2. Rust"
echo "========================================================================"
(cd rust && cargo bench --bench decode_bench 2>/dev/null)

echo ""
echo "========================================================================"
echo "3. Python"
echo "========================================================================"
python3 benchmarks/bench_decode_python.py

echo ""
echo "========================================================================"
echo "4. TypeScript"
echo "========================================================================"
(cd typescript && npx tsx ../benchmarks/bench_decode_ts.ts)

echo ""
echo "========================================================================"
echo "5. C"
echo "========================================================================"
if [ ! -f benchmarks/bench_decode_c ]; then
    echo "Building C benchmark..."
    gcc -O2 -o benchmarks/bench_decode_c benchmarks/bench_decode_c.c \
        c/src/gen1.c c/src/gen2.c -Ic/include -lz -lm
fi
./benchmarks/bench_decode_c

echo ""
echo "========================================================================"
echo "Done."
echo "========================================================================"
