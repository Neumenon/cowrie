#!/usr/bin/env npx tsx
/**
 * Cowrie Gen1 vs Gen2 Decode Benchmark (TypeScript)
 */
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { performance } from 'perf_hooks';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixtureDir = join(__dirname, 'fixtures');

// Import cowrie
import { decode as gen1Decode } from '../typescript/src/gen1/index.js';
import { decode as gen2Decode } from '../typescript/src/gen2/index.js';

function bench(fn: (data: any) => any, data: any, iterations: number, byteLen: number) {
  for (let i = 0; i < Math.min(iterations, 100); i++) fn(data);
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn(data);
  const elapsed = (performance.now() - start) / 1000;
  return {
    ops: iterations / elapsed,
    us: (elapsed / iterations) * 1e6,
    mbps: (byteLen * iterations / elapsed) / 1e6,
  };
}

const fixtures = ['small', 'medium', 'large', 'floats'] as const;

console.log('='.repeat(72));
console.log('Cowrie Decode Benchmark — TypeScript (with JSON baseline)');
console.log('='.repeat(72));
console.log(`${'Payload'.padEnd(10)} ${'Format'.padStart(6)} ${'Size'.padStart(8)} ${'ops/s'.padStart(10)} ${'us/op'.padStart(10)} ${'MB/s'.padStart(10)}`);
console.log('-'.repeat(72));

for (const name of fixtures) {
  const g1 = new Uint8Array(readFileSync(join(fixtureDir, `${name}.gen1`)));
  const g2 = new Uint8Array(readFileSync(join(fixtureDir, `${name}.gen2`)));
  const js = readFileSync(join(fixtureDir, `${name}.json`), 'utf-8');

  const iters = g1.length < 1000 ? 100000 : 5000;

  const rj = bench((d: string) => JSON.parse(d), js, iters, Buffer.byteLength(js));
  const r1 = bench(gen1Decode, g1, iters, g1.length);
  const r2 = bench(gen2Decode, g2, iters, g2.length);

  console.log(`${name.padEnd(10)} ${'JSON'.padStart(6)} ${(js.length + 'B').padStart(8)} ${rj.ops.toFixed(0).padStart(10)} ${rj.us.toFixed(1).padStart(10)} ${rj.mbps.toFixed(1).padStart(10)}`);
  console.log(`${''.padEnd(10)} ${'Gen1'.padStart(6)} ${(g1.length + 'B').padStart(8)} ${r1.ops.toFixed(0).padStart(10)} ${r1.us.toFixed(1).padStart(10)} ${r1.mbps.toFixed(1).padStart(10)}`);
  console.log(`${''.padEnd(10)} ${'Gen2'.padStart(6)} ${(g2.length + 'B').padStart(8)} ${r2.ops.toFixed(0).padStart(10)} ${r2.us.toFixed(1).padStart(10)} ${r2.mbps.toFixed(1).padStart(10)}`);
  console.log(`${''.padEnd(10)} ${''.padStart(6)} Gen1/JSON=${(r1.ops/rj.ops).toFixed(1)}x  Gen2/JSON=${(r2.ops/rj.ops).toFixed(1)}x`);
  console.log();
}
