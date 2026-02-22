/**
 * SJSON - Structured JSON binary codec.
 *
 * This package provides two codecs for binary JSON serialization:
 *
 * - **gen1**: Lightweight codec with proto-tensor support (11 core types + graph types)
 * - **gen2**: Full SJSON v2 with ML extensions (18+ types, dictionary coding)
 *
 * @example
 * ```typescript
 * // Gen1 (Lightweight)
 * import { gen1 } from 'sjson';
 * const data = gen1.encode({ name: 'Alice', scores: [1.0, 2.0, 3.0] });
 * const decoded = gen1.decode(data);
 *
 * // Gen2 (Full)
 * import { gen2 } from 'sjson';
 * const val = gen2.Value.object([
 *   ['name', gen2.Value.string('Alice')],
 *   ['embedding', gen2.Value.tensor(new Float32Array([0.1, 0.2, 0.3]))],
 * ]);
 * const encoded = gen2.encode(val);
 * const result = gen2.decode(encoded);
 * ```
 *
 * @module
 */

export * as gen1 from './gen1';
export * as gen2 from './gen2';

// Re-export common functions from gen2 for convenience
export {
  encode,
  decode,
  fromJSON,
  toJSON,
  fromAny,
  toAny,
  Value,
  Type,
  SJ,
  Compression,
} from './gen2';
