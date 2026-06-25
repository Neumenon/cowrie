/**
 * Cowrie v1 (`COWR`) — deterministic, content-addressable, AI-native binary codec.
 *
 * Equal value ⇒ equal canonical bytes ⇒ equal hash, byte-identical across Python/Go/Rust/TS.
 *
 * @example
 * ```typescript
 * import { fromAny, encode, decode, toAny, contentAddress } from 'cowrie-codec';
 * const v = fromAny({ name: 'Alice', scores: [1, 2, 3] }); // plain JS -> typed Value
 * const wire = encode(v);                                   // canonical bytes
 * toAny(decode(wire));                                      // -> { name: 'Alice', scores: [1, 2, 3] }
 * contentAddress(v);                                        // multihash SHA-256 identity (§3)
 * ```
 *
 * @module
 */

// Legacy v2 namespaces (gen1 = lightweight, gen2 = the v1 codec impl); kept for back-compat.
export * as gen1 from './gen1';
export * as gen2 from './gen2';

// Value-level API.
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

// v1 identity surface (parity with the Python reference's top-level API):
// content address (§3), structural fingerprint (§4), file / Merkle identity (§7), tensor zero-copy spans.
export {
  contentAddress,
  contentAddressHex,
  addressOfBytes,
  schemaFingerprint64,
  fileIdentity,
  merkleRoot,
  decodeFile,
  encodeFile,
  tensorSpans,
} from './gen2';
