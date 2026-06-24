/**
 * C4: skip-reserved-tag round-trip test.
 *
 * Verifies that the TS decoder silently skips reserved tags (0x30 Adjlist,
 * 0x31 RichText, 0x32 Delta, 0x39 GraphShard) inside a Gen2 stream without
 * throwing, and that sibling kept fields decode correctly.
 *
 * The raw bytes are constructed by hand following the Gen2 wire format:
 *   [magic 4B 'COWR'][version 1B][compression 1B][dict-len varint][dict-entries...]
 *   [root-value]
 *
 * The root is a 2-field object:
 *   { "reserved": <tag 0x30, 4-byte payload>, "kept": "hello" }
 *
 * Expected decode: { reserved: null, kept: "hello" }
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { decode, Type } from './index.ts';
import type { Value } from './index.ts';

/**
 * Build a uvarint (little-endian base-128) byte sequence for a non-negative
 * integer. Only handles values that fit in a JS number (sufficient for tests).
 */
function uvarint(n: number): number[] {
  const out: number[] = [];
  while (n >= 0x80) {
    out.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  out.push(n);
  return out;
}

/** Encode a UTF-8 string as uvarint(len) + bytes. */
function encStr(s: string): number[] {
  const enc = new TextEncoder().encode(s);
  return [...uvarint(enc.length), ...enc];
}

/**
 * Build a minimal Gen2 stream with a 2-field object:
 *   { [dictKey0]: <reservedTag, payloadLen bytes>, [dictKey1]: "hello" }
 */
function buildStream(reservedTag: number, payloadLen: number): Uint8Array {
  const MAGIC = [0x43, 0x4f, 0x57, 0x52]; // 'COWR' (SPEC-v1 §2.2)
  const VERSION = 1;
  const COMPRESSION = 0; // none

  const key0 = 'reserved';
  const key1 = 'kept';
  const keptValue = 'hello';

  const payload = Array.from({ length: payloadLen }, (_, i) => 0xaa + (i & 0xf));

  const bytes: number[] = [
    ...MAGIC,
    VERSION,
    COMPRESSION,
    // dict: 2 entries
    ...uvarint(2),
    ...encStr(key0),
    ...encStr(key1),
    // root: FIXMAP with 2 entries (Tag.FIXMAP_BASE = 0xD0)
    0xd0 + 2,
    // field 0: dict index 0, reserved tag, payload
    ...uvarint(0),
    reservedTag,
    ...uvarint(payloadLen),
    ...payload,
    // field 1: dict index 1, STRING tag (0x05), "hello"
    ...uvarint(1),
    0x05,
    ...encStr(keptValue),
  ];

  return new Uint8Array(bytes);
}

describe('gen2 skip reserved tags', () => {
  for (const [name, tag] of [
    ['0x30 (Adjlist)', 0x30],
    ['0x31 (RichText)', 0x31],
    ['0x32 (Delta)', 0x32],
    ['0x39 (GraphShard)', 0x39],
  ] as [string, number][]) {
    it(`silently skips reserved tag ${name} and keeps sibling fields`, () => {
      const stream = buildStream(tag, 4);

      // Must not throw
      let result: Value;
      assert.doesNotThrow(() => {
        result = decode(stream);
      });

      assert.strictEqual(result!.type, Type.OBJECT);
      const obj = result!.data as Record<string, Value>;

      // Reserved field decodes as null (skip returns SJ.null())
      assert.strictEqual(obj['reserved'].type, Type.NULL);

      // Kept field decodes correctly
      assert.strictEqual(obj['kept'].type, Type.STRING);
      assert.strictEqual(obj['kept'].data, 'hello');
    });
  }

  it('silently skips zero-length reserved tag payload', () => {
    const stream = buildStream(0x30, 0);
    let result: Value;
    assert.doesNotThrow(() => {
      result = decode(stream);
    });
    assert.strictEqual((result!.data as Record<string, Value>)['reserved'].type, Type.NULL);
    assert.strictEqual((result!.data as Record<string, Value>)['kept'].data, 'hello');
  });

  it('silently skips multi-byte reserved tag payload (16 bytes)', () => {
    const stream = buildStream(0x31, 16);
    let result: Value;
    assert.doesNotThrow(() => {
      result = decode(stream);
    });
    assert.strictEqual((result!.data as Record<string, Value>)['reserved'].type, Type.NULL);
    assert.strictEqual((result!.data as Record<string, Value>)['kept'].data, 'hello');
  });
});
