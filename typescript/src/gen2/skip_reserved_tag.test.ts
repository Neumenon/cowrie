/**
 * C4: reserved-tag rejection test.
 *
 * Per SPEC-v1 §2.3 / §4, the TS decoder MUST reject any non-core/reserved wire
 * tag with ERR_RESERVED_TAG. Previously the decoder silently skipped reserved
 * tags (0x30 Adjlist, 0x31 RichText, 0x32 Delta, 0x39 GraphShard); that behavior
 * was removed. This test now verifies the decoder throws on those tags.
 *
 * The raw bytes are constructed by hand following the Gen2 wire format:
 *   [magic 4B 'COWR'][version 1B][compression 1B][dict-len varint][dict-entries...]
 *   [root-value]
 *
 * The root is a 2-field object:
 *   { "reserved": <reserved tag, payload>, "kept": "hello" }
 *
 * Expected decode: throws CowrieError with code ERR_RESERVED_TAG.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { decode, CowrieError, ERR_RESERVED_TAG } from './index.ts';

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

describe('gen2 rejects reserved tags', () => {
  for (const [name, tag] of [
    ['0x30 (Adjlist)', 0x30],
    ['0x31 (RichText)', 0x31],
    ['0x32 (Delta)', 0x32],
    ['0x39 (GraphShard)', 0x39],
  ] as [string, number][]) {
    it(`rejects reserved tag ${name} with ERR_RESERVED_TAG`, () => {
      const stream = buildStream(tag, 4);

      assert.throws(
        () => decode(stream),
        (err: unknown) =>
          err instanceof CowrieError && err.code === ERR_RESERVED_TAG,
      );
    });
  }

  it('rejects zero-length reserved tag payload', () => {
    const stream = buildStream(0x30, 0);
    assert.throws(
      () => decode(stream),
      (err: unknown) =>
        err instanceof CowrieError && err.code === ERR_RESERVED_TAG,
    );
  });

  it('rejects multi-byte reserved tag payload (16 bytes)', () => {
    const stream = buildStream(0x31, 16);
    assert.throws(
      () => decode(stream),
      (err: unknown) =>
        err instanceof CowrieError && err.code === ERR_RESERVED_TAG,
    );
  });
});
