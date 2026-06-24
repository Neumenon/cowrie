/**
 * Regression tests for two JSON-bridge bugs:
 *  Bug #3 — stringToDtype() did not cover bool/qint4/qint2/qint3/ternary/binary
 *  Bug #4 — intFieldFromObj() did not range-check fixed-width wire fields
 *
 * Run: node --import tsx --test src/gen2/bugfix.test.ts
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  SJ,
  Type,
  DType,
  toAny,
  fromAny,
  decode,
  encode,
  CowrieError,
  ERR_RESERVED_TAG,
} from './index.ts';

// ============================================================
// Bug #3 — every DType must round-trip through toAny → fromAny
// ============================================================

// Minimal valid payloads for each DType. Sub-byte packed types use a small
// payload; byte-aligned types use shape [1] so we need elemSize bytes.
function makeTensor(dtype: DType): ReturnType<typeof SJ.tensor> {
  switch (dtype) {
    // 4-byte elem
    case DType.FLOAT32:
      return SJ.tensor(dtype, [1], new Uint8Array(4));
    // 2-byte elem
    case DType.FLOAT16:
    case DType.BFLOAT16:
    case DType.INT16:
    case DType.UINT16:
      return SJ.tensor(dtype, [1], new Uint8Array(2));
    // 1-byte elem
    case DType.INT8:
    case DType.UINT8:
    case DType.BOOL:
      return SJ.tensor(dtype, [1], new Uint8Array(1));
    // 4-byte elem
    case DType.INT32:
    case DType.UINT32:
      return SJ.tensor(dtype, [1], new Uint8Array(4));
    // 8-byte elem
    case DType.INT64:
    case DType.UINT64:
    case DType.FLOAT64:
      return SJ.tensor(dtype, [1], new Uint8Array(8));
    // Sub-byte packed — use a small 1-byte payload with shape [] (rank-0 scalar)
    // tensorExpectedBytes returns null for these, so payload size is not validated.
    case DType.QINT4:
    case DType.QINT2:
    case DType.QINT3:
    case DType.TERNARY:
    case DType.BINARY:
      return SJ.tensor(dtype, [], new Uint8Array(1));
    default:
      throw new Error(`Unhandled dtype: ${dtype}`);
  }
}

describe('Bug #3 — dtype JSON round-trip', () => {
  const allDtypes: DType[] = [
    DType.FLOAT32,
    DType.FLOAT16,
    DType.BFLOAT16,
    DType.INT8,
    DType.INT16,
    DType.INT32,
    DType.INT64,
    DType.UINT8,
    DType.UINT16,
    DType.UINT32,
    DType.UINT64,
    DType.FLOAT64,
    DType.BOOL,
    DType.QINT4,
    DType.QINT2,
    DType.QINT3,
    DType.TERNARY,
    DType.BINARY,
  ];

  for (const dtype of allDtypes) {
    it(`DType 0x${dtype.toString(16)} (${DType[dtype]}) round-trips through toAny→fromAny`, () => {
      const original = makeTensor(dtype);
      const projected = toAny(original);
      const reconstructed = fromAny(projected);
      assert.strictEqual(reconstructed.type, Type.TENSOR,
        `Expected TENSOR type after fromAny for dtype ${DType[dtype]}`);
      const td = reconstructed.data as { dtype: DType; shape: number[]; data: Uint8Array };
      assert.strictEqual(td.dtype, dtype,
        `Dtype mismatch: expected ${DType[dtype]} (0x${dtype.toString(16)}), got ${DType[td.dtype]} (0x${td.dtype.toString(16)})`);
    });
  }
});

// ============================================================
// Bug #4 — out-of-range fixed-width fields must throw, not wrap
// ============================================================

describe('Bug #4 — fixed-width field range validation', () => {
  it('tensor_ref.store=300 (uint8 overflow) throws RangeError', () => {
    assert.throws(
      () => fromAny({ _type: 'tensor_ref', store: 300, key: 'AQ==' }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('store'), `Expected message to mention 'store': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('tensor_ref.store=255 (uint8 max) does not throw', () => {
    const v = fromAny({ _type: 'tensor_ref', store: 255, key: 'AQ==' });
    assert.strictEqual(v.type, Type.TENSOR_REF);
    const ref = v.data as { storeId: number; key: Uint8Array };
    assert.strictEqual(ref.storeId, 255);
  });

  it('image.width=70000 (uint16 overflow) throws RangeError', () => {
    assert.throws(
      () => fromAny({ _type: 'image', format: 'jpeg', width: 70000, height: 100, data: btoa('\xff\xd8\xff') }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('width'), `Expected message to mention 'width': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('image.height=70000 (uint16 overflow) throws RangeError', () => {
    assert.throws(
      () => fromAny({ _type: 'image', format: 'jpeg', width: 100, height: 70000, data: btoa('\xff\xd8\xff') }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('height'), `Expected message to mention 'height': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('audio.channels=300 (uint8 overflow) throws RangeError', () => {
    assert.throws(
      () => fromAny({ _type: 'audio', encoding: 'pcm_int16', rate: 44100, channels: 300, data: '' }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('channels'), `Expected message to mention 'channels': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('audio.rate=4294967296 (uint32 overflow) throws RangeError', () => {
    assert.throws(
      () => fromAny({ _type: 'audio', encoding: 'pcm_int16', rate: 4294967296, channels: 2, data: '' }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('rate'), `Expected message to mention 'rate': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('audio.channels=0 (no frames) throws RangeError', () => {
    // Regression: channels=0 was accepted (n < 0 was the only check), unlike
    // Python which rejected it. Zero channels is semantically invalid.
    assert.throws(
      () => fromAny({ _type: 'audio', encoding: 'pcm_int16', rate: 44100, channels: 0, data: '' }),
      (err: unknown) => {
        assert.ok(err instanceof RangeError, `Expected RangeError, got ${(err as Error).constructor.name}`);
        assert.ok((err as Error).message.includes('channels'), `Expected message to mention 'channels': ${(err as Error).message}`);
        return true;
      }
    );
  });

  it('audio.channels=2 (uint8 valid) does not throw', () => {
    const v = fromAny({ _type: 'audio', encoding: 'pcm_int16', rate: 44100, channels: 2, data: '' });
    assert.strictEqual(v.type, Type.AUDIO);
  });

  it('fromAny canonicalizes object key order (cross-language determinism)', () => {
    // Same object, different key order -> identical bytes, so a content hash on the
    // raw bytes is portable across Go/Rust/Python/TS. Matches Go from_json + Rust BTreeMap.
    const enc = (j: string) => Buffer.from(encode(fromAny(JSON.parse(j)))).toString('hex');
    const a = enc('{"b":1,"a":2,"c":3}');
    assert.strictEqual(a, enc('{"c":3,"a":2,"b":1}'));
    assert.strictEqual(a, enc('{"a":2,"b":1,"c":3}'));
    assert.strictEqual(enc('{"z":{"y":1,"x":2},"a":3}'), enc('{"a":3,"z":{"x":2,"y":1}}'));
  });

  it('binary decode rejects AUDIO tag on the wire with ERR_RESERVED_TAG', () => {
    // SPEC-v1 §2.3 / §4: AUDIO (tag 0x23) is no longer a core wire tag; the
    // decoder rejects it with ERR_RESERVED_TAG before any field-level (e.g.
    // channels) validation. The wire-level Audio decode path was removed.
    // Wire: 'COWR' 01 00 (header) 00 (dictLen) | TAG_AUDIO 23 | enc 01 |
    //       sampleRate 44100 LE | channels 00 | len 02 | data
    const bytes = new Uint8Array([
      0x43, 0x4f, 0x57, 0x52, 0x01, 0x00, 0x00,
      0x23, 0x01, 0x44, 0xac, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
    ]);
    assert.throws(
      () => decode(bytes),
      (err: unknown) =>
        err instanceof CowrieError && err.code === ERR_RESERVED_TAG,
    );
  });
});
