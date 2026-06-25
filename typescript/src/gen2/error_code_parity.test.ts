/**
 * RED-first regression tests for two cross-language parity bugs in the gen2 decoder:
 *
 *  Bug (1) — decode failure paths that threw a plain `Error` (no stable ERR_* code).
 *    The Python oracle (tools/cowrie_ref/decode.py) raises CowrieError with a specific
 *    ERR_* token for these conditions; TS/Rust must match:
 *      - OBJECT-arm (tag 0x07) field id >= dict length        -> ERR_INVALID_FIELD_ID  (decode.py:191)
 *      - TENSOR dataLen != product(shape)*elemSize             -> ERR_NON_CANONICAL     (decode.py:166)
 *
 *  Bug (2) — tensorSpans() defaulted strict=false; the Python oracle's tensor_spans()
 *    defaults strict=True (decode.py:210). Non-canonical input must be rejected by default.
 *
 * Verified against the venv oracle:
 *   A invalid-field OBJECT  -> ERR_INVALID_FIELD_ID
 *   B tensor-datalen        -> ERR_NON_CANONICAL
 *   D tensorSpans default   -> ERR_NON_CANONICAL  (strict=false -> OK)
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  decode,
  encode,
  tensorSpans,
  SJ,
  CowrieError,
  SecurityLimitExceeded,
  ERR_INVALID_FIELD_ID,
  ERR_NON_CANONICAL,
  ERR_TOO_LARGE,
} from './index.ts';

function hex(s: string): Uint8Array {
  const clean = s.replace(/\s+/g, '');
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return out;
}

// Header: COWR (43 4f 57 52) + version 01 + compression 00
const HEADER = '434f57520100';

function expectCode(fn: () => unknown, code: string): void {
  let thrown: unknown;
  try {
    fn();
  } catch (e) {
    thrown = e;
  }
  assert.ok(thrown instanceof CowrieError, `expected CowrieError, got ${thrown}`);
  assert.strictEqual((thrown as CowrieError).code, code);
}

describe('Bug (1): decode failures carry a stable ERR_* code (Python-oracle parity)', () => {
  it('OBJECT arm with field id >= dict length rejects with ERR_INVALID_FIELD_ID', () => {
    // dict empty (00) + OBJECT tag (07) + count uvarint 01 + fieldId uvarint 00.
    // dict has 0 keys, so fieldId 0 is out of range. Oracle -> ERR_INVALID_FIELD_ID.
    const data = hex(HEADER + '00' + '07' + '01' + '00');
    expectCode(() => decode(data), ERR_INVALID_FIELD_ID);
  });

  it('TENSOR with mismatched dataLen rejects with ERR_NON_CANONICAL', () => {
    // dict empty (00) + TENSOR (20) + dtype int8 (04, 8 bits) + rank 01 + shape[1]=01
    // + dataLen 02 (expected 1 byte) + 64B-aligned zero padding + 2 data bytes.
    const pre = HEADER + '00' + '20' + '04' + '01' + '01' + '02';
    const pos = pre.length / 2;
    const pad = (((-pos) % 64) + 64) % 64;
    const data = hex(pre + '00'.repeat(pad) + '0000');
    expectCode(() => decode(data), ERR_NON_CANONICAL);
  });
});

describe('Bug (1) cont.: size-limit overruns surface ERR_TOO_LARGE (decode.py MAX_* parity)', () => {
  it('array exceeding maxArrayLen carries .code ERR_TOO_LARGE', () => {
    const enc = encode(SJ.array([SJ.int64(1n), SJ.int64(2n), SJ.int64(3n)]));
    let thrown: unknown;
    try {
      decode(enc, { maxArrayLen: 2 });
    } catch (e) {
      thrown = e;
    }
    assert.ok(thrown instanceof SecurityLimitExceeded, `expected SecurityLimitExceeded, got ${thrown}`);
    assert.strictEqual((thrown as SecurityLimitExceeded).code, ERR_TOO_LARGE);
    // The conformance gate extracts the first /ERR_[A-Z_]+/ token from the message.
    assert.match((thrown as Error).message, /ERR_TOO_LARGE/);
  });
});

describe('Bug (2): tensorSpans defaults to strict=true (Python tensor_spans parity)', () => {
  // Well-formed-but-non-canonical: top-level INT64 (03) carrying zigzag(5)=0x0a,
  // i.e. value 5 which belongs in the FIXINT range. Strict decode rejects this.
  const nonCanonical = hex(HEADER + '00' + '03' + '0a');

  it('rejects non-canonical input by default', () => {
    expectCode(() => tensorSpans(nonCanonical), ERR_NON_CANONICAL);
  });

  it('still accepts non-canonical input when strict:false is passed explicitly', () => {
    const spans = tensorSpans(nonCanonical, { strict: false });
    assert.deepStrictEqual(spans, []);
  });
});
