/**
 * Gen1 codec tests
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { encode, decode } from './index.ts';

describe('Empty Containers', () => {
  it('should round-trip empty array', () => {
    const original: unknown[] = [];
    const encoded = encode(original);
    const decoded = decode(encoded);
    assert.deepStrictEqual(decoded, original);
  });

  it('should round-trip empty object', () => {
    const original = {};
    const encoded = encode(original);
    const decoded = decode(encoded);
    assert.deepStrictEqual(decoded, original);
  });

  it('should round-trip empty string', () => {
    const original = '';
    const encoded = encode(original);
    const decoded = decode(encoded);
    assert.strictEqual(decoded, original);
  });
});

describe('Primitives', () => {
  it('should round-trip null', () => {
    assert.strictEqual(decode(encode(null)), null);
  });

  it('should round-trip true', () => {
    assert.strictEqual(decode(encode(true)), true);
  });

  it('should round-trip false', () => {
    assert.strictEqual(decode(encode(false)), false);
  });

  it('should round-trip positive integer', () => {
    assert.strictEqual(decode(encode(42)), 42);
  });

  it('should round-trip negative integer', () => {
    assert.strictEqual(decode(encode(-42)), -42);
  });

  it('should round-trip zero', () => {
    assert.strictEqual(decode(encode(0)), 0);
  });

  it('should round-trip float', () => {
    const result = decode(encode(3.14159)) as number;
    assert.ok(Math.abs(result - 3.14159) < 0.0001);
  });

  it('should round-trip string', () => {
    assert.strictEqual(decode(encode('hello')), 'hello');
  });

  it('should round-trip unicode string', () => {
    assert.strictEqual(decode(encode('hello 世界 🌍')), 'hello 世界 🌍');
  });
});

describe('Containers', () => {
  it('should round-trip simple array', () => {
    const original = [1, 2, 3];
    assert.deepStrictEqual(decode(encode(original)), original);
  });

  it('should round-trip simple object', () => {
    const original = { name: 'Alice', age: 30 };
    const decoded = decode(encode(original)) as Record<string, unknown>;
    assert.strictEqual(decoded.name, 'Alice');
    assert.strictEqual(decoded.age, 30);
  });

  it('should round-trip nested object', () => {
    const original = { user: { name: 'Bob', address: { city: 'NYC' } } };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.user.address.city, 'NYC');
  });
});

describe('Proto-Tensors', () => {
  it('should round-trip homogeneous int array', () => {
    const original = [1, 2, 3, 4, 5, 6, 7, 8];
    const decoded = decode(encode(original)) as number[];
    assert.deepStrictEqual(decoded, original);
  });

  it('should round-trip homogeneous float array', () => {
    const original = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    const decoded = decode(encode(original)) as number[];
    for (let i = 0; i < original.length; i++) {
      assert.ok(Math.abs(decoded[i] - original[i]) < 0.0001, `Mismatch at index ${i}`);
    }
  });
});

describe('BigInt Support', () => {
  it('should round-trip BigInt', () => {
    const original = 9007199254740993n; // Beyond Number.MAX_SAFE_INTEGER
    const decoded = decode(encode(original));
    assert.strictEqual(decoded, original);
  });

  it('should round-trip negative BigInt', () => {
    const original = -9007199254740993n;
    const decoded = decode(encode(original));
    assert.strictEqual(decoded, original);
  });

  it('should return number for safe integers', () => {
    const original = 42n;
    const decoded = decode(encode(original));
    assert.strictEqual(decoded, 42);
    assert.strictEqual(typeof decoded, 'number');
  });
});
