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

describe('Graph Types', () => {
  it('should round-trip node', () => {
    const original = {
      id: 42,
      label: 'Person',
      properties: {
        name: 'Alice',
        age: 30,
      },
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.id, 42);
    assert.strictEqual(decoded.label, 'Person');
    assert.strictEqual(decoded.properties.name, 'Alice');
    assert.strictEqual(decoded.properties.age, 30);
  });

  it('should round-trip edge', () => {
    const original = {
      src: 1,
      dst: 2,
      label: 'FOLLOWS',
      properties: {
        weight: 3,
      },
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.src, 1);
    assert.strictEqual(decoded.dst, 2);
    assert.strictEqual(decoded.label, 'FOLLOWS');
    assert.strictEqual(decoded.properties.weight, 3);
  });

  it('should round-trip adjlist', () => {
    const original = {
      id_width: 1,
      node_count: 2,
      edge_count: 3,
      row_offsets: [0, 1, 3],
      col_indices: [
        1, 0, 0, 0,
        0, 0, 0, 0,
        1, 0, 0, 0,
      ],
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.id_width, 1);
    assert.strictEqual(decoded.node_count, 2);
    assert.strictEqual(decoded.edge_count, 3);
    assert.deepStrictEqual(decoded.row_offsets, [0n, 1n, 3n]);
    assert.deepStrictEqual(decoded.col_indices, original.col_indices);
  });

  it('should round-trip node batch', () => {
    const original = {
      nodes: [
        { id: 1, label: 'A', properties: { x: 1 } },
        { id: 2, label: 'B', properties: { x: 2 } },
      ],
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.nodes.length, 2);
    assert.strictEqual(decoded.nodes[0].id, 1);
    assert.strictEqual(decoded.nodes[1].label, 'B');
  });

  it('should round-trip edge batch', () => {
    const original = {
      edges: [
        { src: 1, dst: 2, label: 'A', properties: { w: 1 } },
        { src: 2, dst: 3, label: 'B', properties: { w: 2 } },
      ],
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.edges.length, 2);
    assert.strictEqual(decoded.edges[0].src, 1);
    assert.strictEqual(decoded.edges[1].dst, 3);
  });

  it('should round-trip graph shard', () => {
    const original = {
      nodes: [
        { id: 1, label: 'Person', properties: { name: 'Alice' } },
        { id: 2, label: 'Person', properties: { name: 'Bob' } },
      ],
      edges: [
        { src: 1, dst: 2, label: 'KNOWS', properties: { since: 2020 } },
      ],
      meta: {
        shard: 's1',
      },
    };
    const decoded = decode(encode(original)) as any;
    assert.strictEqual(decoded.nodes.length, 2);
    assert.strictEqual(decoded.edges.length, 1);
    assert.strictEqual(decoded.meta.shard, 's1');
    assert.strictEqual(decoded.edges[0].properties.since, 2020);
  });
});
