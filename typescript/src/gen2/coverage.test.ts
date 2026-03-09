/**
 * Additional gen2 coverage tests targeting uncovered encode/decode paths,
 * JSON bridge, tensor views, schema fingerprinting, master stream edge cases,
 * and error paths.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  SJ,
  Value,
  Type,
  encode,
  decode,
  encodeWithOpts,
  fromAny,
  fromJSON,
  toAny,
  toJSON,
  dumps,
  loads,
  schemaFingerprint32,
  schemaFingerprint64,
  schemaEquals,
  writeMasterFrame,
  readMasterFrame,
  isMasterStream,
  isCowrieDocument,
  crc32,
  encodeFramed,
  decodeFramed,
  Compression,
  DType,
  ImageFormat,
  AudioEncoding,
  IDWidth,
  DeltaOpCode,
  SecurityLimitExceeded,
  UnknownExtBehavior,
  tensorViewFloat32,
  tensorViewFloat64,
  tensorViewInt32,
  tensorCopyFloat32,
  tensorCopyFloat64,
  tensorCopyInt32,
  tensorFromFloat32,
} from './index.ts';
import type {
  TensorData,
  ImageData,
  AudioData,
  TensorRefData,
  AdjlistData,
  RichTextData,
  DeltaData,
  DeltaOp,
  NodeData,
  EdgeData,
  NodeBatchData,
  EdgeBatchData,
  GraphShardData,
  BitmaskData,
  Decimal128,
  UnknownExtData,
} from './index.ts';

// Helper: roundtrip encode then decode
function roundtrip(v: Value): Value {
  return decode(encode(v));
}

// ============================================================
// Primitives roundtrip
// ============================================================

describe('gen2 encode/decode roundtrip: primitives', () => {
  it('null', () => {
    const v = roundtrip(SJ.null());
    assert.strictEqual(v.type, Type.NULL);
  });

  it('bool true', () => {
    const v = roundtrip(SJ.bool(true));
    assert.strictEqual(v.type, Type.BOOL);
    assert.strictEqual(v.data, true);
  });

  it('bool false', () => {
    const v = roundtrip(SJ.bool(false));
    assert.strictEqual(v.type, Type.BOOL);
    assert.strictEqual(v.data, false);
  });

  it('int64 positive (small, FIXINT range)', () => {
    const v = roundtrip(SJ.int64(42));
    assert.strictEqual(v.type, Type.INT64);
    assert.strictEqual(v.data, 42n);
  });

  it('int64 positive (FIXINT boundary 127)', () => {
    const v = roundtrip(SJ.int64(127));
    assert.strictEqual(v.data, 127n);
  });

  it('int64 positive (above FIXINT, 128)', () => {
    const v = roundtrip(SJ.int64(128));
    assert.strictEqual(v.data, 128n);
  });

  it('int64 negative (FIXNEG range -1 to -16)', () => {
    for (const n of [-1, -5, -16]) {
      const v = roundtrip(SJ.int64(n));
      assert.strictEqual(v.data, BigInt(n), `FIXNEG for ${n}`);
    }
  });

  it('int64 negative (below FIXNEG, -17)', () => {
    const v = roundtrip(SJ.int64(-17));
    assert.strictEqual(v.data, -17n);
  });

  it('int64 large positive', () => {
    const v = roundtrip(SJ.int64(9223372036854775807n));
    assert.strictEqual(v.data, 9223372036854775807n);
  });

  it('int64 large negative', () => {
    const v = roundtrip(SJ.int64(-9223372036854775808n));
    assert.strictEqual(v.data, -9223372036854775808n);
  });

  it('int64 zero (FIXINT)', () => {
    const v = roundtrip(SJ.int64(0));
    assert.strictEqual(v.data, 0n);
  });

  it('uint64', () => {
    const v = roundtrip(SJ.uint64(18446744073709551615n));
    assert.strictEqual(v.type, Type.UINT64);
    assert.strictEqual(v.data, 18446744073709551615n);
  });

  it('uint64 zero', () => {
    const v = roundtrip(SJ.uint64(0));
    assert.strictEqual(v.type, Type.UINT64);
    assert.strictEqual(v.data, 0n);
  });

  it('uint64 out of range throws', () => {
    assert.throws(() => SJ.uint64(-1), /out of range/);
  });

  it('float64', () => {
    const v = roundtrip(SJ.float64(3.14));
    assert.strictEqual(v.type, Type.FLOAT64);
    assert.ok(Math.abs((v.data as number) - 3.14) < 1e-10);
  });

  it('float64 special values', () => {
    // NaN
    const nan = roundtrip(SJ.float64(NaN));
    assert.ok(isNaN(nan.data as number));
    // Infinity
    const inf = roundtrip(SJ.float64(Infinity));
    assert.strictEqual(inf.data, Infinity);
    // -Infinity
    const ninf = roundtrip(SJ.float64(-Infinity));
    assert.strictEqual(ninf.data, -Infinity);
  });

  it('string', () => {
    const v = roundtrip(SJ.string('hello world'));
    assert.strictEqual(v.type, Type.STRING);
    assert.strictEqual(v.data, 'hello world');
  });

  it('string empty', () => {
    const v = roundtrip(SJ.string(''));
    assert.strictEqual(v.data, '');
  });

  it('string unicode', () => {
    const v = roundtrip(SJ.string('你好世界'));
    assert.strictEqual(v.data, '你好世界');
  });

  it('bytes', () => {
    const original = new Uint8Array([0x00, 0x01, 0x02, 0xff]);
    const v = roundtrip(SJ.bytes(original));
    assert.strictEqual(v.type, Type.BYTES);
    assert.deepStrictEqual(v.data, original);
  });

  it('bytes empty', () => {
    const v = roundtrip(SJ.bytes(new Uint8Array(0)));
    assert.strictEqual((v.data as Uint8Array).length, 0);
  });

  it('datetime64', () => {
    const nanos = 1700000000000000000n;
    const v = roundtrip(SJ.datetime64(nanos));
    assert.strictEqual(v.type, Type.DATETIME64);
    assert.strictEqual(v.data, nanos);
  });

  it('datetime from Date', () => {
    const d = new Date('2025-01-01T00:00:00Z');
    const v = roundtrip(SJ.datetime(d));
    assert.strictEqual(v.type, Type.DATETIME64);
    // Should roundtrip to correct nanos
    const expectedNanos = BigInt(d.getTime()) * 1000000n;
    assert.strictEqual(v.data, expectedNanos);
  });

  it('uuid128 from string', () => {
    const uuid = '550e8400-e29b-41d4-a716-446655440000';
    const v = roundtrip(SJ.uuid128(uuid));
    assert.strictEqual(v.type, Type.UUID128);
    assert.strictEqual((v.data as Uint8Array).length, 16);
  });

  it('uuid128 from bytes', () => {
    const bytes = new Uint8Array(16);
    bytes[0] = 0x55; bytes[15] = 0xaa;
    const v = roundtrip(SJ.uuid128(bytes));
    assert.strictEqual((v.data as Uint8Array)[0], 0x55);
    assert.strictEqual((v.data as Uint8Array)[15], 0xaa);
  });

  it('decimal128', () => {
    const coef = new Uint8Array(16);
    coef[15] = 42; // coefficient = 42
    const v = roundtrip(SJ.decimal128(2, coef));
    assert.strictEqual(v.type, Type.DECIMAL128);
    const d = v.data as Decimal128;
    assert.strictEqual(d.scale, 2);
    assert.deepStrictEqual(d.coef, coef);
  });

  it('decimal128 negative scale (stored as unsigned byte)', () => {
    const coef = new Uint8Array(16);
    coef[15] = 1;
    // Scale -5 encoded as 251 (0xfb), decoded back to -5
    const v = roundtrip(SJ.decimal128(-5, coef));
    const d = v.data as Decimal128;
    assert.strictEqual(d.scale, -5);
  });

  it('bigint from bigint value', () => {
    const v = roundtrip(SJ.bigint(123456789012345678901234567890n));
    assert.strictEqual(v.type, Type.BIGINT);
  });

  it('bigint zero', () => {
    const v = roundtrip(SJ.bigint(0n));
    assert.strictEqual(v.type, Type.BIGINT);
  });

  it('bigint negative', () => {
    const v = roundtrip(SJ.bigint(-42n));
    assert.strictEqual(v.type, Type.BIGINT);
  });

  it('bigint from bytes', () => {
    const bytes = new Uint8Array([0x00, 0x01, 0x02]);
    const v = roundtrip(SJ.bigint(bytes));
    assert.strictEqual(v.type, Type.BIGINT);
    assert.deepStrictEqual(v.data, bytes);
  });
});

// ============================================================
// Containers roundtrip
// ============================================================

describe('gen2 encode/decode roundtrip: containers', () => {
  it('empty array', () => {
    const v = roundtrip(SJ.array([]));
    assert.strictEqual(v.type, Type.ARRAY);
    assert.strictEqual((v.data as Value[]).length, 0);
  });

  it('array with mixed types', () => {
    const v = roundtrip(SJ.array([
      SJ.null(),
      SJ.bool(true),
      SJ.int64(42),
      SJ.string('hello'),
      SJ.float64(3.14),
    ]));
    const arr = v.data as Value[];
    assert.strictEqual(arr.length, 5);
    assert.strictEqual(arr[0].type, Type.NULL);
    assert.strictEqual(arr[1].data, true);
    assert.strictEqual(arr[2].data, 42n);
    assert.strictEqual(arr[3].data, 'hello');
  });

  it('array with >15 elements (non-FIXARRAY)', () => {
    const items: Value[] = [];
    for (let i = 0; i < 20; i++) {
      items.push(SJ.int64(i));
    }
    const v = roundtrip(SJ.array(items));
    const arr = v.data as Value[];
    assert.strictEqual(arr.length, 20);
    assert.strictEqual(arr[19].data, 19n);
  });

  it('empty object', () => {
    const v = roundtrip(SJ.object({}));
    assert.strictEqual(v.type, Type.OBJECT);
    assert.strictEqual(Object.keys(v.data as Record<string, Value>).length, 0);
  });

  it('object with multiple fields', () => {
    const v = roundtrip(SJ.object({
      name: SJ.string('Alice'),
      age: SJ.int64(30),
      active: SJ.bool(true),
    }));
    const obj = v.data as Record<string, Value>;
    assert.strictEqual(obj['name'].data, 'Alice');
    assert.strictEqual(obj['age'].data, 30n);
    assert.strictEqual(obj['active'].data, true);
  });

  it('object with >15 fields (non-FIXMAP)', () => {
    const members: Record<string, Value> = {};
    for (let i = 0; i < 20; i++) {
      members[`field_${i}`] = SJ.int64(i);
    }
    const v = roundtrip(SJ.object(members));
    const obj = v.data as Record<string, Value>;
    assert.strictEqual(Object.keys(obj).length, 20);
  });

  it('nested containers', () => {
    const v = roundtrip(SJ.object({
      nested: SJ.object({
        arr: SJ.array([SJ.int64(1), SJ.int64(2)]),
      }),
    }));
    const outer = v.data as Record<string, Value>;
    const inner = outer['nested'].data as Record<string, Value>;
    const arr = inner['arr'].data as Value[];
    assert.strictEqual(arr.length, 2);
  });
});

// ============================================================
// ML/Multimodal types roundtrip
// ============================================================

describe('gen2 encode/decode roundtrip: ML types', () => {
  it('tensor float32', () => {
    const tensorData = new Uint8Array(new Float32Array([1.0, 2.0, 3.0, 4.0]).buffer);
    const v = roundtrip(SJ.tensor(DType.FLOAT32, [2, 2], tensorData));
    assert.strictEqual(v.type, Type.TENSOR);
    const t = v.data as TensorData;
    assert.strictEqual(t.dtype, DType.FLOAT32);
    assert.deepStrictEqual(t.shape, [2, 2]);
    assert.strictEqual(t.data.length, 16);
  });

  it('tensor int8', () => {
    const tensorData = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const v = roundtrip(SJ.tensor(DType.INT8, [2, 3], tensorData));
    const t = v.data as TensorData;
    assert.strictEqual(t.dtype, DType.INT8);
    assert.deepStrictEqual(t.shape, [2, 3]);
  });

  it('image JPEG', () => {
    const imageBytes = new Uint8Array([0xff, 0xd8, 0xff, 0xe0]);
    const v = roundtrip(SJ.image(ImageFormat.JPEG, 640, 480, imageBytes));
    assert.strictEqual(v.type, Type.IMAGE);
    const img = v.data as ImageData;
    assert.strictEqual(img.format, ImageFormat.JPEG);
    assert.strictEqual(img.width, 640);
    assert.strictEqual(img.height, 480);
    assert.deepStrictEqual(img.data, imageBytes);
  });

  it('image PNG', () => {
    const imageBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);
    const v = roundtrip(SJ.image(ImageFormat.PNG, 100, 200, imageBytes));
    const img = v.data as ImageData;
    assert.strictEqual(img.format, ImageFormat.PNG);
    assert.strictEqual(img.width, 100);
    assert.strictEqual(img.height, 200);
  });

  it('audio PCM_INT16', () => {
    const audioBytes = new Uint8Array(100);
    const v = roundtrip(SJ.audio(AudioEncoding.PCM_INT16, 44100, 2, audioBytes));
    assert.strictEqual(v.type, Type.AUDIO);
    const aud = v.data as AudioData;
    assert.strictEqual(aud.encoding, AudioEncoding.PCM_INT16);
    assert.strictEqual(aud.sampleRate, 44100);
    assert.strictEqual(aud.channels, 2);
    assert.strictEqual(aud.data.length, 100);
  });

  it('audio OPUS', () => {
    const audioBytes = new Uint8Array(50);
    const v = roundtrip(SJ.audio(AudioEncoding.OPUS, 48000, 1, audioBytes));
    const aud = v.data as AudioData;
    assert.strictEqual(aud.encoding, AudioEncoding.OPUS);
    assert.strictEqual(aud.sampleRate, 48000);
    assert.strictEqual(aud.channels, 1);
  });

  it('tensor_ref', () => {
    const key = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
    const v = roundtrip(SJ.tensorRef(5, key));
    assert.strictEqual(v.type, Type.TENSOR_REF);
    const ref = v.data as TensorRefData;
    assert.strictEqual(ref.storeId, 5);
    assert.deepStrictEqual(ref.key, key);
  });

  it('adjlist int32', () => {
    const colIndices = new Uint8Array(new Int32Array([1, 0, 2]).buffer);
    const v = roundtrip(SJ.adjlist(IDWidth.INT32, 3, 3, [0, 1, 2, 3], colIndices));
    assert.strictEqual(v.type, Type.ADJLIST);
    const adj = v.data as AdjlistData;
    assert.strictEqual(adj.idWidth, IDWidth.INT32);
    assert.strictEqual(adj.nodeCount, 3);
    assert.strictEqual(adj.edgeCount, 3);
    assert.deepStrictEqual(adj.rowOffsets, [0, 1, 2, 3]);
  });

  it('adjlist int64', () => {
    const colIndices = new Uint8Array(new BigInt64Array([1n, 2n]).buffer);
    const v = roundtrip(SJ.adjlist(IDWidth.INT64, 2, 2, [0, 1, 2], colIndices));
    const adj = v.data as AdjlistData;
    assert.strictEqual(adj.idWidth, IDWidth.INT64);
    assert.strictEqual(adj.nodeCount, 2);
    assert.strictEqual(adj.edgeCount, 2);
  });

  it('richtext plain', () => {
    const v = roundtrip(SJ.richtext('hello world'));
    assert.strictEqual(v.type, Type.RICHTEXT);
    const rt = v.data as RichTextData;
    assert.strictEqual(rt.text, 'hello world');
    assert.strictEqual(rt.tokens, undefined);
    assert.strictEqual(rt.spans, undefined);
  });

  it('richtext with tokens', () => {
    const v = roundtrip(SJ.richtext('hello world', [101, 102, 103]));
    const rt = v.data as RichTextData;
    assert.strictEqual(rt.text, 'hello world');
    assert.deepStrictEqual(rt.tokens, [101, 102, 103]);
  });

  it('richtext with spans', () => {
    const v = roundtrip(SJ.richtext('hello world', undefined, [
      { start: 0, end: 5, kindId: 1 },
      { start: 6, end: 11, kindId: 2 },
    ]));
    const rt = v.data as RichTextData;
    assert.strictEqual(rt.spans!.length, 2);
    assert.strictEqual(rt.spans![0].start, 0);
    assert.strictEqual(rt.spans![0].end, 5);
    assert.strictEqual(rt.spans![0].kindId, 1);
  });

  it('richtext with tokens and spans', () => {
    const v = roundtrip(SJ.richtext('hello', [1, 2], [
      { start: 0, end: 5, kindId: 3 },
    ]));
    const rt = v.data as RichTextData;
    assert.deepStrictEqual(rt.tokens, [1, 2]);
    assert.strictEqual(rt.spans!.length, 1);
  });

  it('delta with SET_FIELD', () => {
    const ops: DeltaOp[] = [
      { opCode: DeltaOpCode.SET_FIELD, fieldId: 0, value: SJ.string('new_value') },
    ];
    const v = roundtrip(SJ.delta(42, ops));
    assert.strictEqual(v.type, Type.DELTA);
    const delta = v.data as DeltaData;
    assert.strictEqual(delta.baseId, 42);
    assert.strictEqual(delta.ops.length, 1);
    assert.strictEqual(delta.ops[0].opCode, DeltaOpCode.SET_FIELD);
  });

  it('delta with DELETE_FIELD', () => {
    const ops: DeltaOp[] = [
      { opCode: DeltaOpCode.DELETE_FIELD, fieldId: 1 },
    ];
    const v = roundtrip(SJ.delta(0, ops));
    const delta = v.data as DeltaData;
    assert.strictEqual(delta.ops[0].opCode, DeltaOpCode.DELETE_FIELD);
    assert.strictEqual(delta.ops[0].value, undefined);
  });

  it('delta with APPEND_ARRAY', () => {
    const ops: DeltaOp[] = [
      { opCode: DeltaOpCode.APPEND_ARRAY, fieldId: 2, value: SJ.int64(99) },
    ];
    const v = roundtrip(SJ.delta(1, ops));
    const delta = v.data as DeltaData;
    assert.strictEqual(delta.ops[0].opCode, DeltaOpCode.APPEND_ARRAY);
  });

  it('bitmask', () => {
    const bits = new Uint8Array([0b10110101]);
    const v = roundtrip(SJ.bitmask(8, bits));
    assert.strictEqual(v.type, Type.BITMASK);
    const bm = v.data as BitmaskData;
    assert.strictEqual(bm.count, 8);
    assert.deepStrictEqual(bm.bits, bits);
  });

  it('bitmask empty', () => {
    const v = roundtrip(SJ.bitmask(0, new Uint8Array(0)));
    const bm = v.data as BitmaskData;
    assert.strictEqual(bm.count, 0);
  });

  it('bitmaskFromBools', () => {
    const v = roundtrip(SJ.bitmaskFromBools([true, false, true, true, false, false, true, false, true]));
    const bm = v.data as BitmaskData;
    assert.strictEqual(bm.count, 9);
    assert.strictEqual(bm.bits.length, 2); // ceil(9/8)
  });

  it('unknown ext roundtrip', () => {
    const payload = new Uint8Array([0xaa, 0xbb, 0xcc]);
    const v = roundtrip(SJ.unknownExt(42, payload));
    assert.strictEqual(v.type, Type.UNKNOWN_EXT);
    const ext = v.data as UnknownExtData;
    assert.strictEqual(ext.extType, 42n);
    assert.deepStrictEqual(ext.payload, payload);
  });
});

// ============================================================
// Graph types roundtrip
// ============================================================

describe('gen2 encode/decode roundtrip: graph types', () => {
  it('node', () => {
    const v = roundtrip(SJ.node('n1', ['Person', 'User'], {
      name: SJ.string('Alice'),
      age: SJ.int64(30),
    }));
    assert.strictEqual(v.type, Type.NODE);
    const node = v.data as NodeData;
    assert.strictEqual(node.id, 'n1');
    assert.deepStrictEqual(node.labels, ['Person', 'User']);
    assert.strictEqual((node.props['name'] as Value).data, 'Alice');
  });

  it('edge', () => {
    const v = roundtrip(SJ.edge('n1', 'n2', 'FOLLOWS', {
      since: SJ.int64(2020),
    }));
    assert.strictEqual(v.type, Type.EDGE);
    const edge = v.data as EdgeData;
    assert.strictEqual(edge.fromId, 'n1');
    assert.strictEqual(edge.toId, 'n2');
    assert.strictEqual(edge.edgeType, 'FOLLOWS');
  });

  it('node batch', () => {
    const nodes: NodeData[] = [
      { id: 'n1', labels: ['A'], props: { x: SJ.int64(1) } },
      { id: 'n2', labels: ['B'], props: { x: SJ.int64(2) } },
    ];
    const v = roundtrip(SJ.nodeBatch(nodes));
    assert.strictEqual(v.type, Type.NODE_BATCH);
    const batch = v.data as NodeBatchData;
    assert.strictEqual(batch.nodes.length, 2);
    assert.strictEqual(batch.nodes[0].id, 'n1');
    assert.strictEqual(batch.nodes[1].id, 'n2');
  });

  it('edge batch', () => {
    const edges: EdgeData[] = [
      { fromId: 'a', toId: 'b', edgeType: 'X', props: { w: SJ.int64(1) } },
      { fromId: 'b', toId: 'c', edgeType: 'Y', props: { w: SJ.int64(2) } },
    ];
    const v = roundtrip(SJ.edgeBatch(edges));
    assert.strictEqual(v.type, Type.EDGE_BATCH);
    const batch = v.data as EdgeBatchData;
    assert.strictEqual(batch.edges.length, 2);
  });

  it('graph shard', () => {
    const nodes: NodeData[] = [
      { id: 'n1', labels: ['Person'], props: { name: SJ.string('Alice') } },
    ];
    const edges: EdgeData[] = [
      { fromId: 'n1', toId: 'n2', edgeType: 'KNOWS', props: {} },
    ];
    const meta: Record<string, Value> = { shard_id: SJ.string('s1') };
    const v = roundtrip(SJ.graphShard(nodes, edges, meta));
    assert.strictEqual(v.type, Type.GRAPH_SHARD);
    const shard = v.data as GraphShardData;
    assert.strictEqual(shard.nodes.length, 1);
    assert.strictEqual(shard.edges.length, 1);
    assert.strictEqual((shard.metadata['shard_id'] as Value).data, 's1');
  });
});

// ============================================================
// JSON Bridge
// ============================================================

describe('gen2 JSON bridge', () => {
  it('fromAny null/undefined', () => {
    assert.strictEqual(fromAny(null).type, Type.NULL);
    assert.strictEqual(fromAny(undefined).type, Type.NULL);
  });

  it('fromAny boolean', () => {
    assert.strictEqual(fromAny(true).data, true);
    assert.strictEqual(fromAny(false).data, false);
  });

  it('fromAny integer', () => {
    const v = fromAny(42);
    assert.strictEqual(v.type, Type.INT64);
    assert.strictEqual(v.data, 42n);
  });

  it('fromAny float', () => {
    const v = fromAny(3.14);
    assert.strictEqual(v.type, Type.FLOAT64);
    assert.strictEqual(v.data, 3.14);
  });

  it('fromAny bigint positive', () => {
    const v = fromAny(42n);
    assert.strictEqual(v.type, Type.UINT64);
    assert.strictEqual(v.data, 42n);
  });

  it('fromAny bigint negative', () => {
    const v = fromAny(-42n);
    assert.strictEqual(v.type, Type.INT64);
    assert.strictEqual(v.data, -42n);
  });

  it('fromAny string (plain)', () => {
    const v = fromAny('hello');
    assert.strictEqual(v.type, Type.STRING);
    assert.strictEqual(v.data, 'hello');
  });

  it('fromAny string (ISO8601 date)', () => {
    const v = fromAny('2025-01-01T00:00:00Z');
    assert.strictEqual(v.type, Type.DATETIME64);
  });

  it('fromAny string (UUID)', () => {
    const v = fromAny('550e8400-e29b-41d4-a716-446655440000');
    assert.strictEqual(v.type, Type.UUID128);
  });

  it('fromAny string with date field name hint', () => {
    const v = fromAny('2025-01-01T00:00:00Z', 'created_at');
    assert.strictEqual(v.type, Type.DATETIME64);
  });

  it('fromAny string with time field name hint', () => {
    const v = fromAny('2025-06-15T12:30:00Z', 'start_time');
    assert.strictEqual(v.type, Type.DATETIME64);
  });

  it('fromAny Uint8Array', () => {
    const v = fromAny(new Uint8Array([1, 2, 3]));
    assert.strictEqual(v.type, Type.BYTES);
  });

  it('fromAny Date', () => {
    const v = fromAny(new Date('2025-01-01T00:00:00Z'));
    assert.strictEqual(v.type, Type.DATETIME64);
  });

  it('fromAny array', () => {
    const v = fromAny([1, 'two', true]);
    assert.strictEqual(v.type, Type.ARRAY);
    assert.strictEqual((v.data as Value[]).length, 3);
  });

  it('fromAny object', () => {
    const v = fromAny({ name: 'Alice', age: 30 });
    assert.strictEqual(v.type, Type.OBJECT);
    const obj = v.data as Record<string, Value>;
    assert.strictEqual(obj['name'].data, 'Alice');
    assert.strictEqual(obj['age'].data, 30n);
  });

  it('fromAny unknown_ext object', () => {
    const v = fromAny({
      _type: 'unknown_ext',
      ext_type: 42,
      payload: btoa(String.fromCharCode(0xaa, 0xbb)),
    });
    assert.strictEqual(v.type, Type.UNKNOWN_EXT);
    const ext = v.data as UnknownExtData;
    assert.strictEqual(ext.extType, 42n);
  });

  it('fromAny ext object (alternate _type)', () => {
    const v = fromAny({
      _type: 'ext',
      ext_type: '100',
      payload: btoa('test'),
    });
    assert.strictEqual(v.type, Type.UNKNOWN_EXT);
    const ext = v.data as UnknownExtData;
    assert.strictEqual(ext.extType, 100n);
  });

  it('fromJSON', () => {
    const v = fromJSON('{"name":"Alice","age":30}');
    assert.strictEqual(v.type, Type.OBJECT);
    const obj = v.data as Record<string, Value>;
    assert.strictEqual(obj['name'].data, 'Alice');
  });

  it('toAny null', () => {
    assert.strictEqual(toAny(SJ.null()), null);
  });

  it('toAny bool', () => {
    assert.strictEqual(toAny(SJ.bool(true)), true);
  });

  it('toAny int64 safe', () => {
    assert.strictEqual(toAny(SJ.int64(42)), 42);
  });

  it('toAny int64 unsafe (returns string)', () => {
    const result = toAny(SJ.int64(9007199254740992n));
    assert.strictEqual(result, '9007199254740992');
  });

  it('toAny uint64 safe', () => {
    assert.strictEqual(toAny(SJ.uint64(42)), 42);
  });

  it('toAny uint64 unsafe (returns string)', () => {
    const result = toAny(SJ.uint64(18446744073709551615n));
    assert.strictEqual(result, '18446744073709551615');
  });

  it('toAny float64', () => {
    assert.strictEqual(toAny(SJ.float64(3.14)), 3.14);
  });

  it('toAny string', () => {
    assert.strictEqual(toAny(SJ.string('hello')), 'hello');
  });

  it('toAny bytes (base64)', () => {
    const result = toAny(SJ.bytes(new Uint8Array([0x01, 0x02, 0x03])));
    assert.strictEqual(typeof result, 'string');
    // Base64 of [1,2,3]
    assert.strictEqual(result, btoa(String.fromCharCode(1, 2, 3)));
  });

  it('toAny datetime64', () => {
    const nanos = BigInt(new Date('2025-01-01T00:00:00Z').getTime()) * 1000000n;
    const result = toAny(SJ.datetime64(nanos));
    assert.strictEqual(typeof result, 'string');
    assert.ok((result as string).includes('2025'));
  });

  it('toAny uuid128', () => {
    const result = toAny(SJ.uuid128('550e8400-e29b-41d4-a716-446655440000'));
    assert.strictEqual(result, '550e8400-e29b-41d4-a716-446655440000');
  });

  it('toAny bigint', () => {
    const result = toAny(SJ.bigint(12345n));
    assert.strictEqual(result, '12345');
  });

  it('toAny decimal128', () => {
    const coef = new Uint8Array(16);
    coef[15] = 42;
    const result = toAny(SJ.decimal128(1, coef));
    assert.strictEqual(typeof result, 'string');
    assert.ok((result as string).includes('4'));
  });

  it('toAny decimal128 scale=0', () => {
    const coef = new Uint8Array(16);
    coef[15] = 5;
    const result = toAny(SJ.decimal128(0, coef));
    assert.strictEqual(result, '5');
  });

  it('toAny decimal128 large scale', () => {
    const coef = new Uint8Array(16);
    coef[15] = 1;
    // scale >= str.length
    const result = toAny(SJ.decimal128(3, coef));
    assert.strictEqual(result, '0.001');
  });

  it('toAny decimal128 negative scale', () => {
    const coef = new Uint8Array(16);
    coef[15] = 5;
    const result = toAny(SJ.decimal128(-2, coef));
    assert.strictEqual(result, '500');
  });

  it('toAny array', () => {
    const result = toAny(SJ.array([SJ.int64(1), SJ.int64(2)]));
    assert.deepStrictEqual(result, [1, 2]);
  });

  it('toAny object', () => {
    const result = toAny(SJ.object({ x: SJ.int64(1) }));
    assert.deepStrictEqual(result, { x: 1 });
  });

  it('toAny tensor', () => {
    const tensorData = new Uint8Array(8);
    const result = toAny(SJ.tensor(DType.FLOAT32, [2], tensorData)) as Record<string, unknown>;
    assert.strictEqual(result._type, 'tensor');
    assert.strictEqual(result.dtype, 'float32');
    assert.deepStrictEqual(result.shape, [2]);
  });

  it('toAny image', () => {
    const result = toAny(SJ.image(ImageFormat.PNG, 100, 200, new Uint8Array(4))) as Record<string, unknown>;
    assert.strictEqual(result._type, 'image');
    assert.strictEqual(result.format, 'png');
    assert.strictEqual(result.width, 100);
    assert.strictEqual(result.height, 200);
  });

  it('toAny audio', () => {
    const result = toAny(SJ.audio(AudioEncoding.PCM_INT16, 44100, 2, new Uint8Array(4))) as Record<string, unknown>;
    assert.strictEqual(result._type, 'audio');
    assert.strictEqual(result.encoding, 'pcm_int16');
    assert.strictEqual(result.sampleRate, 44100);
    assert.strictEqual(result.channels, 2);
  });

  it('toAny tensor_ref', () => {
    const result = toAny(SJ.tensorRef(1, new Uint8Array([0x01, 0x02]))) as Record<string, unknown>;
    assert.strictEqual(result._type, 'tensor_ref');
    assert.strictEqual(result.storeId, 1);
  });

  it('toAny adjlist', () => {
    const colIndices = new Uint8Array(new Int32Array([1]).buffer);
    const result = toAny(SJ.adjlist(IDWidth.INT32, 2, 1, [0, 1, 1], colIndices)) as Record<string, unknown>;
    assert.strictEqual(result._type, 'adjlist');
    assert.strictEqual(result.idWidth, 'int32');
    assert.strictEqual(result.nodeCount, 2);
    assert.strictEqual(result.edgeCount, 1);
  });

  it('toAny adjlist int64', () => {
    const colIndices = new Uint8Array(new BigInt64Array([1n]).buffer);
    const result = toAny(SJ.adjlist(IDWidth.INT64, 2, 1, [0, 0, 1], colIndices)) as Record<string, unknown>;
    assert.strictEqual(result.idWidth, 'int64');
  });

  it('toAny richtext plain', () => {
    const result = toAny(SJ.richtext('hello')) as Record<string, unknown>;
    assert.strictEqual(result._type, 'richtext');
    assert.strictEqual(result.text, 'hello');
    assert.strictEqual(result.tokens, undefined);
    assert.strictEqual(result.spans, undefined);
  });

  it('toAny richtext with tokens and spans', () => {
    const result = toAny(SJ.richtext('hello', [1, 2], [
      { start: 0, end: 5, kindId: 1 },
    ])) as Record<string, unknown>;
    assert.deepStrictEqual(result.tokens, [1, 2]);
    assert.strictEqual((result.spans as any[]).length, 1);
  });

  it('toAny delta', () => {
    const result = toAny(SJ.delta(1, [
      { opCode: DeltaOpCode.SET_FIELD, fieldId: 0, value: SJ.string('v') },
      { opCode: DeltaOpCode.DELETE_FIELD, fieldId: 1 },
    ])) as Record<string, unknown>;
    assert.strictEqual(result._type, 'delta');
    assert.strictEqual(result.baseId, 1);
    assert.strictEqual((result.ops as any[]).length, 2);
  });

  it('toAny node', () => {
    const result = toAny(SJ.node('n1', ['Person'], { name: SJ.string('Alice') })) as Record<string, unknown>;
    assert.strictEqual(result._type, 'node');
    assert.strictEqual(result.id, 'n1');
    assert.deepStrictEqual(result.labels, ['Person']);
  });

  it('toAny edge', () => {
    const result = toAny(SJ.edge('a', 'b', 'KNOWS', { w: SJ.int64(1) })) as Record<string, unknown>;
    assert.strictEqual(result._type, 'edge');
    assert.strictEqual(result.from, 'a');
    assert.strictEqual(result.to, 'b');
    assert.strictEqual(result.type, 'KNOWS');
  });

  it('toAny node_batch', () => {
    const nodes: NodeData[] = [
      { id: 'n1', labels: ['A'], props: { x: SJ.int64(1) } },
    ];
    const result = toAny(SJ.nodeBatch(nodes)) as Record<string, unknown>;
    assert.strictEqual(result._type, 'node_batch');
    assert.strictEqual((result.nodes as any[]).length, 1);
  });

  it('toAny edge_batch', () => {
    const edges: EdgeData[] = [
      { fromId: 'a', toId: 'b', edgeType: 'X', props: {} },
    ];
    const result = toAny(SJ.edgeBatch(edges)) as Record<string, unknown>;
    assert.strictEqual(result._type, 'edge_batch');
    assert.strictEqual((result.edges as any[]).length, 1);
  });

  it('toAny graph_shard', () => {
    const result = toAny(SJ.graphShard(
      [{ id: 'n1', labels: ['A'], props: { x: SJ.int64(1) } }],
      [{ fromId: 'n1', toId: 'n2', edgeType: 'E', props: { w: SJ.int64(2) } }],
      { key: SJ.string('val') },
    )) as Record<string, unknown>;
    assert.strictEqual(result._type, 'graph_shard');
  });

  it('toAny bitmask', () => {
    const result = toAny(SJ.bitmask(8, new Uint8Array([0b10110101]))) as Record<string, unknown>;
    assert.strictEqual(result._type, 'bitmask');
    assert.strictEqual(result.count, 8);
    assert.deepStrictEqual(result.bits, [true, false, true, false, true, true, false, true]);
  });

  it('toAny unknown_ext', () => {
    const result = toAny(SJ.unknownExt(42, new Uint8Array([1, 2]))) as Record<string, unknown>;
    assert.strictEqual(result._type, 'ext');
    assert.strictEqual(result.ext_type, 42);
  });

  it('toAny unknown_ext large extType (returns string)', () => {
    const result = toAny(SJ.unknownExt(9007199254740992n, new Uint8Array([1]))) as Record<string, unknown>;
    assert.strictEqual(result.ext_type, '9007199254740992');
  });

  it('toJSON and fromJSON roundtrip', () => {
    const v = SJ.object({ name: SJ.string('test'), count: SJ.int64(42) });
    const json = toJSON(v);
    const parsed = JSON.parse(json);
    assert.strictEqual(parsed.name, 'test');
    assert.strictEqual(parsed.count, 42);
  });

  it('dumps and loads', () => {
    const obj = { name: 'Alice', age: 30, active: true };
    const data = dumps(obj);
    const result = loads(data) as Record<string, unknown>;
    assert.strictEqual(result.name, 'Alice');
    assert.strictEqual(result.age, 30);
    assert.strictEqual(result.active, true);
  });

  it('fromAny with non-standard type falls back to string', () => {
    // Symbol or other weird types should stringify
    const v = fromAny('just a string', 'unknown_field');
    assert.strictEqual(v.type, Type.STRING);
  });
});

// ============================================================
// Deterministic encoding
// ============================================================

describe('gen2 deterministic encoding', () => {
  it('encodeWithOpts non-deterministic falls through to normal encode', () => {
    const v = SJ.object({ z: SJ.int64(1), a: SJ.int64(2) });
    const result = encodeWithOpts(v, { deterministic: false });
    const decoded = decode(result);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['z'].data, 1n);
    assert.strictEqual(obj['a'].data, 2n);
  });

  it('deterministic encoding sorts keys', () => {
    const v1 = SJ.object({ z: SJ.int64(1), a: SJ.int64(2) });
    const v2 = SJ.object({ a: SJ.int64(2), z: SJ.int64(1) });
    const enc1 = encodeWithOpts(v1, { deterministic: true });
    const enc2 = encodeWithOpts(v2, { deterministic: true });
    assert.deepStrictEqual(enc1, enc2);
  });

  it('deterministic with omitNull', () => {
    const v = SJ.object({
      name: SJ.string('test'),
      removed: SJ.null(),
      count: SJ.int64(1),
    });
    const enc = encodeWithOpts(v, { deterministic: true, omitNull: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['name'].data, 'test');
    assert.strictEqual(obj['count'].data, 1n);
    // null field should be omitted
    assert.strictEqual(obj['removed'], undefined);
  });

  it('deterministic encode with all value types', () => {
    const v = SJ.object({
      n: SJ.null(),
      b: SJ.bool(true),
      i: SJ.int64(42),
      u: SJ.uint64(100),
      f: SJ.float64(3.14),
      s: SJ.string('hello'),
      by: SJ.bytes(new Uint8Array([1, 2])),
      dt: SJ.datetime64(1000000000n),
      uu: SJ.uuid128(new Uint8Array(16)),
      bi: SJ.bigint(new Uint8Array([0x01, 0x02])),
      d: SJ.decimal128(2, new Uint8Array(16)),
      arr: SJ.array([SJ.int64(1), SJ.int64(2)]),
    });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['s'].data, 'hello');
    assert.strictEqual(obj['i'].data, 42n);
  });

  it('deterministic encode tensor', () => {
    const t = SJ.tensor(DType.FLOAT32, [2], new Uint8Array(8));
    const v = SJ.object({ t });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['t'].type, Type.TENSOR);
  });

  it('deterministic encode image', () => {
    const img = SJ.image(ImageFormat.JPEG, 10, 10, new Uint8Array(4));
    const v = SJ.object({ img });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['img'].type, Type.IMAGE);
  });

  it('deterministic encode audio', () => {
    const aud = SJ.audio(AudioEncoding.OPUS, 48000, 1, new Uint8Array(10));
    const v = SJ.object({ aud });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['aud'].type, Type.AUDIO);
  });

  it('deterministic encode tensor_ref', () => {
    const ref = SJ.tensorRef(1, new Uint8Array([1, 2, 3]));
    const v = SJ.object({ ref });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['ref'].type, Type.TENSOR_REF);
  });

  it('deterministic encode adjlist', () => {
    const adj = SJ.adjlist(IDWidth.INT32, 2, 1, [0, 0, 1], new Uint8Array(new Int32Array([0]).buffer));
    const v = SJ.object({ adj });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['adj'].type, Type.ADJLIST);
  });

  it('deterministic encode richtext', () => {
    const rt = SJ.richtext('hello', [1, 2], [{ start: 0, end: 5, kindId: 1 }]);
    const v = SJ.object({ rt });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['rt'].type, Type.RICHTEXT);
  });

  it('deterministic encode delta', () => {
    const d = SJ.delta(0, [
      { opCode: DeltaOpCode.SET_FIELD, fieldId: 0, value: SJ.int64(1) },
      { opCode: DeltaOpCode.DELETE_FIELD, fieldId: 1 },
    ]);
    const v = SJ.object({ d });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['d'].type, Type.DELTA);
  });

  it('deterministic encode bitmask', () => {
    const bm = SJ.bitmask(4, new Uint8Array([0b1010]));
    const v = SJ.object({ bm });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['bm'].type, Type.BITMASK);
  });

  it('deterministic encode array >15 elements', () => {
    const items: Value[] = [];
    for (let i = 0; i < 20; i++) items.push(SJ.int64(i));
    const v = SJ.array(items);
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    assert.strictEqual((decoded.data as Value[]).length, 20);
  });

  it('deterministic encode object >15 fields', () => {
    const members: Record<string, Value> = {};
    for (let i = 0; i < 20; i++) members[`f${String(i).padStart(2, '0')}`] = SJ.int64(i);
    const v = SJ.object(members);
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    assert.strictEqual(Object.keys(decoded.data as Record<string, Value>).length, 20);
  });

  it('deterministic encode FIXINT and FIXNEG ranges', () => {
    const v = SJ.object({
      zero: SJ.int64(0),
      max_fix: SJ.int64(127),
      above_fix: SJ.int64(128),
      neg1: SJ.int64(-1),
      neg16: SJ.int64(-16),
      neg17: SJ.int64(-17),
    });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['zero'].data, 0n);
    assert.strictEqual(obj['max_fix'].data, 127n);
    assert.strictEqual(obj['above_fix'].data, 128n);
    assert.strictEqual(obj['neg1'].data, -1n);
    assert.strictEqual(obj['neg16'].data, -16n);
    assert.strictEqual(obj['neg17'].data, -17n);
  });

  it('deterministic encode uint64', () => {
    const v = SJ.object({ u: SJ.uint64(999n) });
    const enc = encodeWithOpts(v, { deterministic: true });
    const decoded = decode(enc);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['u'].data, 999n);
    assert.strictEqual(obj['u'].type, Type.UINT64);
  });
});

// ============================================================
// Schema fingerprinting for extended types
// ============================================================

describe('gen2 schema fingerprinting', () => {
  it('64-bit fingerprint returns bigint', () => {
    const v = SJ.object({ x: SJ.int64(1) });
    const fp = schemaFingerprint64(v);
    assert.strictEqual(typeof fp, 'bigint');
  });

  it('tensor schema includes dtype and rank', () => {
    const t1 = SJ.tensor(DType.FLOAT32, [2, 3], new Uint8Array(24));
    const t2 = SJ.tensor(DType.INT8, [2, 3], new Uint8Array(6));
    const fp1 = schemaFingerprint32(t1);
    const fp2 = schemaFingerprint32(t2);
    assert.notStrictEqual(fp1, fp2); // different dtype
  });

  it('image schema includes format', () => {
    const i1 = SJ.image(ImageFormat.JPEG, 10, 10, new Uint8Array(4));
    const i2 = SJ.image(ImageFormat.PNG, 10, 10, new Uint8Array(4));
    assert.notStrictEqual(schemaFingerprint32(i1), schemaFingerprint32(i2));
  });

  it('audio schema includes encoding', () => {
    const a1 = SJ.audio(AudioEncoding.PCM_INT16, 44100, 2, new Uint8Array(4));
    const a2 = SJ.audio(AudioEncoding.OPUS, 44100, 2, new Uint8Array(4));
    assert.notStrictEqual(schemaFingerprint32(a1), schemaFingerprint32(a2));
  });

  it('tensor_ref schema includes storeId', () => {
    const r1 = SJ.tensorRef(1, new Uint8Array([1]));
    const r2 = SJ.tensorRef(2, new Uint8Array([1]));
    assert.notStrictEqual(schemaFingerprint32(r1), schemaFingerprint32(r2));
  });

  it('adjlist schema includes idWidth', () => {
    const a1 = SJ.adjlist(IDWidth.INT32, 1, 0, [0, 0], new Uint8Array(0));
    const a2 = SJ.adjlist(IDWidth.INT64, 1, 0, [0, 0], new Uint8Array(0));
    assert.notStrictEqual(schemaFingerprint32(a1), schemaFingerprint32(a2));
  });

  it('richtext schema is type-only', () => {
    const r1 = SJ.richtext('hello', [1], [{ start: 0, end: 5, kindId: 1 }]);
    const r2 = SJ.richtext('world', [2, 3], [{ start: 0, end: 5, kindId: 2 }]);
    assert.strictEqual(schemaFingerprint32(r1), schemaFingerprint32(r2));
  });

  it('delta schema includes ops', () => {
    const d1 = SJ.delta(0, [{ opCode: DeltaOpCode.SET_FIELD, fieldId: 0, value: SJ.int64(1) }]);
    const d2 = SJ.delta(0, [{ opCode: DeltaOpCode.DELETE_FIELD, fieldId: 0 }]);
    assert.notStrictEqual(schemaFingerprint32(d1), schemaFingerprint32(d2));
  });

  it('scalar types same schema', () => {
    const fp1 = schemaFingerprint32(SJ.int64(1));
    const fp2 = schemaFingerprint32(SJ.int64(999));
    assert.strictEqual(fp1, fp2);
  });

  it('schemaEquals for different type trees', () => {
    const v1 = SJ.object({ x: SJ.int64(1) });
    const v2 = SJ.object({ x: SJ.string('hello') });
    assert.strictEqual(schemaEquals(v1, v2), false);
  });
});

// ============================================================
// Tensor views
// ============================================================

describe('gen2 tensor views', () => {
  it('tensorViewFloat32 aligned', () => {
    const floats = new Float32Array([1.0, 2.0, 3.0, 4.0]);
    const data = new Uint8Array(floats.buffer);
    const td: TensorData = { dtype: DType.FLOAT32, shape: [4], data };
    const view = tensorViewFloat32(td);
    assert.ok(view !== null);
    assert.strictEqual(view!.length, 4);
    assert.ok(Math.abs(view![0] - 1.0) < 1e-6);
  });

  it('tensorViewFloat32 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.INT8, shape: [4], data: new Uint8Array(4) };
    assert.strictEqual(tensorViewFloat32(td), null);
  });

  it('tensorViewFloat32 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.FLOAT32, shape: [1], data: new Uint8Array(3) };
    assert.strictEqual(tensorViewFloat32(td), null);
  });

  it('tensorViewFloat64', () => {
    const doubles = new Float64Array([1.0, 2.0]);
    const data = new Uint8Array(doubles.buffer);
    const td: TensorData = { dtype: DType.FLOAT64, shape: [2], data };
    const view = tensorViewFloat64(td);
    assert.ok(view !== null);
    assert.strictEqual(view!.length, 2);
  });

  it('tensorViewFloat64 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.FLOAT32, shape: [1], data: new Uint8Array(8) };
    assert.strictEqual(tensorViewFloat64(td), null);
  });

  it('tensorViewFloat64 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.FLOAT64, shape: [1], data: new Uint8Array(5) };
    assert.strictEqual(tensorViewFloat64(td), null);
  });

  it('tensorViewInt32', () => {
    const ints = new Int32Array([10, 20, 30]);
    const data = new Uint8Array(ints.buffer);
    const td: TensorData = { dtype: DType.INT32, shape: [3], data };
    const view = tensorViewInt32(td);
    assert.ok(view !== null);
    assert.strictEqual(view![0], 10);
    assert.strictEqual(view![2], 30);
  });

  it('tensorViewInt32 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.INT8, shape: [4], data: new Uint8Array(4) };
    assert.strictEqual(tensorViewInt32(td), null);
  });

  it('tensorViewInt32 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.INT32, shape: [1], data: new Uint8Array(3) };
    assert.strictEqual(tensorViewInt32(td), null);
  });

  it('tensorCopyFloat32', () => {
    const buf = new ArrayBuffer(8);
    const dv = new DataView(buf);
    dv.setFloat32(0, 1.5, true);
    dv.setFloat32(4, 2.5, true);
    const data = new Uint8Array(buf);
    const td: TensorData = { dtype: DType.FLOAT32, shape: [2], data };
    const copy = tensorCopyFloat32(td);
    assert.ok(copy !== null);
    assert.ok(Math.abs(copy![0] - 1.5) < 1e-6);
    assert.ok(Math.abs(copy![1] - 2.5) < 1e-6);
  });

  it('tensorCopyFloat32 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.INT8, shape: [4], data: new Uint8Array(4) };
    assert.strictEqual(tensorCopyFloat32(td), null);
  });

  it('tensorCopyFloat32 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.FLOAT32, shape: [1], data: new Uint8Array(3) };
    assert.strictEqual(tensorCopyFloat32(td), null);
  });

  it('tensorCopyFloat64', () => {
    const buf = new ArrayBuffer(16);
    const dv = new DataView(buf);
    dv.setFloat64(0, 1.5, true);
    dv.setFloat64(8, 2.5, true);
    const data = new Uint8Array(buf);
    const td: TensorData = { dtype: DType.FLOAT64, shape: [2], data };
    const copy = tensorCopyFloat64(td);
    assert.ok(copy !== null);
    assert.ok(Math.abs(copy![0] - 1.5) < 1e-10);
  });

  it('tensorCopyFloat64 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.INT8, shape: [8], data: new Uint8Array(8) };
    assert.strictEqual(tensorCopyFloat64(td), null);
  });

  it('tensorCopyFloat64 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.FLOAT64, shape: [1], data: new Uint8Array(5) };
    assert.strictEqual(tensorCopyFloat64(td), null);
  });

  it('tensorCopyInt32', () => {
    const buf = new ArrayBuffer(8);
    const dv = new DataView(buf);
    dv.setInt32(0, 42, true);
    dv.setInt32(4, -7, true);
    const data = new Uint8Array(buf);
    const td: TensorData = { dtype: DType.INT32, shape: [2], data };
    const copy = tensorCopyInt32(td);
    assert.ok(copy !== null);
    assert.strictEqual(copy![0], 42);
    assert.strictEqual(copy![1], -7);
  });

  it('tensorCopyInt32 wrong dtype returns null', () => {
    const td: TensorData = { dtype: DType.INT8, shape: [4], data: new Uint8Array(4) };
    assert.strictEqual(tensorCopyInt32(td), null);
  });

  it('tensorCopyInt32 wrong length returns null', () => {
    const td: TensorData = { dtype: DType.INT32, shape: [1], data: new Uint8Array(3) };
    assert.strictEqual(tensorCopyInt32(td), null);
  });

  it('tensorFromFloat32', () => {
    const floats = new Float32Array([1.0, 2.0, 3.0]);
    const v = tensorFromFloat32([3], floats);
    // tensorFromFloat32 returns Type.BYTES with TensorData payload
    assert.strictEqual(v.type, Type.BYTES);
    const td = v.data as TensorData;
    assert.strictEqual(td.dtype, DType.FLOAT32);
    assert.deepStrictEqual(td.shape, [3]);
  });
});

// ============================================================
// Framed compression
// ============================================================

describe('gen2 framed compression', () => {
  it('encodeFramed no compression roundtrips', () => {
    const v = SJ.object({ name: SJ.string('test') });
    const framed = encodeFramed(v, Compression.NONE);
    const decoded = decodeFramed(framed);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['name'].data, 'test');
  });

  it('decodeFramed with no compression passes through to decode', () => {
    const v = SJ.object({ x: SJ.int64(42) });
    const encoded = encode(v);
    const decoded = decodeFramed(encoded);
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['x'].data, 42n);
  });

  it('decodeFramed with DecodeOptions (new API)', () => {
    const v = SJ.object({ x: SJ.int64(42) });
    const encoded = encode(v);
    const decoded = decodeFramed(encoded, { maxDepth: 100 });
    const obj = decoded.data as Record<string, Value>;
    assert.strictEqual(obj['x'].data, 42n);
  });

  it('decodeFramed truncated throws', () => {
    assert.throws(() => decodeFramed(new Uint8Array(2)), /truncated/i);
  });
});

// ============================================================
// Master stream edge cases
// ============================================================

describe('gen2 master stream edge cases', () => {
  it('isMasterStream returns false for short data', () => {
    assert.strictEqual(isMasterStream(new Uint8Array(2)), false);
  });

  it('isCowrieDocument for valid cowrie doc', () => {
    const v = SJ.object({ x: SJ.int64(1) });
    const enc = encode(v);
    assert.strictEqual(isCowrieDocument(enc), true);
  });

  it('isCowrieDocument returns false for short data', () => {
    assert.strictEqual(isCowrieDocument(new Uint8Array(2)), false);
  });

  it('isCowrieDocument returns false for master stream', () => {
    const frame = writeMasterFrame(SJ.null(), null);
    assert.strictEqual(isCowrieDocument(frame), false);
  });

  it('isCowrieDocument returns false for wrong magic', () => {
    assert.strictEqual(isCowrieDocument(new Uint8Array([0x00, 0x00, 0x00, 0x00])), false);
  });

  it('readMasterFrame from legacy cowrie document', () => {
    // readMasterFrame requires >= 24 bytes. Build an object large enough.
    const v = SJ.object({
      x: SJ.int64(42),
      padding1: SJ.string('abcdefghij'),
      padding2: SJ.string('klmnopqrst'),
    });
    const enc = encode(v);
    assert.ok(enc.length >= 24, `encoded length ${enc.length} must be >= 24`);
    const [frame, consumed] = readMasterFrame(enc);
    assert.strictEqual((frame.payload.data as Record<string, Value>)['x'].data, 42n);
    assert.ok(consumed > 0);
  });

  it('readMasterFrame from legacy stream (length-prefixed)', () => {
    const v = SJ.object({
      y: SJ.int64(99),
      padding1: SJ.string('abcdefghij'),
      padding2: SJ.string('klmnopqrst'),
    });
    const enc = encode(v);
    // Build legacy stream: 4-byte LE length + cowrie data
    const legacy = new Uint8Array(4 + enc.length);
    const dv = new DataView(legacy.buffer);
    dv.setUint32(0, enc.length, true);
    legacy.set(enc, 4);
    assert.ok(legacy.length >= 24, `legacy length ${legacy.length} must be >= 24`);
    const [frame, consumed] = readMasterFrame(legacy);
    assert.strictEqual((frame.payload.data as Record<string, Value>)['y'].data, 99n);
    assert.strictEqual(consumed, 4 + enc.length);
  });

  it('readMasterFrame truncated throws', () => {
    assert.throws(() => readMasterFrame(new Uint8Array(10)));
  });

  it('readMasterFrame invalid legacy stream length=0 throws', () => {
    // 24 bytes of zeros: not master magic, not cowrie magic, falls to legacy stream
    // with length=0 which is invalid
    const data = new Uint8Array(24);
    assert.throws(() => readMasterFrame(data));
  });
});

// ============================================================
// Decode error paths
// ============================================================

describe('gen2 decode error paths', () => {
  it('invalid magic bytes', () => {
    assert.throws(() => decode(new Uint8Array([0x00, 0x00, 0x02, 0x00, 0x00, 0x00])), /magic/i);
  });

  it('unsupported version', () => {
    assert.throws(() => decode(new Uint8Array([0x53, 0x4a, 0x99, 0x00, 0x00, 0x00])), /version/i);
  });

  it('truncated data', () => {
    assert.throws(() => decode(new Uint8Array([0x53, 0x4a])), /end of data/i);
  });

  it('invalid tag', () => {
    // Valid header, dict=0, then invalid tag 0x1F
    const data = new Uint8Array([0x53, 0x4a, 0x02, 0x00, 0x00, 0x1f]);
    assert.throws(() => decode(data), /Invalid tag/);
  });

  it('invalid dictionary index', () => {
    // Valid header, dict has 1 entry, then object with fieldId=5 (out of range)
    const v = SJ.object({ test: SJ.int64(1) });
    const enc = encode(v);
    // Corrupt the field index (byte after FIXMAP tag)
    // Find the FIXMAP tag position - it's after header(4) + dict
    const corrupted = new Uint8Array(enc);
    // Modify field index to be out of range
    // The structure is: SJ(2) + ver(1) + flags(1) + dictLen(1) + dictEntry... + FIXMAP(1) + fieldIdx(1) + value...
    // Find fieldIdx byte and set to 0xFF
    for (let i = 5; i < corrupted.length; i++) {
      // Look for the field index byte (should be 0x00 for first dict entry)
      if (corrupted[i] >= 0xd0 && corrupted[i] <= 0xdf) {
        // FIXMAP tag found, next byte is field index
        corrupted[i + 1] = 0x7f; // Way out of range
        break;
      }
    }
    assert.throws(() => decode(corrupted), /dictionary index/i);
  });

  it('trailing data throws', () => {
    const v = SJ.null();
    const enc = encode(v);
    const withTrailing = new Uint8Array(enc.length + 2);
    withTrailing.set(enc);
    withTrailing[enc.length] = 0xff;
    withTrailing[enc.length + 1] = 0xff;
    assert.throws(() => decode(withTrailing), /trailing data/i);
  });

  it('decode with UnknownExtBehavior.ERROR throws on ext', () => {
    const v = SJ.unknownExt(42, new Uint8Array([1, 2, 3]));
    const enc = encode(v);
    assert.throws(
      () => decode(enc, { onUnknownExt: UnknownExtBehavior.ERROR }),
      /Unknown extension/
    );
  });

  it('decode with UnknownExtBehavior.SKIP_AS_NULL returns null', () => {
    const v = SJ.unknownExt(42, new Uint8Array([1, 2, 3]));
    const enc = encode(v);
    const result = decode(enc, { onUnknownExt: UnknownExtBehavior.SKIP_AS_NULL });
    assert.strictEqual(result.type, Type.NULL);
  });

  it('decode with custom limits', () => {
    // Array that would exceed a very low limit
    const v = SJ.array([SJ.int64(1), SJ.int64(2), SJ.int64(3)]);
    const enc = encode(v);
    assert.throws(
      () => decode(enc, { maxArrayLen: 2 }),
      /Array too large|SecurityLimitExceeded/
    );
  });

  it('max depth exceeded', () => {
    // Create deeply nested structure
    let v: Value = SJ.int64(1);
    for (let i = 0; i < 10; i++) {
      v = SJ.array([v]);
    }
    const enc = encode(v);
    assert.throws(
      () => decode(enc, { maxDepth: 5 }),
      /depth/i
    );
  });
});

// ============================================================
// Float32 decode path
// ============================================================

describe('gen2 float32 decode', () => {
  it('float32 tag decodes correctly', () => {
    // Manually craft a cowrie payload with FLOAT32 tag (0x0f)
    const buf = new ArrayBuffer(4);
    new DataView(buf).setFloat32(0, 1.5, true);
    const f32bytes = new Uint8Array(buf);

    const payload = new Uint8Array([
      0x53, 0x4a,  // magic
      0x02,        // version
      0x00,        // flags
      0x00,        // dict count = 0
      0x0f,        // FLOAT32 tag
      ...f32bytes,
    ]);

    const v = decode(payload);
    assert.strictEqual(v.type, Type.FLOAT64); // float32 decoded as float64
    assert.ok(Math.abs((v.data as number) - 1.5) < 1e-6);
  });
});

// ============================================================
// Invariant #4: Trailing garbage
// ============================================================

describe('Invariant Tests', () => {
  it('Invariant #4: decode rejects trailing garbage', () => {
    const v = SJ.object({ a: SJ.int64(42) });
    const enc = encode(v);
    const corrupted = new Uint8Array(enc.length + 1);
    corrupted.set(enc);
    corrupted[enc.length] = 0xff;
    assert.throws(() => decode(corrupted), /trailing data/i);
  });
});

// ============================================================
// Truncated input invariant (#3)
// ============================================================

describe('gen2 truncated input invariant', () => {
  const testValues: Array<{ label: string; value: Value }> = [
    { label: 'null', value: SJ.null() },
    { label: 'int64', value: SJ.int64(42) },
    { label: 'float64', value: SJ.float64(3.14) },
    { label: 'string', value: SJ.string('hello world') },
    { label: 'bool', value: SJ.bool(true) },
    { label: 'bytes', value: SJ.bytes(new Uint8Array([1, 2, 3, 4, 5])) },
    { label: 'array', value: SJ.array([SJ.int64(1), SJ.string('two'), SJ.bool(false)]) },
    { label: 'object', value: SJ.object({ name: SJ.string('Alice'), age: SJ.int64(30) }) },
    { label: 'nested object', value: SJ.object({
      a: SJ.object({ b: SJ.array([SJ.int64(1), SJ.int64(2)]) }),
      c: SJ.string('deep'),
    })},
  ];

  for (const { label, value } of testValues) {
    it(`should reject truncated ${label} at every byte position`, () => {
      const encoded = encode(value);
      // The header alone (magic + version + flags + empty dict) is 5 bytes;
      // truncating at any point from len-1 down to 1 must throw.
      for (let len = encoded.length - 1; len >= 1; len--) {
        const truncated = encoded.slice(0, len);
        assert.throws(
          () => decode(truncated),
          (err: unknown) => err instanceof Error,
          `expected error decoding ${label} truncated to ${len}/${encoded.length} bytes`
        );
      }
    });
  }
});

// ============================================================
// NaN/Inf Binary Tests
// ============================================================

describe('NaN/Inf Binary Tests', () => {
  it('NaN roundtrips through binary encoding', () => {
    const encoded = encode(SJ.float64(NaN));
    const decoded = decode(encoded);
    assert.strictEqual(decoded.type, Type.FLOAT64);
    assert.ok(isNaN(decoded.data as number), 'expected NaN after roundtrip');
  });

  it('+Infinity roundtrips through binary encoding', () => {
    const encoded = encode(SJ.float64(Infinity));
    const decoded = decode(encoded);
    assert.strictEqual(decoded.type, Type.FLOAT64);
    assert.strictEqual(decoded.data, Infinity);
  });

  it('-Infinity roundtrips through binary encoding', () => {
    const encoded = encode(SJ.float64(-Infinity));
    const decoded = decode(encoded);
    assert.strictEqual(decoded.type, Type.FLOAT64);
    assert.strictEqual(decoded.data, -Infinity);
  });
});
