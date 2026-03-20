// Quick smoke test for cowrie-wasm
import { encode, decode, encodeFramed, decodeFramed, fromJSON, toJSON, schemaFingerprint32, schemaEquals } from './pkg-node/cowrie_wasm.js';

let pass = 0;
let fail = 0;

function assert(condition, msg) {
    if (condition) {
        pass++;
    } else {
        fail++;
        console.error(`FAIL: ${msg}`);
    }
}

function sortedStringify(v) {
    if (v === null || v === undefined) return JSON.stringify(v);
    if (Array.isArray(v)) return '[' + v.map(sortedStringify).join(',') + ']';
    if (typeof v === 'object') {
        const keys = Object.keys(v).sort();
        return '{' + keys.map(k => JSON.stringify(k) + ':' + sortedStringify(v[k])).join(',') + '}';
    }
    return JSON.stringify(v);
}

function assertDeepEqual(a, b, msg) {
    const sa = sortedStringify(a);
    const sb = sortedStringify(b);
    assert(sa === sb, `${msg}: ${sa} !== ${sb}`);
}

// === Primitives ===
{
    const cases = [
        null,
        true,
        false,
        42,
        -1,
        3.14159,
        0,
        "hello world",
        "",
        [1, 2, 3],
        [],
        { name: "Alice", age: 30 },
        {},
        { nested: { deep: [1, { x: true }] } },
    ];

    for (const val of cases) {
        const encoded = encode(val);
        assert(encoded instanceof Uint8Array, `encode returns Uint8Array for ${JSON.stringify(val)}`);
        assert(encoded.length > 0, `encoded length > 0 for ${JSON.stringify(val)}`);
        const decoded = decode(encoded);
        assertDeepEqual(decoded, val, `roundtrip ${JSON.stringify(val)}`);
    }
}

// === Bytes (Uint8Array) ===
{
    const bytes = new Uint8Array([1, 2, 3, 4, 5]);
    const encoded = encode({ _cowrie_type: "bytes", data: bytes });
    const decoded = decode(encoded);
    assert(decoded instanceof Uint8Array, "bytes decoded as Uint8Array");
    assert(decoded.length === 5, "bytes length preserved");
    assert(decoded[0] === 1 && decoded[4] === 5, "bytes values preserved");
}

// === Tensor ===
{
    const data = new Float32Array([1.0, 2.0, 3.0, 4.0]);
    const tensor = {
        _cowrie_type: "tensor",
        dtype: "float32",
        shape: [2, 2],
        data: new Uint8Array(data.buffer),
    };
    const encoded = encode(tensor);
    const decoded = decode(encoded);
    assert(decoded._cowrie_type === "tensor", "tensor type preserved");
    assert(decoded.dtype === "float32", "tensor dtype preserved");
    assertDeepEqual(decoded.shape, [2, 2], "tensor shape preserved");
    assert(decoded.data.length === 16, "tensor data length preserved");
}

// === Compression (framed) ===
{
    const bigObj = { data: "x".repeat(1000), count: 42 };
    const framed = encodeFramed(bigObj, true);
    const plain = encode(bigObj);
    assert(framed.length < plain.length, `gzip compressed: ${framed.length} < ${plain.length}`);
    const decoded = decodeFramed(framed);
    assert(decoded.count === 42, "framed decode preserves data");
    assert(decoded.data.length === 1000, "framed decode preserves string length");
}

// === JSON bridge ===
{
    const json = '{"name":"Bob","score":99.5}';
    const binary = fromJSON(json);
    assert(binary instanceof Uint8Array, "fromJSON returns Uint8Array");
    const backJson = toJSON(binary);
    const parsed = JSON.parse(backJson);
    assert(parsed.name === "Bob", "JSON roundtrip name");
    assert(parsed.score === 99.5, "JSON roundtrip score");
}

// === Schema fingerprinting ===
{
    const a = { name: "Alice", age: 30 };
    const b = { name: "Bob", age: 25 };
    const c = { id: 1, label: "test" };

    const fpA = schemaFingerprint32(a);
    const fpB = schemaFingerprint32(b);
    const fpC = schemaFingerprint32(c);

    assert(fpA === fpB, "same schema same fingerprint");
    assert(fpA !== fpC, "different schema different fingerprint");
    assert(schemaEquals(a, b), "schemaEquals true for same schema");
    assert(!schemaEquals(a, c), "schemaEquals false for different schema");
}

// === Large integer / BigInt ===
{
    // Safe integer
    const encoded = encode(Number.MAX_SAFE_INTEGER);
    const decoded = decode(encoded);
    assert(decoded === Number.MAX_SAFE_INTEGER, `MAX_SAFE_INTEGER roundtrip: ${decoded}`);
}

console.log(`\n=== Results: ${pass} passed, ${fail} failed ===`);
process.exit(fail > 0 ? 1 : 0);
