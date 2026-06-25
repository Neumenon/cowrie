# cowrie-codec

The TypeScript implementation of **Cowrie v1** — a deterministic, content-addressable,
AI-native binary codec.

**Invariant:** equal value ⇒ equal canonical bytes ⇒ equal hash, **byte-identical** across
Python / Go / Rust / TypeScript. Magic `COWR`, version 1. The content address is a multihash
SHA-256 (spec §3).

> All four implementations encode `{"name":"Alice","scores":[1,2,3]}` to the same content address:
> `122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1`.

This package is `0.9.0` — pre-1.0, a release candidate. The wire format is **not yet frozen**.

## Install

Nothing is published to npm yet. npm cannot install a git subdirectory, so today you build the
tarball from source:

```bash
git clone -b phase0-spec https://github.com/Neumenon/cowrie.git
cd cowrie/typescript
npm install && npm run build
npm pack            # -> cowrie-codec-0.9.0.tgz
```

Then install the tarball into your project:

```bash
npm install /path/to/cowrie/typescript/cowrie-codec-0.9.0.tgz
```

**After publish** (do not run this yet):

```bash
npm install cowrie-codec
```

## Usage

JavaScript numbers can't distinguish `int` from `float`, so you first lift a plain value into a
typed `Value` with `fromAny`, then encode:

```ts
const { fromAny, encode, decode, toAny, contentAddress } = require("cowrie-codec");

const v = fromAny({ name: "Alice", scores: [1, 2, 3] }); // plain JS -> typed Value
const wire = encode(v);                                   // canonical bytes
toAny(decode(wire));                                      // -> { name: "Alice", scores: [1, 2, 3] }
contentAddress(v);                                        // multihash SHA-256 identity (§3)
```

ESM import works the same way:

```ts
import { fromAny, encode, decode, toAny, contentAddress } from "cowrie-codec";
```

## API

The root export also includes the full v1 identity surface (parity with the reference
implementations):

| Export                                  | Purpose                                        |
| --------------------------------------- | ---------------------------------------------- |
| `fromAny` / `toAny`                     | Lift plain JS ⇄ typed `Value`                  |
| `encode` / `decode`                     | `Value` ⇄ canonical wire bytes                 |
| `contentAddress` / `contentAddressHex`  | Multihash SHA-256 content address (§3)         |
| `addressOfBytes`                        | Content address of already-encoded wire bytes  |
| `schemaFingerprint64`                   | Structural fingerprint (§4)                    |
| `fileIdentity` / `merkleRoot`           | File / Merkle identity (§7)                     |
| `encodeFile` / `decodeFile`             | File-framed encode / decode                    |
| `tensorSpans`                           | Zero-copy tensor span offsets                  |

## Links

- [Cowrie repository README](https://github.com/Neumenon/cowrie#readme)
- [Authoritative wire spec — `docs/SPEC-v1.md`](https://github.com/Neumenon/cowrie/blob/phase0-spec/docs/SPEC-v1.md)

## License

MIT
