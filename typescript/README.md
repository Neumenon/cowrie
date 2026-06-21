# cowrie-codec

TypeScript implementation of Cowrie, a deterministic binary codec for JSON-like and ML-native data.

## Install

```bash
npm install cowrie-codec
```

## Usage

```ts
import { gen2 } from 'cowrie-codec';

const value = gen2.SJ.object({ name: gen2.SJ.string('Alice') });
const encoded = gen2.encode(value);
const decoded = gen2.decode(encoded);
```

See the main repository for the wire spec and cross-language conformance notes: https://github.com/Neumenon/cowrie
