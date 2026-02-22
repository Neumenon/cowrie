# glyph-js

GLYPH v2 - Token-efficient serialization for LLM communication.

## Installation

```bash
npm install glyph-js
```

## Features

- **Token Efficient**: 40-60% fewer tokens than JSON (tokens matter more than bytes!)
- **Schema-Driven**: Type-safe encoding with field IDs
- **Multiple Modes**: Packed, Tabular, Patch encoding
- **JSON Compatible**: Seamless conversion to/from JSON
- **TypeScript First**: Full type definitions included

### Token Savings

| Data Type | Savings |
|-----------|---------|
| LLM messages | 40% |
| Tool calls | 42% |
| Conversations (25 msgs) | 49% |
| Search results (50 rows) | 52% |
| Batch tool results | 62% |

## Quick Start

```typescript
import { 
  g, field,
  SchemaBuilder, t,
  fromJson, toJson,
  emitPacked, parsePacked,
  jsonToPacked,
} from 'glyph-js';

// 1. Define a schema
const schema = new SchemaBuilder()
  .addPackedStruct('Team', 'v2')
    .field('id', t.id(), { fid: 1, wireKey: 't' })
    .field('name', t.str(), { fid: 2, wireKey: 'n' })
    .field('league', t.str(), { fid: 3, wireKey: 'l' })
  .build();

// 2. Create values
const team = g.struct('Team',
  field('id', g.id('t', 'ARS')),
  field('name', g.str('Arsenal')),
  field('league', g.str('EPL'))
);

// 3. Emit as packed LYPH
const packed = emitPacked(team, schema);
// => "Team@(^t:ARS Arsenal EPL)"

// 4. Parse back
const parsed = parsePacked(packed, schema);
console.log(parsed.get('name')?.asStr()); // => "Arsenal"

// 5. Convert from JSON
const json = { $type: 'Team', id: '^t:ARS', name: 'Arsenal', league: 'EPL' };
const lyph = jsonToPacked(json, schema);
```

## Encoding Modes

### Packed Mode

Positional encoding for structs:

```
Team@(^t:ARS Arsenal EPL)
```

With bitmap for sparse optionals:

```
Match@{bm=0b101}(^m:123 2025-12-19T20:00:00Z 2 1)
```

### Tabular Mode

For lists of structs:

```
@tab Team [t n l]
^t:ARS Arsenal EPL
^t:LIV Liverpool EPL
^t:MCI "Man City" EPL
@end
```

### Struct Mode (v1 compatible)

```
Team{id=^t:ARS name=Arsenal league=EPL}
```

## JSON Conversion

### From JSON

```typescript
import { fromJson } from 'glyph-js';

// Automatic type detection
const gv = fromJson({
  $type: 'Match',
  id: '^m:123',          // Parsed as ref ID
  kickoff: '2025-12-19T20:00:00Z',  // Parsed as time
  active: true,
});

// With schema hints
const gv2 = fromJson(json, { schema, typeName: 'Match' });
```

### To JSON

```typescript
import { toJson } from 'glyph-js';

const json = toJson(gvalue, {
  includeTypeMarkers: true,  // Add $type to structs
  compactRefs: true,         // Use ^prefix:value format
  useWireKeys: false,        // Use full field names
});
```

## Token Savings

```typescript
import { compareTokens } from 'glyph-js';

const stats = compareTokens(jsonData, schema);
console.log(`Savings: ${stats.savingsPercent.toFixed(1)}%`);
// Typical savings: 40-60% for structured data
// Large datasets (50+ rows): 52-62% savings
```

## API Reference

### Value Constructors

```typescript
import { g, field } from 'glyph-js';

g.null()           // null value
g.bool(true)       // boolean
g.int(42)          // integer
g.float(3.14)      // float
g.str('hello')     // string
g.id('t', 'ARS')   // reference ID (^t:ARS)
g.time(new Date()) // timestamp
g.list(v1, v2)     // list
g.map(entry1)      // map
g.struct('Type', field('k', v))  // typed struct
g.sum('Tag', v)    // tagged union
```

### Schema Builder

```typescript
import { SchemaBuilder, t } from 'glyph-js';

const schema = new SchemaBuilder()
  .addPackedStruct('Type', 'v2')
    .field('name', t.str(), { 
      fid: 1,           // Field ID for packed encoding
      wireKey: 'n',     // Short key for wire format
      optional: true,   // Optional field
    })
  .addSum('Result', 'v1')
    .variant('Ok', t.str())
    .variant('Err', t.str())
  .withPack('Type')    // Enable packed encoding
  .withTab('Type')     // Enable tabular encoding
  .build();
```

### Type Specs

```typescript
import { t } from 'glyph-js';

t.null()
t.bool()
t.int()
t.float()
t.str()
t.bytes()
t.time()
t.id()
t.list(t.str())         // list<str>
t.map(t.str(), t.int()) // map<str, int>
t.ref('TypeName')       // reference to named type
```

## License

MIT
