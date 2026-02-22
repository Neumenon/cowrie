# GLYPH Python

Python implementation of GLYPH - token-efficient serialization for AI agents.

**40-60% fewer tokens than JSON** - tokens matter more than bytes for LLM costs and context windows.

| Data Type | Token Savings |
|-----------|---------------|
| LLM messages | 40% |
| Tool calls | 42% |
| Conversations (25 msgs) | 49% |
| Search results (50 rows) | 52% |
| Batch tool results | 62% |

## Installation

```bash
pip install glyph-serial
```

## Quick Start

```python
import glyph

# JSON to GLYPH
data = {"action": "search", "query": "weather", "max_results": 10}
text = glyph.json_to_glyph(data)
print(text)  # {action=search max_results=10 query=weather}

# GLYPH to JSON
restored = glyph.glyph_to_json(text)
assert restored == data

# Parse GLYPH text
result = glyph.parse('{name=Alice age=30}')
print(result.get("name").as_str())  # Alice

# Emit GLYPH text
from glyph import g, field
team = g.struct("Team", field("name", g.str("Arsenal")), field("rank", g.int(1)))
print(glyph.emit(team))  # Team{name=Arsenal rank=1}
```

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `parse(text)` | Parse GLYPH text to GValue |
| `emit(value)` | Emit GValue as GLYPH text |
| `json_to_glyph(data)` | Convert Python dict/list to GLYPH text |
| `glyph_to_json(text)` | Convert GLYPH text to Python dict/list |
| `from_json(data)` | Convert Python value to GValue |
| `to_json(value)` | Convert GValue to Python value |
| `fingerprint_loose(value)` | SHA-256 hash of canonical form |
| `equal_loose(a, b)` | Check equality in canonical form |

### Value Constructors

```python
from glyph import GValue, g, field, MapEntry

# Using GValue class
GValue.null()
GValue.bool_(True)
GValue.int_(42)
GValue.float_(3.14)
GValue.str_("hello")
GValue.bytes_(b"data")
GValue.time(datetime.now())
GValue.id("prefix", "value")
GValue.list_(v1, v2, v3)
GValue.map_(MapEntry("key", value))
GValue.struct("TypeName", MapEntry("field", value))
GValue.sum("Tag", value)

# Using g shorthand
g.null()
g.bool(True)
g.int(42)
g.float(3.14)
g.str("hello")
g.list(g.int(1), g.int(2))
g.map(MapEntry("a", g.int(1)))
g.struct("Type", field("x", g.int(1)))
```

### GValue Methods

```python
v = glyph.parse('{name=Alice age=30}')

# Type checking
v.type          # GType.MAP
v.is_null()     # False

# Accessors (raise TypeError if wrong type)
v.as_bool()
v.as_int()
v.as_float()
v.as_str()
v.as_bytes()
v.as_time()
v.as_id()       # Returns RefID
v.as_list()     # Returns List[GValue]
v.as_map()      # Returns List[MapEntry]
v.as_struct()   # Returns StructValue
v.as_sum()      # Returns SumValue

# Field access (for maps/structs)
v.get("name")   # Returns GValue or None

# List access
v.index(0)      # Returns GValue

# Length
len(v)          # Works for list, map, struct
```

## Auto-Tabular Mode

Lists of homogeneous objects (3+ items) automatically emit as tables:

```python
data = [
    {"id": "a", "score": 0.9},
    {"id": "b", "score": 0.8},
    {"id": "c", "score": 0.7},
]
print(glyph.json_to_glyph(data))
```

Output:
```
@tab _ [id score]
|a|0.9|
|b|0.8|
|c|0.7|
@end
```

## Options

```python
from glyph import LooseCanonOpts, NullStyle, llm_loose_canon_opts

# Default options (null = ∅)
opts = LooseCanonOpts()

# LLM-friendly options (null = _)
opts = llm_loose_canon_opts()

# Custom options
opts = LooseCanonOpts(
    auto_tabular=True,
    min_rows=3,
    max_cols=20,
    null_style=NullStyle.UNDERSCORE,
)

text = glyph.canonicalize_loose(value, opts)
```

## Types

```python
from glyph import GType, RefID, MapEntry, StructValue, SumValue

# GType enum
GType.NULL, GType.BOOL, GType.INT, GType.FLOAT, GType.STR
GType.BYTES, GType.TIME, GType.ID, GType.LIST, GType.MAP
GType.STRUCT, GType.SUM

# RefID (for ^prefix:value references)
ref = RefID(prefix="user", value="123")

# MapEntry (for map/struct fields)
entry = MapEntry(key="name", value=GValue.str_("Alice"))

# StructValue (for typed structs)
sv = StructValue(type_name="Team", fields=[...])

# SumValue (for tagged unions)
sum_val = SumValue(tag="Some", value=GValue.int_(42))
```

## Development

```bash
# Run tests
cd py
python -m pytest tests/ -v

# Install in development mode
pip install -e .
```

## License

Apache 2.0
