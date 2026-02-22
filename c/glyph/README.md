# GLYPH Codec - C

Token-efficient serialization for AI agents.

## Build

```bash
make        # Build library
make test   # Run tests
make clean  # Clean build
```

## Quick Start

```c
#include "glyph.h"
#include <stdio.h>

int main() {
    // JSON to GLYPH
    glyph_value_t *v = glyph_from_json("{\"action\": \"search\", \"query\": \"weather\"}");
    char *glyph = glyph_canonicalize_loose(v);
    printf("%s\n", glyph);  // {action=search query=weather}

    glyph_free(glyph);
    glyph_value_free(v);

    // Build values directly
    glyph_value_t *map = glyph_map_new();
    glyph_map_set(map, "name", glyph_str("Alice"));
    glyph_map_set(map, "age", glyph_int(30));

    char *result = glyph_canonicalize_loose(map);
    printf("%s\n", result);  // {age=30 name=Alice}

    glyph_free(result);
    glyph_value_free(map);
    return 0;
}
```

Compile:
```bash
gcc -I./include your_file.c -L./build -lglyph -lm -o your_program
```

## API Reference

### Types

```c
typedef enum {
    GLYPH_NULL, GLYPH_BOOL, GLYPH_INT, GLYPH_FLOAT,
    GLYPH_STR, GLYPH_BYTES, GLYPH_TIME, GLYPH_ID,
    GLYPH_LIST, GLYPH_MAP, GLYPH_STRUCT, GLYPH_SUM,
} glyph_type_t;

typedef struct glyph_value glyph_value_t;
```

### Constructors

| Function | Description |
|----------|-------------|
| `glyph_null()` | Create null value |
| `glyph_bool(bool)` | Create boolean |
| `glyph_int(int64_t)` | Create integer |
| `glyph_float(double)` | Create float |
| `glyph_str(const char*)` | Create string (copies) |
| `glyph_bytes(uint8_t*, size_t)` | Create bytes (copies) |
| `glyph_id(prefix, value)` | Create reference ID |
| `glyph_list_new()` | Create empty list |
| `glyph_map_new()` | Create empty map |
| `glyph_struct_new(type_name)` | Create struct |
| `glyph_sum(tag, value)` | Create sum type |

### List/Map Operations

```c
glyph_list_append(list, item);      // Append to list (takes ownership)
glyph_map_set(map, key, value);     // Set map entry (copies key, takes ownership of value)
glyph_struct_set(s, key, value);    // Set struct field
```

### Accessors

```c
glyph_type_t glyph_get_type(v);     // Get value type
bool glyph_as_bool(v);              // Get bool (false if not bool)
int64_t glyph_as_int(v);            // Get int (0 if not int)
double glyph_as_float(v);           // Get float (0.0 if not float)
const char* glyph_as_str(v);        // Get string (NULL if not string)
size_t glyph_list_len(v);           // Get list length
glyph_value_t* glyph_list_get(v, i); // Get list item by index
glyph_value_t* glyph_get(v, key);   // Get map/struct value by key
```

### Canonicalization

```c
char* glyph_canonicalize_loose(v);              // Default options
char* glyph_canonicalize_loose_no_tabular(v);   // No auto-tabular
char* glyph_canonicalize_loose_with_opts(v, opts);
char* glyph_fingerprint_loose(v);               // Same as canonicalize
char* glyph_hash_loose(v);                      // Hash (16 hex chars)
bool glyph_equal_loose(a, b);                   // Semantic equality
```

### JSON Bridge

```c
glyph_value_t* glyph_from_json(const char* json);  // Parse JSON
char* glyph_to_json(const glyph_value_t* v);       // Emit JSON
```

### Memory Management

```c
void glyph_value_free(v);  // Free value and children
void glyph_free(s);        // Free string from glyph functions
```

## Options

```c
glyph_canon_opts_t opts = glyph_canon_opts_default();
opts.auto_tabular = true;
opts.min_rows = 3;
opts.max_cols = 64;
opts.allow_missing = true;
opts.null_style = GLYPH_NULL_UNDERSCORE;  // _ or GLYPH_NULL_SYMBOL (∅)
```

## Examples

### Token Savings

```c
// JSON: 67 chars
// {"action":"search","query":"weather in NYC","max_results":10}

// GLYPH: 52 chars (22% smaller)
// {action=search max_results=10 query="weather in NYC"}
```

### Auto-Tabular Mode

```c
const char *json = "[{\"id\":\"doc_1\",\"score\":0.95},"
                   "{\"id\":\"doc_2\",\"score\":0.89},"
                   "{\"id\":\"doc_3\",\"score\":0.84}]";
glyph_value_t *v = glyph_from_json(json);
char *glyph = glyph_canonicalize_loose(v);
// @tab _ rows=3 cols=2 [id score]
// |doc_1|0.95|
// |doc_2|0.89|
// |doc_3|0.84|
// @end
```

## License

MIT
