"""
GLYPH - Token-efficient serialization for AI agents

A schema-optional serialization format that is 30-50% more token-efficient than JSON.

Example:
    >>> import glyph
    >>>
    >>> # Convert JSON to GLYPH
    >>> data = {"action": "search", "query": "weather in NYC", "max_results": 10}
    >>> text = glyph.json_to_glyph(data)
    >>> print(text)
    {action=search max_results=10 query="weather in NYC"}
    >>>
    >>> # Parse GLYPH text
    >>> v = glyph.parse('{action=search query="test"}')
    >>> print(v.get("action").as_str())
    search
    >>>
    >>> # Build values programmatically
    >>> from glyph import g, field
    >>> team = g.struct("Team", field("name", g.str("Arsenal")), field("rank", g.int(1)))
    >>> print(glyph.emit(team))
    Team{name=Arsenal rank=1}
"""

__version__ = "1.0.0"

# Core types
from .types import (
    GValue,
    GType,
    RefID,
    MapEntry,
    StructValue,
    SumValue,
    field,
    g,
    G,
)

# Parsing
from .parse import (
    parse,
    parse_loose,
)

# Canonicalization / Emission
from .loose import (
    canonicalize_loose,
    canonicalize_loose_no_tabular,
    fingerprint_loose,
    equal_loose,
    # Options
    LooseCanonOpts,
    NullStyle,
    default_loose_canon_opts,
    llm_loose_canon_opts,
    no_tabular_loose_canon_opts,
    # JSON bridge
    from_json_loose,
    to_json_loose,
    parse_json_loose,
    stringify_json_loose,
    json_to_glyph,
    glyph_to_json,
)

# Streaming validation
from .stream_validator import (
    StreamingValidator,
    ToolRegistry,
    ToolSchema,
    ArgSchema,
    ArgType,
    StreamValidationResult,
    ValidatorState,
    TimelineEvent,
)

# Convenient aliases
emit = canonicalize_loose
from_json = from_json_loose
to_json = to_json_loose

__all__ = [
    # Version
    "__version__",
    # Core types
    "GValue",
    "GType",
    "RefID",
    "MapEntry",
    "StructValue",
    "SumValue",
    "field",
    "g",
    "G",
    # Parsing
    "parse",
    "parse_loose",
    # Emission
    "emit",
    "canonicalize_loose",
    "canonicalize_loose_no_tabular",
    "fingerprint_loose",
    "equal_loose",
    # Options
    "LooseCanonOpts",
    "NullStyle",
    "default_loose_canon_opts",
    "llm_loose_canon_opts",
    "no_tabular_loose_canon_opts",
    # JSON bridge
    "from_json",
    "to_json",
    "from_json_loose",
    "to_json_loose",
    "parse_json_loose",
    "stringify_json_loose",
    "json_to_glyph",
    "glyph_to_json",
    # Streaming validation
    "StreamingValidator",
    "ToolRegistry",
    "ToolSchema",
    "ArgSchema",
    "ArgType",
    "StreamValidationResult",
    "ValidatorState",
    "TimelineEvent",
]
