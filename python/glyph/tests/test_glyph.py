"""Tests for GLYPH Python implementation."""

import pytest
from datetime import datetime, timezone

import sys
sys.path.insert(0, str(__file__).rsplit('/', 2)[0])

from glyph import (
    GValue, GType, g, field, MapEntry,
    parse, emit,
    from_json, to_json,
    json_to_glyph,
    canonicalize_loose,
    equal_loose,
    fingerprint_loose,
    LooseCanonOpts, NullStyle,
)


class TestGValue:
    """Tests for GValue type."""

    def test_null(self):
        v = GValue.null()
        assert v.type == GType.NULL
        assert v.is_null()

    def test_bool(self):
        t = GValue.bool_(True)
        f = GValue.bool_(False)
        assert t.as_bool() == True
        assert f.as_bool() == False

    def test_int(self):
        v = GValue.int_(42)
        assert v.as_int() == 42
        assert v.type == GType.INT

    def test_float(self):
        v = GValue.float_(3.14)
        assert abs(v.as_float() - 3.14) < 0.001

    def test_str(self):
        v = GValue.str_("hello")
        assert v.as_str() == "hello"

    def test_bytes(self):
        v = GValue.bytes_(b"hello")
        assert v.as_bytes() == b"hello"

    def test_time(self):
        dt = datetime(2025, 1, 13, 12, 0, 0, tzinfo=timezone.utc)
        v = GValue.time(dt)
        assert v.as_time() == dt

    def test_id(self):
        v = GValue.id("user", "123")
        ref = v.as_id()
        assert ref.prefix == "user"
        assert ref.value == "123"

    def test_list(self):
        v = GValue.list_(GValue.int_(1), GValue.int_(2), GValue.int_(3))
        lst = v.as_list()
        assert len(lst) == 3
        assert lst[0].as_int() == 1

    def test_map(self):
        v = GValue.map_(
            MapEntry("a", GValue.int_(1)),
            MapEntry("b", GValue.int_(2))
        )
        assert v.get("a").as_int() == 1
        assert v.get("b").as_int() == 2

    def test_struct(self):
        v = GValue.struct("Team",
            MapEntry("name", GValue.str_("Arsenal")),
            MapEntry("rank", GValue.int_(1))
        )
        sv = v.as_struct()
        assert sv.type_name == "Team"
        assert v.get("name").as_str() == "Arsenal"

    def test_sum(self):
        v = GValue.sum("Some", GValue.int_(42))
        sm = v.as_sum()
        assert sm.tag == "Some"
        assert sm.value.as_int() == 42

    def test_shorthand_g(self):
        v = g.struct("Match",
            field("home", g.str("Arsenal")),
            field("away", g.str("Liverpool")),
            field("score", g.list(g.int(2), g.int(1)))
        )
        assert v.get("home").as_str() == "Arsenal"


class TestCanonicalizeLoose:
    """Tests for loose canonicalization."""

    def test_null(self):
        assert emit(GValue.null()) == "_"

    def test_null_symbol(self):
        opts = LooseCanonOpts(null_style=NullStyle.SYMBOL)
        assert canonicalize_loose(GValue.null(), opts) == "∅"

    def test_bool(self):
        assert emit(GValue.bool_(True)) == "t"
        assert emit(GValue.bool_(False)) == "f"

    def test_int(self):
        assert emit(GValue.int_(42)) == "42"
        assert emit(GValue.int_(0)) == "0"
        assert emit(GValue.int_(-7)) == "-7"

    def test_float(self):
        assert emit(GValue.float_(3.14)) == "3.14"
        assert emit(GValue.float_(0.0)) == "0"

    def test_str_bare(self):
        assert emit(GValue.str_("hello")) == "hello"
        assert emit(GValue.str_("foo_bar")) == "foo_bar"

    def test_str_quoted(self):
        assert emit(GValue.str_("hello world")) == '"hello world"'
        assert emit(GValue.str_('say "hi"')) == '"say \\"hi\\""'

    def test_str_reserved(self):
        # Reserved words must be quoted
        assert emit(GValue.str_("t")) == '"t"'
        assert emit(GValue.str_("f")) == '"f"'
        assert emit(GValue.str_("null")) == '"null"'

    def test_list_empty(self):
        assert emit(GValue.list_()) == "[]"

    def test_list(self):
        v = GValue.list_(GValue.int_(1), GValue.int_(2), GValue.int_(3))
        assert emit(v) == "[1 2 3]"

    def test_map_empty(self):
        assert emit(GValue.map_()) == "{}"

    def test_map_sorted(self):
        v = GValue.map_(
            MapEntry("z", GValue.int_(3)),
            MapEntry("a", GValue.int_(1)),
            MapEntry("m", GValue.int_(2))
        )
        # Keys should be sorted
        assert emit(v) == "{a=1 m=2 z=3}"

    def test_struct(self):
        v = GValue.struct("Team",
            MapEntry("name", GValue.str_("Arsenal")),
            MapEntry("rank", GValue.int_(1))
        )
        assert emit(v) == "Team{name=Arsenal rank=1}"

    def test_id(self):
        assert emit(GValue.id("t", "ARS")) == "^t:ARS"
        assert emit(GValue.id("", "123")) == "^123"


class TestParse:
    """Tests for parsing."""

    def test_null(self):
        v = parse("∅")
        assert v.is_null()

    def test_null_underscore(self):
        v = parse("_")
        assert v.is_null()

    def test_bool(self):
        assert parse("t").as_bool() == True
        assert parse("f").as_bool() == False
        assert parse("true").as_bool() == True
        assert parse("false").as_bool() == False

    def test_int(self):
        assert parse("42").as_int() == 42
        assert parse("-7").as_int() == -7

    def test_float(self):
        assert abs(parse("3.14").as_float() - 3.14) < 0.001

    def test_str_bare(self):
        assert parse("hello").as_str() == "hello"

    def test_str_quoted(self):
        assert parse('"hello world"').as_str() == "hello world"
        assert parse('"say \\"hi\\""').as_str() == 'say "hi"'

    def test_list(self):
        v = parse("[1 2 3]")
        lst = v.as_list()
        assert len(lst) == 3
        assert lst[0].as_int() == 1

    def test_map(self):
        v = parse("{a=1 b=2}")
        assert v.get("a").as_int() == 1
        assert v.get("b").as_int() == 2

    def test_struct(self):
        v = parse("Team{name=Arsenal rank=1}")
        sv = v.as_struct()
        assert sv.type_name == "Team"
        assert v.get("name").as_str() == "Arsenal"

    def test_id(self):
        v = parse("^t:ARS")
        ref = v.as_id()
        assert ref.prefix == "t"
        assert ref.value == "ARS"

    def test_roundtrip(self):
        original = "{action=search max_results=10 query=\"weather in NYC\"}"
        v = parse(original)
        emitted = emit(v)
        v2 = parse(emitted)
        assert equal_loose(v, v2)


class TestJSONBridge:
    """Tests for JSON conversion."""

    def test_from_json_simple(self):
        data = {"name": "Alice", "age": 30}
        v = from_json(data)
        assert v.get("name").as_str() == "Alice"
        assert v.get("age").as_int() == 30

    def test_to_json(self):
        v = GValue.map_(
            MapEntry("name", GValue.str_("Alice")),
            MapEntry("age", GValue.int_(30))
        )
        data = to_json(v)
        assert data["name"] == "Alice"
        assert data["age"] == 30

    def test_json_to_glyph(self):
        data = {"action": "search", "query": "weather", "max_results": 10}
        text = json_to_glyph(data)
        # Should be valid GLYPH
        v = parse(text)
        assert v.get("action").as_str() == "search"


class TestFingerprint:
    """Tests for fingerprinting."""

    def test_fingerprint_deterministic(self):
        v = GValue.map_(
            MapEntry("a", GValue.int_(1)),
            MapEntry("b", GValue.int_(2))
        )
        fp1 = fingerprint_loose(v)
        fp2 = fingerprint_loose(v)
        assert fp1 == fp2

    def test_fingerprint_order_independent(self):
        v1 = GValue.map_(
            MapEntry("a", GValue.int_(1)),
            MapEntry("b", GValue.int_(2))
        )
        v2 = GValue.map_(
            MapEntry("b", GValue.int_(2)),
            MapEntry("a", GValue.int_(1))
        )
        assert fingerprint_loose(v1) == fingerprint_loose(v2)


class TestAutoTabular:
    """Tests for auto-tabular mode."""

    def test_tabular_output(self):
        rows = GValue.list_(
            GValue.map_(MapEntry("name", GValue.str_("Alice")), MapEntry("age", GValue.int_(30))),
            GValue.map_(MapEntry("name", GValue.str_("Bob")), MapEntry("age", GValue.int_(25))),
            GValue.map_(MapEntry("name", GValue.str_("Carol")), MapEntry("age", GValue.int_(35))),
        )
        text = emit(rows)
        # Should produce tabular format
        assert "@tab" in text
        assert "@end" in text

    def test_tabular_roundtrip(self):
        rows = GValue.list_(
            GValue.map_(MapEntry("x", GValue.int_(1)), MapEntry("y", GValue.int_(2))),
            GValue.map_(MapEntry("x", GValue.int_(3)), MapEntry("y", GValue.int_(4))),
            GValue.map_(MapEntry("x", GValue.int_(5)), MapEntry("y", GValue.int_(6))),
        )
        text = emit(rows)
        v = parse(text)
        assert len(v) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
