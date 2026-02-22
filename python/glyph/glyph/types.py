"""
GLYPH Core Types

GValue is the universal value container for GLYPH data.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union
import copy


class GType(Enum):
    """GLYPH value types."""
    NULL = "null"
    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BYTES = "bytes"
    TIME = "time"
    ID = "id"
    LIST = "list"
    MAP = "map"
    STRUCT = "struct"
    SUM = "sum"


@dataclass
class RefID:
    """Reference ID with prefix and value."""
    prefix: str
    value: str

    def __str__(self) -> str:
        if self.prefix:
            return f"^{self.prefix}:{self.value}"
        return f"^{self.value}"


@dataclass
class MapEntry:
    """Key-value pair for maps and structs."""
    key: str
    value: "GValue"


@dataclass
class StructValue:
    """Struct with type name and fields."""
    type_name: str
    fields: List[MapEntry]


@dataclass
class SumValue:
    """Tagged union (sum type)."""
    tag: str
    value: Optional["GValue"]


class GValue:
    """
    Universal value container for GLYPH data.

    Supports: null, bool, int, float, str, bytes, time, id, list, map, struct, sum
    """

    __slots__ = (
        '_type', '_bool', '_int', '_float', '_str', '_bytes',
        '_time', '_id', '_list', '_map', '_struct', '_sum'
    )

    def __init__(self, gtype: GType):
        self._type = gtype
        self._bool: Optional[bool] = None
        self._int: Optional[int] = None
        self._float: Optional[float] = None
        self._str: Optional[str] = None
        self._bytes: Optional[bytes] = None
        self._time: Optional[datetime] = None
        self._id: Optional[RefID] = None
        self._list: Optional[List[GValue]] = None
        self._map: Optional[List[MapEntry]] = None
        self._struct: Optional[StructValue] = None
        self._sum: Optional[SumValue] = None

    @property
    def type(self) -> GType:
        return self._type

    # ============================================================
    # Constructors
    # ============================================================

    @staticmethod
    def null() -> "GValue":
        return GValue(GType.NULL)

    @staticmethod
    def bool_(v: bool) -> "GValue":
        gv = GValue(GType.BOOL)
        gv._bool = v
        return gv

    @staticmethod
    def int_(v: int) -> "GValue":
        gv = GValue(GType.INT)
        gv._int = int(v)
        return gv

    @staticmethod
    def float_(v: float) -> "GValue":
        gv = GValue(GType.FLOAT)
        gv._float = float(v)
        return gv

    @staticmethod
    def str_(v: str) -> "GValue":
        gv = GValue(GType.STR)
        gv._str = v
        return gv

    @staticmethod
    def bytes_(v: bytes) -> "GValue":
        gv = GValue(GType.BYTES)
        gv._bytes = v
        return gv

    @staticmethod
    def time(v: datetime) -> "GValue":
        gv = GValue(GType.TIME)
        gv._time = v
        return gv

    @staticmethod
    def id(prefix: str, value: str) -> "GValue":
        gv = GValue(GType.ID)
        gv._id = RefID(prefix, value)
        return gv

    @staticmethod
    def id_from_ref(ref: RefID) -> "GValue":
        gv = GValue(GType.ID)
        gv._id = ref
        return gv

    @staticmethod
    def list_(*values: "GValue") -> "GValue":
        gv = GValue(GType.LIST)
        gv._list = list(values)
        return gv

    @staticmethod
    def map_(*entries: MapEntry) -> "GValue":
        gv = GValue(GType.MAP)
        gv._map = list(entries)
        return gv

    @staticmethod
    def struct(type_name: str, *fields: MapEntry) -> "GValue":
        gv = GValue(GType.STRUCT)
        gv._struct = StructValue(type_name, list(fields))
        return gv

    @staticmethod
    def sum(tag: str, value: Optional["GValue"]) -> "GValue":
        gv = GValue(GType.SUM)
        gv._sum = SumValue(tag, value)
        return gv

    # ============================================================
    # Accessors
    # ============================================================

    def is_null(self) -> bool:
        return self._type == GType.NULL

    def as_bool(self) -> bool:
        if self._type != GType.BOOL:
            raise TypeError("not a bool")
        return self._bool  # type: ignore

    def as_int(self) -> int:
        if self._type != GType.INT:
            raise TypeError("not an int")
        return self._int  # type: ignore

    def as_float(self) -> float:
        if self._type != GType.FLOAT:
            raise TypeError("not a float")
        return self._float  # type: ignore

    def as_str(self) -> str:
        if self._type != GType.STR:
            raise TypeError("not a str")
        return self._str  # type: ignore

    def as_bytes(self) -> bytes:
        if self._type != GType.BYTES:
            raise TypeError("not bytes")
        return self._bytes  # type: ignore

    def as_time(self) -> datetime:
        if self._type != GType.TIME:
            raise TypeError("not a time")
        return self._time  # type: ignore

    def as_id(self) -> RefID:
        if self._type != GType.ID:
            raise TypeError("not an id")
        return self._id  # type: ignore

    def as_list(self) -> List["GValue"]:
        if self._type != GType.LIST:
            raise TypeError("not a list")
        return self._list  # type: ignore

    def as_map(self) -> List[MapEntry]:
        if self._type != GType.MAP:
            raise TypeError("not a map")
        return self._map  # type: ignore

    def as_struct(self) -> StructValue:
        if self._type != GType.STRUCT:
            raise TypeError("not a struct")
        return self._struct  # type: ignore

    def as_sum(self) -> SumValue:
        if self._type != GType.SUM:
            raise TypeError("not a sum")
        return self._sum  # type: ignore

    def as_number(self) -> Union[int, float]:
        """Get numeric value (works for int or float)."""
        if self._type == GType.INT:
            return self._int  # type: ignore
        if self._type == GType.FLOAT:
            return self._float  # type: ignore
        raise TypeError("not a number")

    def get(self, key: str) -> Optional["GValue"]:
        """Get field from struct or map by key."""
        if self._type == GType.STRUCT:
            for f in self._struct.fields:  # type: ignore
                if f.key == key:
                    return f.value
            return None
        if self._type == GType.MAP:
            for e in self._map:  # type: ignore
                if e.key == key:
                    return e.value
            return None
        return None

    def index(self, i: int) -> "GValue":
        """Get element from list by index."""
        if self._type != GType.LIST:
            raise TypeError("not a list")
        if i < 0 or i >= len(self._list):  # type: ignore
            raise IndexError("index out of bounds")
        return self._list[i]  # type: ignore

    def __len__(self) -> int:
        """Get length of list, map, or struct fields."""
        if self._type == GType.LIST:
            return len(self._list)  # type: ignore
        if self._type == GType.MAP:
            return len(self._map)  # type: ignore
        if self._type == GType.STRUCT:
            return len(self._struct.fields)  # type: ignore
        return 0

    # ============================================================
    # Mutators
    # ============================================================

    def set(self, key: str, value: "GValue") -> None:
        """Set field on struct or map."""
        if self._type == GType.STRUCT:
            for i, f in enumerate(self._struct.fields):  # type: ignore
                if f.key == key:
                    self._struct.fields[i].value = value  # type: ignore
                    return
            self._struct.fields.append(MapEntry(key, value))  # type: ignore
        elif self._type == GType.MAP:
            for i, e in enumerate(self._map):  # type: ignore
                if e.key == key:
                    self._map[i].value = value  # type: ignore
                    return
            self._map.append(MapEntry(key, value))  # type: ignore
        else:
            raise TypeError("cannot set on non-struct/map")

    def append(self, value: "GValue") -> None:
        """Append to list."""
        if self._type != GType.LIST:
            raise TypeError("cannot append to non-list")
        self._list.append(value)  # type: ignore

    # ============================================================
    # Deep Copy
    # ============================================================

    def clone(self) -> "GValue":
        """Create a deep copy of this value."""
        if self._type == GType.NULL:
            return GValue.null()
        elif self._type == GType.BOOL:
            return GValue.bool_(self._bool)  # type: ignore
        elif self._type == GType.INT:
            return GValue.int_(self._int)  # type: ignore
        elif self._type == GType.FLOAT:
            return GValue.float_(self._float)  # type: ignore
        elif self._type == GType.STR:
            return GValue.str_(self._str)  # type: ignore
        elif self._type == GType.BYTES:
            return GValue.bytes_(bytes(self._bytes))  # type: ignore
        elif self._type == GType.TIME:
            return GValue.time(self._time)  # type: ignore
        elif self._type == GType.ID:
            return GValue.id(self._id.prefix, self._id.value)  # type: ignore
        elif self._type == GType.LIST:
            return GValue.list_(*[v.clone() for v in self._list])  # type: ignore
        elif self._type == GType.MAP:
            return GValue.map_(*[MapEntry(e.key, e.value.clone()) for e in self._map])  # type: ignore
        elif self._type == GType.STRUCT:
            return GValue.struct(
                self._struct.type_name,  # type: ignore
                *[MapEntry(f.key, f.value.clone()) for f in self._struct.fields]  # type: ignore
            )
        elif self._type == GType.SUM:
            return GValue.sum(
                self._sum.tag,  # type: ignore
                self._sum.value.clone() if self._sum.value else None  # type: ignore
            )
        raise ValueError(f"unknown type: {self._type}")

    def __repr__(self) -> str:
        if self._type == GType.NULL:
            return "GValue.null()"
        elif self._type == GType.BOOL:
            return f"GValue.bool_({self._bool})"
        elif self._type == GType.INT:
            return f"GValue.int_({self._int})"
        elif self._type == GType.FLOAT:
            return f"GValue.float_({self._float})"
        elif self._type == GType.STR:
            return f"GValue.str_({self._str!r})"
        elif self._type == GType.BYTES:
            return f"GValue.bytes_({self._bytes!r})"
        elif self._type == GType.TIME:
            return f"GValue.time({self._time!r})"
        elif self._type == GType.ID:
            return f"GValue.id({self._id.prefix!r}, {self._id.value!r})"
        elif self._type == GType.LIST:
            return f"GValue.list_({', '.join(repr(v) for v in self._list)})"  # type: ignore
        elif self._type == GType.MAP:
            return f"GValue.map_(...)"
        elif self._type == GType.STRUCT:
            return f"GValue.struct({self._struct.type_name!r}, ...)"  # type: ignore
        elif self._type == GType.SUM:
            return f"GValue.sum({self._sum.tag!r}, ...)"  # type: ignore
        return f"GValue({self._type})"


# ============================================================
# Helper Functions
# ============================================================

def field(key: str, value: GValue) -> MapEntry:
    """Create a field entry for struct construction."""
    return MapEntry(key, value)


# Shorthand constructors
class G:
    """Shorthand constructors for GValue."""

    @staticmethod
    def null() -> GValue:
        return GValue.null()

    @staticmethod
    def bool(v: bool) -> GValue:
        return GValue.bool_(v)

    @staticmethod
    def int(v: int) -> GValue:
        return GValue.int_(v)

    @staticmethod
    def float(v: float) -> GValue:
        return GValue.float_(v)

    @staticmethod
    def str(v: str) -> GValue:
        return GValue.str_(v)

    @staticmethod
    def bytes(v: bytes) -> GValue:
        return GValue.bytes_(v)

    @staticmethod
    def time(v: datetime) -> GValue:
        return GValue.time(v)

    @staticmethod
    def id(prefix: str, value: str) -> GValue:
        return GValue.id(prefix, value)

    @staticmethod
    def list(*values: GValue) -> GValue:
        return GValue.list_(*values)

    @staticmethod
    def map(*entries: MapEntry) -> GValue:
        return GValue.map_(*entries)

    @staticmethod
    def struct(type_name: str, *fields: MapEntry) -> GValue:
        return GValue.struct(type_name, *fields)

    @staticmethod
    def sum(tag: str, value: Optional[GValue]) -> GValue:
        return GValue.sum(tag, value)


g = G()
