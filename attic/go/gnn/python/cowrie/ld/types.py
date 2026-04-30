"""
Cowrie-LD Types - Type definitions for binary JSON-LD codec.

This module defines:
  - IRI: Internationalized Resource Identifier
  - BNode: Blank node (anonymous resource)
  - Literal: RDF literal with optional datatype and language
  - TermEntry: JSON-LD term mapping
  - LDDocument: Complete Cowrie-LD document
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, NewType
from enum import IntFlag
import sys

# Add parent module path
sys.path.insert(0, str(__file__).rsplit('/', 2)[0])

from cowrie.types import Value


# Wire format constants
MAGIC = b'SJLD'
VERSION = 1

# Extended type tags for Cowrie-LD
TAG_IRI = 0x11    # [iriID:uvarint] -> IRIs[iriID]
TAG_BNODE = 0x12  # [len:uvarint][utf8]


# Type aliases
IRI = NewType('IRI', str)
BNode = NewType('BNode', str)


class TermFlags(IntFlag):
    """Container and type semantics for JSON-LD terms."""
    NONE = 0x00
    LIST = 0x01       # @container: @list
    SET = 0x02        # @container: @set
    LANGUAGE = 0x04   # @container: @language
    INDEX = 0x08      # @container: @index
    ID = 0x10         # @type: @id
    VOCAB = 0x20      # @type: @vocab


@dataclass
class Literal:
    """RDF literal with optional datatype and language tag."""
    value: Any          # The underlying value
    datatype: IRI = IRI("")  # Datatype IRI (empty = plain literal)
    lang: str = ""      # Language tag (e.g., "en", "fr-CA")


@dataclass
class TermEntry:
    """Maps a JSON-LD term to its IRI and semantics."""
    term: str           # The short name used in JSON (e.g., "name")
    iri: IRI           # The full IRI (e.g., "http://schema.org/name")
    flags: TermFlags = TermFlags.NONE


@dataclass
class LDValue:
    """Wraps a Cowrie value with optional LD metadata."""
    value: Value
    is_iri: bool = False
    iri_id: int = 0          # Index into IRIs table (if is_iri)
    is_bnode: bool = False
    bnode_id: str = ""       # Blank node identifier (if is_bnode)


class LDDocument:
    """
    Complete Cowrie-LD document with context and value tree.

    Usage:
        doc = LDDocument()
        doc.add_term("name", IRI("http://schema.org/name"))

        # Add IRI to table and create value reference
        iri_val = doc.iri_value(IRI("http://example.org/alice"))

        # Build root value
        doc.root = cowrie.Object(
            ("@id", iri_val),
            ("name", cowrie.String("Alice")),
        )
    """

    def __init__(self):
        self.field_dict: List[str] = []        # Object key dictionary
        self.terms: List[TermEntry] = []       # JSON-LD term definitions
        self.iris: List[IRI] = []              # IRI table
        self.datatypes: List[IRI] = []         # Datatype IRI table
        self.root: Optional[Value] = None

        # Lookup tables for fast indexing
        self._field_lookup: Dict[str, int] = {}
        self._iri_lookup: Dict[str, int] = {}
        self._datatype_lookup: Dict[str, int] = {}

    def add_term(self, term: str, iri: IRI, flags: TermFlags = TermFlags.NONE):
        """Add a term mapping to the document."""
        self.terms.append(TermEntry(term=term, iri=iri, flags=flags))

    def add_iri(self, iri: IRI) -> int:
        """Add an IRI to the table and return its index."""
        iri_str = str(iri)
        if iri_str in self._iri_lookup:
            return self._iri_lookup[iri_str]
        idx = len(self.iris)
        self.iris.append(iri)
        self._iri_lookup[iri_str] = idx
        return idx

    def add_datatype(self, dt: IRI) -> int:
        """Add a datatype IRI to the table and return its index."""
        dt_str = str(dt)
        if dt_str in self._datatype_lookup:
            return self._datatype_lookup[dt_str]
        idx = len(self.datatypes)
        self.datatypes.append(dt)
        self._datatype_lookup[dt_str] = idx
        return idx

    def add_field(self, field: str) -> int:
        """Add a field name to the dictionary and return its index."""
        if field in self._field_lookup:
            return self._field_lookup[field]
        idx = len(self.field_dict)
        self.field_dict.append(field)
        self._field_lookup[field] = idx
        return idx

    def get_iri(self, idx: int) -> Optional[IRI]:
        """Get IRI at the given index."""
        if 0 <= idx < len(self.iris):
            return self.iris[idx]
        return None

    def get_datatype(self, idx: int) -> Optional[IRI]:
        """Get datatype IRI at the given index."""
        if 0 <= idx < len(self.datatypes):
            return self.datatypes[idx]
        return None

    def get_field(self, idx: int) -> Optional[str]:
        """Get field name at the given index."""
        if 0 <= idx < len(self.field_dict):
            return self.field_dict[idx]
        return None

    def lookup_term(self, term: str) -> Optional[TermEntry]:
        """Find a term entry by its short name."""
        for entry in self.terms:
            if entry.term == term:
                return entry
        return None

    def lookup_term_by_iri(self, iri: IRI) -> Optional[TermEntry]:
        """Find a term entry by its IRI."""
        for entry in self.terms:
            if entry.iri == iri:
                return entry
        return None

    def iri_value(self, iri: IRI) -> LDValue:
        """Create an LDValue representing an IRI reference."""
        from cowrie.types import String
        idx = self.add_iri(iri)
        return LDValue(
            value=String(str(iri)),
            is_iri=True,
            iri_id=idx,
        )

    def bnode_value(self, id: str) -> LDValue:
        """Create an LDValue representing a blank node."""
        from cowrie.types import String
        return LDValue(
            value=String(id),
            is_bnode=True,
            bnode_id=id,
        )


# Common XSD datatypes
XSD_STRING = IRI("http://www.w3.org/2001/XMLSchema#string")
XSD_BOOLEAN = IRI("http://www.w3.org/2001/XMLSchema#boolean")
XSD_INTEGER = IRI("http://www.w3.org/2001/XMLSchema#integer")
XSD_DOUBLE = IRI("http://www.w3.org/2001/XMLSchema#double")
XSD_DECIMAL = IRI("http://www.w3.org/2001/XMLSchema#decimal")
XSD_DATETIME = IRI("http://www.w3.org/2001/XMLSchema#dateTime")
XSD_DATE = IRI("http://www.w3.org/2001/XMLSchema#date")
XSD_TIME = IRI("http://www.w3.org/2001/XMLSchema#time")
XSD_ANYURI = IRI("http://www.w3.org/2001/XMLSchema#anyURI")
XSD_BASE64 = IRI("http://www.w3.org/2001/XMLSchema#base64Binary")

# Common RDF namespace IRIs
RDF_TYPE = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
RDF_FIRST = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
RDF_REST = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
RDF_NIL = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
RDFS_LABEL = IRI("http://www.w3.org/2000/01/rdf-schema#label")
RDFS_RANGE = IRI("http://www.w3.org/2000/01/rdf-schema#range")
RDFS_DOMAIN = IRI("http://www.w3.org/2000/01/rdf-schema#domain")
