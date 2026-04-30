"""
GraphCowrie-Stream Types - Event types for streaming graph format.

This module defines:
  - EventKind: Node, Edge, Triple
  - Op: Upsert, Delete
  - Event structures: NodeEvent, EdgeEvent, TripleEvent
  - RDF types: TermKind, RDFTerm
  - StreamHeader for dictionary encoding
"""

from enum import IntEnum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Wire format constants
MAGIC = b'SJGS'
VERSION = 1

# Stream flags
FLAG_HAS_PRED_DICT = 0x01
FLAG_COMPRESSED = 0x02
FLAG_HAS_TIMESTAMP = 0x04


class EventKind(IntEnum):
    """Type of graph event."""
    NODE = 0    # Node upsert/delete
    EDGE = 1    # Edge upsert/delete
    TRIPLE = 2  # RDF triple assert/retract


class Op(IntEnum):
    """Operation type."""
    UPSERT = 0  # Create or update (assert for triples)
    DELETE = 1  # Delete (retract for triples)


class TermKind(IntEnum):
    """Type of RDF term."""
    IRI = 0      # IRI reference
    BNODE = 1    # Blank node
    LITERAL = 2  # Literal value


@dataclass
class RDFTerm:
    """RDF term: subject, predicate, or object in a triple."""
    kind: TermKind
    value: str          # IRI string, blank node ID, or literal value
    datatype: str = ""  # Datatype IRI for literals (empty = plain literal)
    lang: str = ""      # Language tag for literals (e.g., "en", "fr")

    @classmethod
    def iri(cls, iri: str) -> 'RDFTerm':
        """Create an IRI term."""
        return cls(kind=TermKind.IRI, value=iri)

    @classmethod
    def bnode(cls, id: str) -> 'RDFTerm':
        """Create a blank node term."""
        return cls(kind=TermKind.BNODE, value=id)

    @classmethod
    def literal(cls, value: str, datatype: str = "", lang: str = "") -> 'RDFTerm':
        """Create a literal term."""
        return cls(kind=TermKind.LITERAL, value=value, datatype=datatype, lang=lang)


@dataclass
class NodeEvent:
    """Node creation, update, or deletion event."""
    op: Op
    id: str                           # Node identifier
    labels: List[str] = field(default_factory=list)  # Node labels
    props: Dict[str, Any] = field(default_factory=dict)  # Properties
    timestamp: int = 0                # Optional: nanoseconds since epoch


@dataclass
class EdgeEvent:
    """Edge creation, update, or deletion event."""
    op: Op
    label: str                        # Edge type/label (e.g., "KNOWS")
    from_id: str                      # Source node ID
    to_id: str                        # Target node ID
    id: str = ""                      # Edge identifier (optional)
    props: Dict[str, Any] = field(default_factory=dict)  # Properties
    timestamp: int = 0                # Optional: nanoseconds since epoch


@dataclass
class TripleEvent:
    """RDF triple assertion or retraction event."""
    op: Op                            # UPSERT = assert, DELETE = retract
    subject: RDFTerm                  # Subject (IRI or BNode)
    predicate: str                    # Predicate IRI
    object: RDFTerm                   # Object (IRI, BNode, or Literal)
    graph: str = ""                   # Named graph IRI (empty = default graph)
    timestamp: int = 0                # Optional: nanoseconds since epoch


@dataclass
class Event:
    """Union type for all event kinds."""
    kind: EventKind
    node: Optional[NodeEvent] = None
    edge: Optional[EdgeEvent] = None
    triple: Optional[TripleEvent] = None

    @classmethod
    def from_node(cls, node: NodeEvent) -> 'Event':
        return cls(kind=EventKind.NODE, node=node)

    @classmethod
    def from_edge(cls, edge: EdgeEvent) -> 'Event':
        return cls(kind=EventKind.EDGE, edge=edge)

    @classmethod
    def from_triple(cls, triple: TripleEvent) -> 'Event':
        return cls(kind=EventKind.TRIPLE, triple=triple)


class StreamHeader:
    """Header containing dictionaries for field names, labels, and predicates."""

    def __init__(self):
        self.flags: int = 0
        self.field_dict: List[str] = []
        self.label_dict: List[str] = []
        self.pred_dict: List[str] = []
        self._field_lookup: Dict[str, int] = {}
        self._label_lookup: Dict[str, int] = {}
        self._pred_lookup: Dict[str, int] = {}

    def add_field(self, field: str) -> int:
        """Add a field to the dictionary and return its index."""
        if field in self._field_lookup:
            return self._field_lookup[field]
        idx = len(self.field_dict)
        self.field_dict.append(field)
        self._field_lookup[field] = idx
        return idx

    def add_label(self, label: str) -> int:
        """Add a label to the dictionary and return its index."""
        if label in self._label_lookup:
            return self._label_lookup[label]
        idx = len(self.label_dict)
        self.label_dict.append(label)
        self._label_lookup[label] = idx
        return idx

    def add_predicate(self, pred: str) -> int:
        """Add a predicate to the dictionary and return its index."""
        self.flags |= FLAG_HAS_PRED_DICT
        if pred in self._pred_lookup:
            return self._pred_lookup[pred]
        idx = len(self.pred_dict)
        self.pred_dict.append(pred)
        self._pred_lookup[pred] = idx
        return idx

    def get_field(self, idx: int) -> str:
        """Get field name at index."""
        if 0 <= idx < len(self.field_dict):
            return self.field_dict[idx]
        return ""

    def get_label(self, idx: int) -> str:
        """Get label at index."""
        if 0 <= idx < len(self.label_dict):
            return self.label_dict[idx]
        return ""

    def get_predicate(self, idx: int) -> str:
        """Get predicate at index."""
        if 0 <= idx < len(self.pred_dict):
            return self.pred_dict[idx]
        return ""


# Common XSD datatypes
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
XSD_BOOLEAN = "http://www.w3.org/2001/XMLSchema#boolean"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
XSD_DOUBLE = "http://www.w3.org/2001/XMLSchema#double"
XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"

# Common RDF namespace IRIs
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
