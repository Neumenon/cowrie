"""
Cowrie-LD - Binary JSON-LD Codec.

Cowrie-LD extends Cowrie v2 with:
  - IRI references (0x11 tag)
  - Blank nodes (0x12 tag)
  - Term tables for JSON-LD context mapping
  - Datatype tables for typed literals

Usage:
    from cowrie.ld import LDDocument, IRI, encode, decode

    # Create document with context
    doc = LDDocument()
    doc.add_term("name", IRI("http://schema.org/name"))
    doc.add_term("knows", IRI("http://schema.org/knows"))

    # Set root value with IRI references
    doc.root = cowrie.Object(
        ("@id", doc.iri_value(IRI("http://example.org/alice"))),
        ("name", cowrie.String("Alice")),
        ("knows", doc.iri_value(IRI("http://example.org/bob"))),
    )

    # Encode to binary
    data = encode(doc)

    # Decode back
    doc2 = decode(data)

Wire format:
    Magic:     'S' 'J' 'L' 'D'  (4 bytes)
    Version:   0x01             (1 byte)
    Flags:     0x00             (1 byte)
    FieldDict: [count:uvarint][entries...]
    Terms:     [count:uvarint][TermEntry...]
    IRIs:      [count:uvarint][entries...]
    Datatypes: [count:uvarint][entries...]
    RootValue: Cowrie v2 value tree
"""

from .types import (
    # Types
    IRI, BNode, Literal, TermEntry, TermFlags,
    LDDocument, LDValue,
    # Common datatypes
    XSD_STRING, XSD_BOOLEAN, XSD_INTEGER, XSD_DOUBLE,
    XSD_DECIMAL, XSD_DATETIME, XSD_DATE, XSD_TIME,
    XSD_ANYURI, XSD_BASE64,
    # Common RDF IRIs
    RDF_TYPE, RDF_FIRST, RDF_REST, RDF_NIL,
    RDFS_LABEL, RDFS_RANGE, RDFS_DOMAIN,
)
from .codec import encode, decode, encode_compressed, decode_compressed

__all__ = [
    # Types
    "IRI", "BNode", "Literal", "TermEntry", "TermFlags",
    "LDDocument", "LDValue",
    # Datatypes
    "XSD_STRING", "XSD_BOOLEAN", "XSD_INTEGER", "XSD_DOUBLE",
    "XSD_DECIMAL", "XSD_DATETIME", "XSD_DATE", "XSD_TIME",
    "XSD_ANYURI", "XSD_BASE64",
    # RDF
    "RDF_TYPE", "RDF_FIRST", "RDF_REST", "RDF_NIL",
    "RDFS_LABEL", "RDFS_RANGE", "RDFS_DOMAIN",
    # IO
    "encode", "decode",
    "encode_compressed", "decode_compressed",
]
