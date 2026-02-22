"""
GraphCowrie-Stream - Streaming Graph Event Format.

This module provides a streaming format for graph mutations (nodes, edges, triples).
Each event is encoded as a complete frame, allowing efficient streaming and compression.

Usage:
    from cowrie.graph import StreamWriter, StreamReader, NodeEvent, EdgeEvent

    # Write events
    writer = StreamWriter()
    writer.write_node(NodeEvent(op=Op.UPSERT, id="n1", labels=["Person"],
                                props={"name": "Alice"}))
    writer.write_edge(EdgeEvent(op=Op.UPSERT, label="KNOWS",
                                from_id="n1", to_id="n2"))
    data = writer.getvalue()

    # Read events
    for event in StreamReader(data):
        if event.kind == EventKind.NODE:
            print(event.node)
        elif event.kind == EventKind.EDGE:
            print(event.edge)

Wire format:
    Magic:     'S' 'J' 'G' 'S'  (4 bytes)
    Version:   0x01             (1 byte)
    Flags:     bitfield         (1 byte)
    FieldDict: [count:uvarint][entries...]
    LabelDict: [count:uvarint][entries...]
    PredDict:  [count:uvarint][entries...]  (optional)
    Frames:    [len:u32][frameBody:Cowrie]...
"""

from .types import (
    # Event types
    EventKind, Op,
    Event, NodeEvent, EdgeEvent, TripleEvent,
    # RDF types
    TermKind, RDFTerm,
    # Stream header
    StreamHeader,
)
from .stream import (
    StreamWriter,
    StreamReader,
)

__all__ = [
    # Types
    "EventKind", "Op",
    "Event", "NodeEvent", "EdgeEvent", "TripleEvent",
    "TermKind", "RDFTerm",
    "StreamHeader",
    # IO
    "StreamWriter", "StreamReader",
]
