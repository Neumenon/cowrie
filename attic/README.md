# Cowrie — attic

Parked features. Not built or tested by default.

## Wire-format reservation

Tag codes `0x30` (AdjList), `0x31` (RichText), `0x32` (Delta), and `0x39`
(GraphShard) are reserved (deprecated). Decoders MUST skip the
length-prefixed payload silently. Encoders MUST NOT emit them.

## What lives here

- `go/graph/` — graph value types (Node 0x35, Edge 0x36, NodeBatch 0x37,
  EdgeBatch 0x38, GraphShard 0x39, AdjList 0x30) and frame helpers.
- `go/gnn/` — GNN algorithms, edge tables, batching helpers, and the
  Python sibling package (`go/gnn/python/`).
- `go/ld/` — linked-data context resolver / JSON-LD-style helpers.
- `go/delta/` — Delta tag (0x32) diff/apply/store.
- `go/hints.go`, `go/column_reader.go`, `go/hints_test.go` — column-wise
  access via the now-reserved `FlagHasColumnHints` (header bit 3) and the
  `ColumnHints` block.
- `docs/STATE_SYNC_AND_PATCHES.md` — research note for state-sync work that
  is no longer part of the lead pitch.

To revive any of these, restore the files and re-introduce the
encoder/decoder/JSON-bridge/fingerprint switch arms (search the cut commit
for the original integration sites).
