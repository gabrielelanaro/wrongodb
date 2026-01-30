## Context

Current architecture uses a hybrid approach: append-only log for documents with B-tree indexes. This was simple to implement but hits limitations:
- Updates create garbage (old document versions never reclaimed)
- Indexes store file offsets, preventing storage reorganization
- Startup requires full file scan

We already have a working B-tree (`BTree`, `BlockFile`, `Pager`) with COW and checkpoints. This change extends that to store documents directly.

## Goals / Non-Goals

**Goals:**
- Main table B-tree: `_id` → document
- Extent-based free space management (WiredTiger-style)
- Index format: store `_id` instead of offset
- Space reclamation after checkpoint
- Automatic migration from old format

**Non-Goals:**
- Multi-tree single file (keep separate files)
- MVCC/version chains (keep existing COW)
- Compression/encryption (future work)
- Async I/O

## Decisions

### Decision: Keep separate files per B-tree
**Rationale:** WiredTiger multiplexes multiple B-trees in one file with a shared block manager. This adds complexity (multi-tree checkpoint coordination). Since we don't need that complexity yet, keep separate files with independent block managers.

**Alternatives considered:**
- Single file with shared block manager: More complex, requires multi-tree atomic checkpoint
- Single file per document: Too many files, poor locality

### Decision: Best-fit allocation strategy
**Rationale:** Extent lists support both first-fit (lowest offset) and best-fit (smallest fitting). Best-fit reduces fragmentation by finding the tightest fit.

**Trade-off:** Slightly slower than first-fit (O(log n) skiplist search vs linear scan) but better space utilization.

### Decision: Skiplist for extent indexing
**Rationale:** Need two indexes on extents: by offset (for coalescing) and by size (for allocation). Skiplist provides O(log n) for both with simpler code than a balanced tree.

**Note:** Skiplist is in-memory only; rebuilt from serialized extent lists on open.

### Decision: BSON for key/value encoding
**Rationale:** Need to encode `_id` values (strings, ints, ObjectIds) and documents to bytes. BSON is standard for MongoDB-compatible databases, handles all JSON types, and has efficient binary representation.

**Alternatives considered:**
- JSON: Larger, slower parsing
- Custom format: More code to maintain
- MessagePack: Good alternative, but BSON is more MongoDB-native

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Migration data loss | High | Backup old files, verify checksums, atomic rename |
| Fragmentation | Medium | Extent coalescing, compaction pass in future |
| Memory usage (skiplist) | Low | Extents compact; rebuild on open |
| Performance regression | Medium | Benchmark each phase, keep simple free-list as fallback |

## Open Questions

- Should we keep the old free-list as a fallback for small allocations?
- What's the target extent size hint? (4KB blocks → 64KB extents?)
- Should we compress extent lists on disk?
