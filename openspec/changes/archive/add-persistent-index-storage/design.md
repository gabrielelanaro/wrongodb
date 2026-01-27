## Context
Secondary indexes are currently maintained in-memory as maps from (field, scalar) -> offsets and rebuilt at startup by replaying the append-only log. The project already has a durable B+tree implementation with copy-on-write and checkpoint semantics for the `_id` index (id_index).

## Goals / Non-Goals
- Goals:
  - Persist secondary index data and load it on open
  - Keep index maintenance consistent with inserts, updates, and deletes
  - Support basic equality lookups for top-level scalar fields
- Non-Goals:
  - Compound, multikey, or range indexes
  - Query planner cost modeling or sort/coverage
  - Multi-document transactions or concurrency control

## Decisions
- Decision: One B+tree per secondary index, stored in a separate file per collection+field.
  - Rationale: Aligns with current storage primitives and avoids a new multiplexed index file format.
- Decision: Encode index key as (scalar key, record offset) to support duplicates without a posting list structure.
  - Rationale: Simpler than posting lists and works with existing B+tree get/scan APIs.

## Risks / Trade-offs
- More files on disk per collection/field
- Duplicate keys increase B+tree size; later work may add posting lists

## Migration Plan
- On createIndex, build the B+tree by scanning existing records and persisting keys
- On open, load index metadata and open B+tree files; if missing, fall back to rebuild

## Open Questions
- Should index metadata live in a single per-collection manifest or be inferred from files?
- Should delete tombstones in the data log trigger index cleanup eagerly or lazily?
