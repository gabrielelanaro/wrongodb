## Context
Option B is checkpoint-first durability without WAL. The current engine overwrites pages in place and updates the root pointer immediately, which can corrupt reachable pages on crash. We need crash-consistent snapshots with minimal complexity.

## Goals / Non-Goals
- Goals:
  - Crash-consistent checkpoints without WAL.
  - Preserve a stable on-disk root until checkpoint commit.
  - Keep the single-file layout and current B+tree data model.
- Non-Goals:
  - Transactional durability for every write.
  - MVCC, background compaction, or sophisticated space management.
  - WiredTiger-level cache/reconciliation.

## Decisions
- **Copy-on-write updates**: write modified pages to new blocks; never overwrite blocks reachable from the last durable checkpoint.
- **Stable vs working root**: keep the last checkpoint root stable on disk while mutations advance an in-memory working root.
- **Atomic root swap**: persist checkpoint metadata using dual root slots with generation numbers and CRC; open selects the latest valid slot.
- **Retired blocks**: record replaced block IDs during updates and add them to the free list only after a successful checkpoint (leaks on crash are acceptable for now). Retired-block lists are not persisted in this phase.
- **Checkpoint trigger**: checkpoints are explicit API calls in this phase (no implicit checkpoint per write).
- **Metadata/turtle integration**: keep the header root pointing directly at the primary B+tree for now; when metadata/turtle is added, repoint the header root to the metadata root, and have the metadata table store the primary B+tree root.

## Alternatives considered
- WAL + in-place writes (Option A): smaller change but adds logging and recovery complexity.
- Single-slot header updates: simplest but unsafe on torn writes.
- Full tree-walk GC after checkpoint: correct reclamation but expensive without a cache.

## Risks / Trade-offs
- Write amplification: each update copies the path to root.
- Space growth: retired blocks may leak until future GC/compaction.
- Format change: dual header slots require a version bump.

## Open Questions
