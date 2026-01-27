## Context
The current WAL records page-level changes (leaf/internal inserts and splits). Recovery replays those records by
mutating pages directly, which requires page-id mapping and split logging correctness. This has led to failures in
COW tracking and replay ordering. WiredTiger logs logical operations and replays them via normal B-tree updates,
avoiding page-id coupling.

## Goals / Non-Goals
- Goals:
  - Align WAL and recovery with WiredTiger-style logical replay.
  - Remove page-id mapping and split record dependencies.
  - Keep checkpoint/COW semantics intact for normal operation.
  - Preserve idempotent recovery behavior.
- Non-Goals:
  - Full transaction logging, rollback-to-stable, or history store.
  - Multi-file or multi-tree recovery orchestration.

## Decisions
- Log logical operations (Put/Delete) rather than physical page edits.
- Replay WAL by calling BTree write APIs with WAL disabled, so splits/root changes are handled by normal logic.
- Maintain checkpoint LSN as the recovery start bound.
- Treat Put as upsert; treat Delete of missing keys as success (idempotence).
- Bump WAL format version and define compatibility behavior.
  - WAL version mismatches are treated as recovery errors; since this is unreleased, we do not support migration.
  - Open continues after recovery failure, which effectively skips incompatible WAL with a warning.
- Recovery does not auto-checkpoint; WAL is retained until an explicit checkpoint.

## Alternatives Considered
- Keep physical WAL and add stable page-id mapping: complex and error-prone.
- Log full page images: simpler replay, larger WAL and harder to validate.
- Continue in-place page replay: keeps drift issues and split ordering hazards.

## Risks / Trade-offs
- Logical replay can be slower than page-image replay.
- WAL format change requires compatibility handling.
- Recovery checkpoint policy must be explicit to avoid repeated replays.

## Migration Plan
- Bump WAL version for logical records.
- Do not implement migration or backwards compatibility (unreleased).
- On open, incompatible WAL versions are rejected during recovery and the database opens with a warning.
- Document the behavior in docs/wal_recovery_implementation.md.

## Open Questions
- Do we want delete/truncate logging now or only put/overwrite?
