# Make Public Oplog Cursors Mongo-Faithful Enough For Follower Tailing

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document lives in `.codex/plans/public-oplog-cursors-mongo-faithful.md` and must be maintained in accordance with `.codex/PLANS.md`.

## Purpose / Big Picture

WrongoDB now serves Mongo-style server-side cursors through the public command surface. A client can run `find` on `local.oplog.rs` with `_id > last_seen`, receive a real non-zero cursor id, and continue reading with `getMore`. Ordinary `find` also uses a real server-side bookmark instead of dumping every result into `firstBatch`.

The observable proof is twofold. A normal collection query with `batchSize: 1` now returns one document and a live cursor id, and `getMore` returns the later batch. A tailable `awaitData` cursor on `local.oplog.rs` now blocks in `getMore` until another client commits a write, then returns the new oplog row.

## Progress

- [x] (2026-03-30 15:06Z) Read `.codex/PLANS.md` and inspected the current command, query, and replication code paths.
- [x] (2026-03-30 15:16Z) Verified that `src/server/commands/handlers/cursor.rs` was still a stub and that `src/document_query.rs` materialized complete result sets before truncating.
- [x] (2026-03-30 16:30Z) Added process-local cursor state, async command execution, and paged `find` planning with resume tokens.
- [x] (2026-03-30 16:45Z) Added oplog await notifications driven by committed oplog appends and wired tailable `getMore` for `local.oplog.rs`.
- [x] (2026-03-30 16:55Z) Added end-to-end protocol tests for live cursors, `killCursors`, and tailable oplog `awaitData`.
- [x] (2026-03-30 17:20Z) Updated docs, decisions, and this ExecPlan after the implementation passed `cargo test`.

## Surprises & Discoveries

- Observation: materialized command cursors cannot safely store WrongoDB's internal JSON document type because commands such as `listCollections` return BSON-only values like UUID binaries.
  Evidence: `listCollections` builds BSON documents with a binary UUID, so the materialized cursor state stores `Vec<bson::Document>` instead.

- Observation: the storage cursor primitives already had almost all of the mechanics needed for range continuation.
  Evidence: `TableCursor::set_range(...)` and `FileCursor::set_range(...)` were already present; the main missing piece was query-layer planning plus a server-side bookmark.

- Observation: the write-side wakeup must happen after commit, not from inside the oplog append call.
  Evidence: the final implementation threads the highest appended `OpTime` back through `CollectionWritePath` and `WriteOps`, then calls `OplogAwaitService::notify_committed(...)` only after `Session::with_transaction(...)` succeeds.

## Decision Log

- Decision: Use the public command path, not an internal replication RPC, as the oplog-read prerequisite.
  Rationale: MongoDB secondaries read the oplog through normal query machinery plus cursor behavior. Matching that shape teaches the right boundaries and avoids throwaway replication-only transport.
  Date/Author: 2026-03-30 / Codex

- Decision: Use a dedicated oplog await service backed by `tokio::sync::watch`.
  Rationale: This keeps `awaitData` wakeups event-driven and tied to committed oplog advancement instead of periodic polling.
  Date/Author: 2026-03-30 / Codex

- Decision: Reopen a fresh read transaction for each `getMore` batch.
  Rationale: This matches MongoDB's ordinary bookmark-based cursor behavior more closely than pinning one snapshot for the whole cursor lifetime.
  Date/Author: 2026-03-30 / Codex

- Decision: Keep `tailable` and `awaitData` limited to `local.oplog.rs`.
  Rationale: WrongoDB needed oplog tailing, not a generic capped-collection subsystem.
  Date/Author: 2026-03-30 / Codex

- Decision: Keep materialized command cursors for `listCollections`, `listIndexes`, and `aggregate`, but store raw BSON documents in those cursor states.
  Rationale: These commands already compute their full result eagerly, and BSON storage preserves command-only types cleanly.
  Date/Author: 2026-03-30 / Codex

## Outcomes & Retrospective

The implemented result matches the original goal. WrongoDB now has real server-side cursors for command-based `find`, `listCollections`, `listIndexes`, and `aggregate`. `getMore` resumes saved cursor state, `killCursors` removes it, `_id.$gt` and `_id.$gte` support oplog-style continuation, and `local.oplog.rs` supports oplog-only `tailable` plus `awaitData` behavior with post-commit wakeups.

What remains out of scope is still out of scope: there is no general sort support, no general query-operator engine, no exhaust cursor mode, no capped-collection subsystem, and no snapshot-stable cross-batch read guarantee. Those are separate pieces of MongoDB behavior and should be planned independently if needed.

## Context and Orientation

A cursor is server-side saved read state. The client only gets a `cursor.id`. A later `getMore` asks the same server node to continue from that bookmark. A tailable cursor stays alive when it reaches the current end of an append-only collection. `awaitData` means `getMore` may wait briefly for a later append instead of replying immediately with an empty batch.

The main files touched by this work are:

- `src/server/commands/mod.rs`, `src/server/commands/registry.rs`, and `src/server/commands/cursor_manager.rs` for async command execution and saved cursor state.
- `src/server/commands/handlers/crud.rs` and `src/server/commands/handlers/cursor.rs` for `find`, `getMore`, and `killCursors`.
- `src/document_query.rs` for paged reads, query planning, resume tokens, and `_id.$gt` / `_id.$gte`.
- `src/replication/await_service.rs`, `src/api/database_context.rs`, `src/collection_write_path.rs`, and `src/write_ops/executor.rs` for committed-oplog wakeups.
- `tests/server/protocol.rs` for the driver-visible acceptance coverage.

## Plan of Work

Milestone 1 introduced a process-local cursor manager and made command execution async with `async-trait`. That let `getMore` wait without blocking a Tokio worker thread.

Milestone 2 replaced the old “materialize everything, then truncate” `find` path with explicit `FindPlan`, `FindRequest`, `FindResumeToken`, and `FindPage` types in `src/document_query.rs`. The read path now supports table scans, exact `_id` lookups, `_id` range scans, and one-field equality index scans, and it resumes later batches from saved raw keys.

Milestone 3 added `OplogAwaitService` and threaded the last committed `OpTime` back out of the write path so a tailable oplog `getMore` can sleep until the oplog advances. The wakeup happens after commit, not from inside the append call.

Milestone 4 wired the other public cursor-returning commands onto the same server-side cursor model and updated the docs and tests.

## Concrete Steps

All work was done from `/Users/gabrielelanaro/workspace/wrongodb`.

The main verification commands were:

    cargo check
    cargo test document_query:: -- --nocapture
    cargo test --test server_suite protocol:: -- --nocapture
    cargo test
    cargo fmt

The targeted protocol tests that prove the new behavior are:

    cargo test --test server_suite protocol::test_find_returns_non_zero_cursor_when_batch_is_short
    cargo test --test server_suite protocol::test_get_more_drains_remaining_batches
    cargo test --test server_suite protocol::test_kill_cursors_removes_saved_state
    cargo test --test server_suite protocol::test_tailable_oplog_cursor_waits_for_new_write

## Validation and Acceptance

Acceptance is now demonstrated by tests, not just by code shape.

The ordinary cursor proof is `tests/server/protocol.rs::test_find_returns_non_zero_cursor_when_batch_is_short` plus `tests/server/protocol.rs::test_get_more_drains_remaining_batches`. These prove that the first batch is shortened, the cursor id is non-zero, and `getMore` returns the later batch before closing the cursor.

The oplog proof is `tests/server/protocol.rs::test_tailable_oplog_cursor_waits_for_new_write`. It opens a tailable `awaitData` cursor on `local.oplog.rs`, waits in `getMore`, performs a later user write, and verifies that the waiting `getMore` returns the new oplog row.

The full regression gate is:

    cargo test

At the time this document was updated, that full command passed.

## Idempotence and Recovery

The implementation is additive. Re-running the build and test commands is safe. Cursor state is process-local and intentionally disappears on server restart. If future work needs to revert part of the oplog waiting behavior, the safe fallback is to keep the server-side cursor work and reject `tailable` and `awaitData` explicitly rather than silently degrading to polling.

Because WrongoDB's oplog is still unbounded, this implementation does not handle “fell off history” behavior. That must be revisited if oplog truncation is added later.

## Artifacts and Notes

Important command examples that now work:

    { "find": "users", "$db": "app", "filter": {}, "batchSize": 1 }

    { "getMore": <cursor id>, "collection": "users", "$db": "app" }

    {
      "find": "oplog.rs",
      "$db": "local",
      "filter": { "_id": { "$gt": 5 } },
      "batchSize": 2,
      "tailable": true,
      "awaitData": true
    }

    {
      "getMore": <cursor id>,
      "collection": "oplog.rs",
      "$db": "local",
      "maxTimeMS": 1000
    }

The most important files added were:

- `src/server/commands/cursor_manager.rs`
- `src/replication/await_service.rs`

## Interfaces and Dependencies

The new stable internal shapes are:

    src/server/commands/mod.rs
      CommandContext { db_name, cursor_manager, oplog_await_service }
      async trait Command

    src/server/commands/cursor_manager.rs
      CursorManager
      CursorState
      FindCursorState
      MaterializedCursorState

    src/document_query.rs
      FindRequest
      FindPage
      PlannedFindPage
      FindPlan
      FindResumeToken

    src/replication/await_service.rs
      OplogAwaitService

`Cargo.toml` now depends on `async-trait = "0.1"` for the async command trait.

## Revision Note

This file started as the planning document for the Mongo-faithful public cursor prerequisite. It was revised after implementation so the living-document sections now reflect the code that landed, the tests that prove it, and the decisions that turned out to matter during the build.
