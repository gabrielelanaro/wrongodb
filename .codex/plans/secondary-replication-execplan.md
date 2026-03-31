# Build Secondary Replication On Top Of Public Oplog Cursors

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document lives at `.codex/plans/secondary-replication-execplan.md` and must be maintained in accordance with `.codex/PLANS.md`.

## Purpose / Big Picture

After this change, WrongoDB will be able to run one node as a writable primary and another node as a non-writable secondary that copies and applies the primary's oplog. A user will be able to start two servers, write to the primary, and then observe the same collection and index state appear on the secondary without issuing writes there.

The visible proof is simple. Start a primary and a secondary, create a collection and index on the primary, insert or update documents on the primary, and then query the secondary. The secondary should show the same data after it catches up. `hello` on the secondary should report that it is not writable primary, and direct writes to the secondary should fail.

## Progress

- [x] (2026-03-30 15:06Z) Investigated MongoDB's oplog replication flow and identified the important pieces: source selection, oplog fetch, buffering, apply, and progress feedback.
- [x] (2026-03-30 17:20Z) Landed the cursor prerequisite in WrongoDB: public `find` / `getMore`, `_id.$gt` / `_id.$gte`, and oplog-only `tailable` plus `awaitData`.
- [x] (2026-03-30 18:35Z) Replaced the completed cursor prerequisite plan with this active replication plan and rewrote it to assume the cursor work is already present.
- [x] (2026-03-30 20:10Z) Added durable replication-owned state through `local.repl_state`, with a persisted `lastApplied` marker and local-oplog helpers to read the durable tail and scan entries after one oplog index.
- [x] (2026-03-30 20:35Z) Extended server replication configuration with `Primary` versus `Secondary`, node identity, sync-source URI, and non-writable-secondary `hello` / write-admission behavior.
- [x] (2026-03-30 21:15Z) Extended the logical oplog and DDL path so `createCollection` and `createIndexes` replicate through the same oplog model as CRUD.
- [x] (2026-03-30 22:05Z) Added the secondary background runtime, local backlog recovery, oplog apply path, and primary feedback command `replSetUpdatePosition`.
- [x] (2026-03-30 22:45Z) Added two-node replication integration tests for CRUD, DDL, restart recovery, secondary write rejection, and handler-level progress tracking.
- [x] (2026-03-30 23:05Z) Ran `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test --test replication_suite -- --nocapture`, and `cargo test` successfully.

## Surprises & Discoveries

- Observation: the hardest prerequisite for Mongo-style follower replication was not the follower loop itself, but the public cursor behavior needed to read `local.oplog.rs` incrementally.
  Evidence: WrongoDB now has tailable `awaitData` oplog cursors in `src/server/commands/handlers/cursor.rs`, which lets a follower use public `find` / `getMore` instead of a replication-only fetch RPC.

- Observation: once DDL started appending oplog rows, several older one-node tests began handing out duplicate oplog indexes.
  Evidence: test fixtures in `src/api/ddl_path.rs` and `src/write_ops/tests.rs` had been creating separate `ReplicationCoordinator` instances for DDL and CRUD paths, so both coordinators started at oplog index `1`. The fix was to share one coordinator per logical node.

- Observation: the durable replication-state marker must still look like a normal document row, even though it is only internal metadata.
  Evidence: the first replication-suite run failed with `document validation error: missing _id` until `LocalReplicationStateStore` started encoding `_id: "lastApplied"` in `local.repl_state`.

- Observation: using the local oplog itself as the durable fetch queue worked cleanly enough that a separate in-memory fetch buffer never became necessary for v1.
  Evidence: `SecondaryReplicator` in `src/replication/secondary.rs` now follows the sequence "fetch remote batch -> append locally -> apply locally -> save `lastApplied` -> report progress" and restart recovery reuses the same local backlog path.

## Decision Log

- Decision: Build the follower fetch path on top of the public oplog cursor work that already landed instead of inventing a dedicated `replFetchOplog` command.
  Rationale: MongoDB followers read the oplog through the normal query machinery. WrongoDB now has the same minimum capability, so a replication-only fetch path would duplicate the transport we just built.
  Date/Author: 2026-03-30 / Codex

- Decision: Keep the first secondary runtime single-source and direct-primary only.
  Rationale: MongoDB can pull from other secondaries, but WrongoDB does not need chaining to prove correct follower behavior. One configured primary keeps the plan smaller and easier to validate.
  Date/Author: 2026-03-30 / Codex

- Decision: Use the local oplog as the secondary's durable fetch buffer and persist only `lastApplied` separately.
  Rationale: Once a fetched oplog row is saved locally, the secondary can re-open and continue after restart. `lastWritten` can be derived from the local oplog tail, so the only extra durable marker needed is the last oplog entry that has actually been applied to user data.
  Date/Author: 2026-03-30 / Codex

- Decision: Include primary-side progress reporting in this plan, but defer actual write-concern waiting to a later plan.
  Rationale: A follower must still tell the primary how far it has applied, and the primary must store that state somewhere. That progress map is also the groundwork for a later `w: "majority"` implementation.
  Date/Author: 2026-03-30 / Codex

- Decision: Extend the logical oplog to include `createCollection` and `createIndexes` before building the follower apply path.
  Rationale: A secondary that can only apply CRUD but not DDL would diverge as soon as the primary creates a new collection or index. The follower story is not complete until the primary's logical schema changes are also replicated.
  Date/Author: 2026-03-30 / Codex

- Decision: Reuse the existing public oplog cursor on the primary, but keep the follower runtime itself as a dedicated `SecondaryReplicator` module above `DatabaseContext`.
  Rationale: MongoDB separates the follower runtime from the query engine even though it fetches through the query path. WrongoDB follows the same shape while collapsing MongoDB's fetcher, background sync, and feedback pieces into one smaller runtime.
  Date/Author: 2026-03-30 / Codex

- Decision: Reuse `CollectionWritePath` and `DdlPath` for follower apply, and add small oplog-apply entry points instead of building a second mutation engine.
  Rationale: The local write paths already know how to update tables, indexes, and catalog state correctly. Secondary apply only needs "use this exact oplog payload" plus `OplogMode::SuppressOplog`, not an entirely separate storage layer.
  Date/Author: 2026-03-30 / Codex

## Outcomes & Retrospective

This plan is implemented. WrongoDB can now run one node as a writable primary and another as a non-writable secondary that tails the primary's public oplog cursor, persists fetched entries locally, applies them through the existing write and DDL paths, and reports `lastWritten` / `lastApplied` progress back to the primary.

The important limits remain intentional. This is still direct-primary-only replication. There are no elections, no rollback, no sync-source chaining, and no majority write concern yet. MongoDB has more moving parts here; WrongoDB's first version keeps the right boundaries but uses fewer modules and treats the local oplog as the durable fetch queue.

## Context and Orientation

WrongoDB already has a logical oplog in `src/replication/oplog.rs`. Every primary-side CRUD write goes through `src/write_ops/executor.rs`, which calls `src/collection_write_path.rs`, which in turn calls `src/replication/observer.rs` to append an oplog row in the same local transaction as the user-data change. That means the primary already has the most important replication source of truth: one durable log of logical operations.

WrongoDB also already has the public cursor features a follower needs. The MongoDB wire-protocol path in `src/server/commands/*` can now run `find` on `local.oplog.rs` with `_id > last_seen`, return a real server-side cursor id, and continue with `getMore`. The oplog cursor is special because it is append-only from the follower's point of view: rows are added at the end, and a tailable `awaitData` cursor can wait there for more rows.

The relevant pieces are now in place. `src/replication/coordinator.rs` stores the configured node role, node identity, sync-source URI, follower progress map, and oplog index allocator. `src/replication/state_store.rs` persists `lastApplied` in the reserved `local.repl_state` namespace. `src/replication/secondary.rs` owns the background follower loop. `src/replication/applier.rs` turns one oplog entry into one local apply step by reusing `src/collection_write_path.rs` and `src/api/ddl_path.rs`. `src/server/commands/handlers/replication.rs` accepts `replSetUpdatePosition` from followers. `src/api/ddl_path.rs` now writes DDL oplog entries and applies them on secondaries.

This plan uses a few terms of art in plain language. A "primary" is the node that accepts user writes. A "secondary" is the node that rejects user writes and instead copies the primary's oplog. "Apply" means taking one oplog entry and making the same logical change to the local data on the secondary. "Progress feedback" means a small message from the secondary to the primary saying, in effect, "I have fetched up to oplog index X and applied up to oplog index Y."

The main files a novice needs to understand before editing are `src/replication/coordinator.rs`, `src/replication/oplog.rs`, `src/replication/observer.rs`, `src/api/database_context.rs`, `src/write_ops/executor.rs`, `src/collection_write_path.rs`, `src/server/mod.rs`, and `src/server/commands/handlers/connection.rs`. The end-to-end protocol tests currently live in `tests/server/protocol.rs`. This plan will likely add a new top-level integration suite, `tests/replication_suite.rs`, with supporting modules under `tests/replication/`, because two-node replication is a different domain from the existing single-node server protocol tests.

## Plan of Work

The implementation followed the same sequence the design called for. `src/replication/coordinator.rs` now models `Primary` versus `Secondary`, stores the node name and sync-source URI, and keeps follower progress by node name. `src/replication/oplog.rs`, `src/replication/bootstrap.rs`, `src/replication/state_store.rs`, and `src/replication/mod.rs` now provide the durable oplog and replication-owned `local.repl_state` marker a follower needs for crash-safe restart.

`src/api/ddl_path.rs` was extended so collection and index creation append logical DDL oplog rows in the same durable transaction as the catalog-ready step. `src/collection_write_path.rs` now has exact oplog-apply helpers for CRUD, and `src/replication/applier.rs` reuses those plus the new secondary DDL-apply helpers in `DdlPath` to apply one oplog row locally with oplog generation suppressed.

`src/replication/secondary.rs` is the new follower runtime. It first drains locally persisted-but-unapplied oplog rows using the durable `lastApplied` marker. Then it connects to the primary through the Rust `mongodb` driver, opens a tailable public oplog cursor, appends fetched rows to the local oplog, applies them, updates `lastApplied`, and reports progress through `replSetUpdatePosition`. `src/server/commands/handlers/replication.rs` handles that command on the primary, and `src/server/mod.rs` plus `src/bin/server.rs` start the follower runtime only when the configured role is `Secondary`.

## Concrete Steps

Work from `/Users/gabrielelanaro/workspace/wrongodb`.

The following commands were used while implementing and validating this work from `/Users/gabrielelanaro/workspace/wrongodb`:

    sed -n '1,260p' src/replication/coordinator.rs
    sed -n '1,260p' src/replication/oplog.rs
    sed -n '1,260p' src/replication/observer.rs
    sed -n '1,220p' src/api/database_context.rs
    sed -n '1,260p' src/server/mod.rs

Focused iteration commands:

    cargo check

    cargo test --test replication_suite -- --nocapture

    cargo fmt
    cargo clippy -- -D warnings
    cargo test

The replication suite should show at least these behaviors:

    1. a secondary rejects direct writes
    2. a primary write later appears on the secondary
    3. a created collection and index on the primary later exist on the secondary
    4. a restarted secondary resumes from its local oplog instead of starting over

## Validation and Acceptance

Acceptance is now behavioral and implemented. A user can start two WrongoDB servers on different ports, one as `Primary` and one as `Secondary`, write only to the primary, and then observe the same data on the secondary after a short catch-up delay. Querying `hello` on the secondary shows that it is not writable primary. Attempting `insert` or `createCollection` directly against the secondary returns the not-writable-primary error.

The automated acceptance bar is the dedicated replication integration suite in `tests/replication/mod.rs` plus the handler-level progress test in `src/server/commands/handlers/replication.rs`. These now cover CRUD replication, DDL replication, secondary restart recovery, secondary write rejection, and `replSetUpdatePosition` progress storage. The final gate remains:

    cargo test

The full suite should pass with new replication tests included. The new replication suite should also be runnable on its own with:

    cargo test --test replication_suite -- --nocapture

## Idempotence and Recovery

This plan remained additive and restart-safe during implementation. If the secondary crashes after fetching oplog rows but before applying them, restart is safe because fetched rows are persisted in the local oplog first and `lastApplied` is durable state. The startup sequence in `src/replication/secondary.rs` always drains "persisted but not yet applied" rows before it opens a fresh remote oplog cursor.

The main risky area is DDL replication because physical table and index creation may have more than one step. The safe approach is to keep any preparatory work retriable and to make the final "this DDL is durable" step idempotent. If a DDL apply helper sees that the target collection or index already exists in the expected form, it should treat that as success rather than failure.

## Artifacts and Notes

The most important current-state facts for the implementer are these:

    src/replication/observer.rs
      primary CRUD and DDL writes now append logical oplog rows in the same transaction as the logical change

    src/replication/secondary.rs
      follower runtime uses public find/getMore on local.oplog.rs and then writes fetched rows into the local oplog before applying them

    src/replication/state_store.rs
      local.repl_state persists lastApplied as a normal internal document row

    src/server/commands/handlers/replication.rs
      replSetUpdatePosition stores follower progress by node name on the primary

The first end-to-end manual proof to mirror in tests should look like this:

    1. start a primary on port P and a secondary on port S with role flags
    2. connect a MongoDB client to P
    3. run createCollection, createIndexes, insert, update, and delete on P
    4. connect the client to S and query the same collection
    5. observe the same logical results on S
    6. run hello on S and observe isWritablePrimary=false

## Interfaces and Dependencies

Use the existing Rust `mongodb` driver already present in `Cargo.toml` for the secondary's client connection to the primary. Reuse `tokio` for the background replication task. Do not add a second networking stack.

At the end of this plan, the codebase should have these stable concepts:

In `src/replication/coordinator.rs`, define a richer replication configuration that includes node role and identity. One acceptable shape is:

    pub(crate) enum ReplicationRole {
        Primary,
        Secondary,
    }

    pub(crate) struct ReplicationConfig {
        pub(crate) role: ReplicationRole,
        pub(crate) node_name: String,
        pub(crate) primary_hint: Option<String>,
        pub(crate) sync_source_uri: Option<String>,
        pub(crate) term: u64,
    }

Also in that file, add in-memory follower progress storage keyed by node name, with explicit methods to update and read `lastWritten` and `lastApplied`.

In `src/replication/oplog.rs`, extend `OplogOperation` for DDL and add non-test helpers to read oplog rows after a given index and to load the durable tail cleanly.

In a new `src/replication/secondary.rs`, define one background runner that owns the secondary loop. It should expose a small API such as:

    pub(crate) struct SecondaryReplicator { ... }

    impl SecondaryReplicator {
        pub(crate) fn new(... ) -> Self;
        pub(crate) async fn run(&self) -> Result<(), WrongoDBError>;
    }

In `src/server/commands/handlers/`, add one handler for:

    replSetUpdatePosition

The command payload should carry the follower's node name plus its `lastWritten` and `lastApplied` oplog positions. Keep the name Mongo-like because the whole point of this phase is to stay close to the shape of MongoDB's replication protocol.

## Revision Note

This file was created on 2026-03-30 after the public oplog cursor prerequisite was fully implemented and the old cursor plan was removed. It was revised later the same day after the replication work landed so the document reflects the implemented runtime, the DDL oplog extension, the durable `lastApplied` marker, the `replSetUpdatePosition` feedback path, and the test evidence that now proves the behavior.
