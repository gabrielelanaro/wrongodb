# Decisions

## 2026-03-19: Make `ConnectionConfig::new` explicit about WAL and RAFT policy

**Decision**
- Change `ConnectionConfig::new` to take `wal_enabled: bool` and `raft_mode: RaftMode`.
- Keep `Default` as the conventional standalone durable preset for callers that want the common
  startup mode explicitly.
- Update connection startup call sites to pass the policy at construction time.

**Why**
- `ConnectionConfig` carries startup semantics, so `new()` should not silently pick the write
  policy.
- Making the parameters explicit keeps standalone vs clustered behavior visible at the call site
  and aligns the API with the refactor rule.

## 2026-03-08: Cache parsed page headers in the page-store and pin pages by token

**Decision**
- Store parsed in-memory `Page` objects in the page cache instead of raw `Vec<u8>` payloads.
- Cache the fixed page header in `PageHeader`:
  - `page_type`
  - `flags`
  - `slot_count`
  - `lower`
  - `upper`
  - `first_child`
- Replace read-path `PinnedPage` copies with lightweight `ReadPin` tokens plus
  `PageRead::get_page(&ReadPin) -> &Page`.
- Keep copy-on-write semantics on the write path, but make the owned mutable value explicit as
  `PageEdit { page_id, original_page_id, page }`.
- Split B-tree page wrappers into read and write forms:
  - `LeafPage` / `LeafPageMut`
  - `InternalPage` / `InternalPageMut`
- Keep the on-disk page format unchanged; this refactor is purely in-memory.

**Why**
- The previous read path cloned page bytes on every pin and reparsed header fields on every page
  access, which made the hot B-tree traversal path do avoidable work.
- Returning direct page references from `pin_page()` would have forced the store to stay borrowed
  across traversal and iteration, which does not fit the current trait-object based B-tree API.
- Token-based pinning keeps eviction control explicit without fighting Rust's borrowing rules,
  while still removing the read-path payload copy.
- Making header mutations go through `Page` setters keeps cached header fields synchronized with
  the backing byte image.

**Notes**
- Structural page validation still lives in the leaf/internal page wrappers; `Page::from_bytes`
  only parses the fixed header.
- Read paths now inspect `page.header()` directly; write paths still clone page contents only when
  entering the copy-on-write edit flow.

## 2026-03-06: Freeze the public API around `Connection`, `Session`, `WriteUnitOfWork`, and `Cursor`

**Decision**
- Expose only the small DB-facing surface from the crate root:
  - `Connection`, `ConnectionConfig`, `RaftMode`, `RaftPeerConfig`
  - `Session`
  - `WriteUnitOfWork`
  - `Cursor`, `CursorEntry`
  - `WrongoDBError`, `DocumentValidationError`, `StorageError`
  - `start_server`
- Make `WriteUnitOfWork` the only public transactional facade:
  - add `WriteUnitOfWork::open_cursor(...)`
  - remove `session_mut()` and raw transaction escape hatches
- Make public cursor semantics WT-like:
  - cursor methods no longer take explicit `txn_id`
  - `Session::open_cursor(...)` binds a non-transactional cursor
  - `WriteUnitOfWork::open_cursor(...)` binds a transactional cursor
- Keep document/index orchestration internal via explicit services:
  - `CollectionWritePath`
  - `DocumentQuery`
  - `StoreWritePath`
- Stop compiling storage-engine white-box integration tests as part of the public API suite.

**Why**
- The crate root had drifted into a mixed DB API plus storage-engine export surface, which made
  the supported interface impossible to reason about.
- `Session` was also leaking collaborators to internal callers through helper accessors, which
  recreated the same ownership knot one level down.
- The clean Mongo/WT-inspired split is:
  - public `Session`/`Cursor` for storage-facing usage
  - public `WriteUnitOfWork` for transaction scope
  - internal write/query services for document/index orchestration

**Notes**
- The API surface is now mechanically checked with:
  - trybuild UI tests for allowed and forbidden usage
  - a source-level audit of `lib.rs`, `api/mod.rs`, and `server/mod.rs`
- Public low-level cursor writes remain unsupported in clustered RAFT mode by design.

## 2026-03-06: Make the public transaction guard the actual `WriteUnitOfWork`

**Decision**
- Make `Session::transaction()` return the public RAII guard `WriteUnitOfWork`.
- Rename the old passive state bag to an internal `ActiveWriteUnit`.
- Keep `Session` as the owner of the `RecoveryUnit`, closer to Mongo's
  `OperationContext -> RecoveryUnit` relationship.
- Expand `RecoveryUnit` to own unit-of-work lifecycle hooks:
  - `begin_unit_of_work`
  - `commit_unit_of_work`
  - `abort_unit_of_work`
- Keep `Cursor` unchanged in responsibility: it remains a WT-style one-store access path and only
  uses the session recovery unit for local WAL record emission.

**Why**
- The previous step fixed the storage boundary but still left the names inverted:
  the public RAII handle was `SessionTxn` while an internal passive state bag was called
  `WriteUnitOfWork`.
- Mongo's `WriteUnitOfWork` is the actual begin/commit/abort boundary, and the recovery unit is
  session/request-owned, not nested inside a state bag.
- Flipping the names to match the responsibilities makes the transaction flow read like the
  Mongo/WT split we are imitating: `Session` owns context, `WriteUnitOfWork` owns the unit of
  work, `RecoveryUnit` owns local durability hooks, and `TransactionManager` stays MVCC-only.

**Notes**
- Deferred RAFT writes still do not use `RecoveryUnit` for commit markers; they continue to go
  through `DurabilityBackend::record(...)`.
- This supersedes the previous same-day note that described `SessionTxn` as the public handle over
  an internal `WriteUnitOfWork`.

## 2026-03-06: Move local WAL durability into a Mongo-style recovery unit

**Decision**
- Remove the `LocalWriteDurability` hook from `TransactionManager`.
- Introduce internal `RecoveryUnit` and `WriteUnitOfWork` types:
  - `RecoveryUnit` owns local WAL marker emission for local-store writes
  - `WriteUnitOfWork` initially owned the active transaction, touched stores, and recovery unit handle
- Keep `SessionTxn` as the public transaction handle in that intermediate refactor.
- Keep `Cursor` WT-like:
  - local/store-only semantics
  - writable cursors record local WAL through the session recovery unit
  - no deferred-replication policy branching inside the cursor

**Why**
- The previous refactor fixed the public cursor semantics, but it pushed local durability down
  into `TransactionManager`, which made the MVCC layer own part of the storage-recovery policy.
- Mongo keeps that seam above the storage engine with `WriteUnitOfWork` and `RecoveryUnit`,
  while WiredTiger keeps the cursor/storage API boring and stable.
- Moving local WAL logging into a session-owned recovery unit restores the intended split:
  `TransactionManager` owns MVCC, the public transaction guard owns transaction orchestration, and `Cursor`
  remains a one-store access path.

**Notes**
- Local WAL `Put` / `Delete` / `TxnCommit` / `TxnAbort` markers now flow through
  `RecoveryUnit`, not `TransactionManager`.
- Deferred RAFT writes remain unchanged: they still go through `DurabilityBackend::record(...)`
  and `StoreCommandApplier`.
- This supersedes the note in the previous same-day decision that described local WAL logging as
  happening through the transaction manager.

## 2026-03-06: Split standalone local durability from RAFT replication and keep the public cursor WT-like

**Decision**
- Split runtime durability into three modes:
  - `Disabled`
  - `LocalWal`
  - `Raft`
- Treat `wal_enabled=true` with `RaftMode::Standalone` as `LocalWal`, not standalone RAFT.
- Keep `Cursor` as a WT-style local store cursor:
  - one-store reads and local writes only
  - no hidden request-path durability branching
- In clustered RAFT mode, public cursor writes are intentionally unsupported.
- Move Mongo-style document write orchestration into `src/collection_write_path.rs` and keep
  read/query helpers in `src/document_query.rs`.

**Why**
- The design knot came from asking one public cursor API to serve two different layers:
  WT-style local storage access and Mongo-style replicated write orchestration.
- WiredTiger keeps cursor meaning stable, and Mongo keeps replicated write control in an
  explicit write path above storage primitives.
- Splitting standalone durability from RAFT removes the last reason to hide apply-policy inside
  the public cursor path: standalone/default durability can log locally and still let the cursor
  mean “mutate this one store”.

**Notes**
- Local WAL logging now happens below the cursor boundary through a session-owned recovery unit.
- The previous same-day decision about restoring a WT-style public cursor API is superseded here:
  the public cursor stays, but only with strictly local/store semantics.
- Old integration tests that used the public cursor as a replicated write API were removed and
  replaced by coverage on the internal collection write path and server protocol path.

## 2026-03-06: Restore a WT-style public cursor API while keeping request-path policy internal

**Decision**
- Restore `Cursor` and `Session::open_cursor(uri)` as public low-level API surface.
- Keep `Cursor` semantics narrow and stable:
  - one-store access path
  - explicit `txn_id` on read/write methods
  - no public durability-policy branching
- Remove public document CRUD/index helpers from `Session` and keep Mongo-style document
  orchestration internal in `src/document_ops/`.
- Keep request-path apply policy behind an internal cursor write executor chosen by
  `DurabilityBackend::request_apply_mode()`.
- Extend `Session::create(uri)` to support both:
  - `table:<collection>`
  - `index:<collection>:<field>`

**Why**
- Making `Cursor` internal improved ownership clarity, but it moved the public API farther away
  from the WiredTiger mental model that the storage layer is already converging toward.
- Re-exposing the cursor is only correct if its meaning is stable across backends. The earlier
  hidden “maybe apply now, maybe record for later” behavior was the real problem, not the
  existence of a public cursor.
- Keeping document/index orchestration in `document_ops` preserves the cleaned-up responsibility
  split:
  public API is storage-engine flavored, while Mongo-style collection semantics stay internal.

**Notes**
- This supersedes the earlier same-day decision that made `Cursor` internal.
- The committed replay path is unchanged: `StoreCommandApplier` remains the only deferred local
  applier.

## 2026-03-06: Remove hook-owned apply policy and make `Session` the durable write orchestrator

**Decision**
- Remove the `MutationHooks` abstraction and the `should_apply_locally()` flag entirely.
- Put the request-path apply decision on `DurabilityBackend` as an explicit
  `RequestApplyMode`:
  - `LocalNow`
  - `DeferredCommittedApply`
- Make `Session` the only durable write orchestrator:
  - request-time writes either apply locally now or record `DurableOp` values for committed apply
  - transaction commit/abort use the same mode split
- Keep `StoreCommandApplier` as the only deferred/replay applier.
- Make `Cursor` an internal crate-private store helper and remove it from the public API.

**Why**
- `should_apply_locally()` was not really a hook concern; it was an execution-policy flag hidden
  behind a hook interface.
- That made the same public cursor write call mean two different things depending on backend:
  local mutation in disabled-durability mode versus record-now/apply-later in Raft mode.
- Moving the branch up to `Session` makes the write path readable in one place and restores a
  clearer split:
  `Session` orchestrates durable writes, `Cursor` mutates one store locally, and
  `StoreCommandApplier` applies committed durable ops.

**Notes**
- Public supported write operations now live on `Session`; low-level raw cursor writes are no
  longer part of the crate API.
- Raft semantics are unchanged: proposal acknowledgement still waits for quorum commit plus local
  committed apply; `Sync` remains the extra flush barrier for sync-grade operations.

## 2026-03-06: Make MVCC cleanup checkpoint-owned reconciliation

**Decision**
- Remove the dormant runtime GC surface and make checkpoint the only owner of MVCC cleanup.
- Add `TransactionManager::reconcile_store_for_checkpoint(...)` as the internal maintenance step
  before `BTree::checkpoint()`.
- Reconciliation performs three steps per store:
  - materialize the latest committed entry from each update chain into the base store
  - prune obsolete updates that are no longer needed by any active transaction
  - when there are no active transactions, drop fully materialized current committed heads
- Keep `Session::checkpoint()` as the only runtime maintenance entrypoint; do not add a separate
  public `compact` or `run_gc` API in this pass.

**Why**
- The previous refactor left a dead GC path in place without a real runtime caller.
- The actual missing feature was not “a GC API”, but a complete checkpoint-owned reconciliation
  pass that closes the loop between in-memory MVCC chains and the on-disk base store.
- This matches the intended split:
  `Session` owns maintenance entrypoints, storage internals perform reconciliation, and callers do
  not need to choose between multiple cleanup mechanisms.

**Notes**
- This is an internal correctness/readability change only; public API stays the same.
- The reconciliation logic uses the stopping transaction of overwritten versions to decide when
  they are obsolete, so older snapshots are preserved until their replacement is visible to all
  active transactions.

## 2026-03-06: Make `Session` the document/index controller and `Table` a pure store handle

**Decision**
- Introduce an internal `SchemaCatalog` that owns collection/index metadata and URI resolution:
  - `table:<collection>` resolves to the primary store
  - `index:<collection>:<index>` resolves to the backing index store
- Make `Table` a one-store primitive only:
  - own `BTree`, `store_name`, and `TransactionManager`
  - remove embedded index metadata and index fan-out behavior
  - keep only store-local MVCC and maintenance methods
- Make `TableCache` a generic store-handle cache with `get_or_open_store(store_name)`.
- Make `Cursor` a plain store cursor:
  - no `CursorKind`
  - no primary/index special casing
  - explicit store-write tracking for transaction finalization
- Move document/index orchestration into `Session`:
  - CRUD methods
  - index creation/backfill
  - checkpoint fan-out over all known stores
  - transaction finalization over touched stores
- Reduce `document_ops` to update-expression helpers only.

**Why**
- The previous model mixed store-level and collection-level responsibilities:
  `Table` wrapped a single store but also owned index metadata and recursive maintenance logic.
- That made the write path harder to follow because collection semantics were hidden behind a
  low-level handle type.
- The new split is closer to the WiredTiger mental model:
  `Session` owns request/transaction orchestration, `Cursor` mutates one store, and `Table`
  implements one store.
- Moving schema/index relationships into `SchemaCatalog` makes the object graph easier to read:
  cursors talk to tables, sessions talk to schema, and tables no longer reach sideways into
  collection metadata.

**Notes**
- This is a readability/ownership refactor only; WAL format, recovery behavior, and Raft
  semantics are unchanged.
- Direct index-store cursors remain supported by design.
- `Session::open_cursor(uri)` stays stable as the low-level API surface.

## 2026-03-05: Collapse storage handle caching into `TableCache` and make durability hooks explicit

**Decision**
- Replace the overlapping `DataHandleCache` and `StoreRegistry` types with a single
  `TableCache`.
- Make `Table` storage-only:
  - remove stored `MutationHooks`
  - keep only explicit local-apply and recovery methods
- Move durability interception to the orchestration edges:
  - `Cursor` calls `before_put` / `before_delete` explicitly before local mutation
  - `IndexCatalog` and `PersistentIndex` take `&dyn MutationHooks` on mutating calls
  - `Session` owns the shared hooks instance for a request/session
- Keep committed apply and recovery on the same local-apply path through
  `StoreCommandApplier -> TableCache -> Table`.

**Why**
- `DataHandleCache` and `StoreRegistry` represented the same responsibility at two different
  abstraction levels, which made the ownership graph harder to follow than the code required.
- Storing `MutationHooks` inside `Table` hid durability behavior inside cached storage handles.
  Making hook calls explicit keeps durability policy in the write orchestrators instead of the
  storage primitive.
- `TableCache` is the readability-first name for what the type actually does: cache/open table
  handles for both request-time writes and committed apply.

**Notes**
- This is an internal readability refactor only; WAL, recovery, and Raft semantics are unchanged.
- The weak-backed durability hook adapter remains in place to avoid reintroducing a strong
  ownership cycle through cached tables/hooks.

## 2026-03-05: Use storage-facing mutation hooks on top of the durability backend

**Decision**
- Replace the storage-layer `WalSink` interface with a generic `MutationHooks` boundary:
  - `before_put`
  - `before_delete`
  - `before_commit`
  - `before_abort`
  - `should_apply_locally()`
- Keep `DurabilityBackend` as the runtime durability boundary and expose a
  `mutation_hooks()` adapter from it.
- Route table, index, cursor, and session write paths through `MutationHooks` instead of
  calling durability/WAL APIs directly.
- Keep local application explicit:
  - durable backends return `should_apply_locally() == false`
  - disabled durability returns `should_apply_locally() == true`
  - recovery still uses explicit local-apply methods and does not invoke hooks

**Why**
- `WalSink` encoded one concrete implementation detail into the storage layer.
- `MutationHooks` keeps storage code focused on “a mutation is about to happen” while
  `DurabilityBackend` remains responsible for deciding how that mutation becomes durable.
- This also gives transaction commit/abort the same interception boundary as put/delete,
  which makes the call graph more uniform and easier to follow.

**Notes**
- The runtime durability backend still owns checkpoint/status/truncation behavior.
- The storage layer no longer knows about WAL-specific naming.

## 2026-03-05: Replace runtime durability manager with backend-oriented durability API

**Decision**
- Remove `DurabilityManager` as the shared runtime entry point and replace it with
  `DurabilityBackend`.
- Model runtime durability writes as `DurableOp` values:
  - `Put`
  - `Delete`
  - `TxnCommit`
  - `TxnAbort`
  - `Checkpoint`
- Keep the backend surface durability-centric:
  - `record(op, guarantee)`
  - `is_enabled()`
  - `status()`
  - `mutation_hooks()`
  - `truncate_to_checkpoint()`
- Implement the current runtime as a closed backend enum:
  - `Disabled`
  - `Raft(RaftDurabilityBackend)`
- Keep checkpoint policy in `Session` instead of the durability backend:
  the backend performs checkpoint/truncate commands, while the caller decides whether active
  transactions make truncation unsafe.

**Why**
- The previous type did little runtime coordination and mostly acted as a thin adapter over
  Raft-backed durability, so the `Manager` name overstated its responsibility.
- A backend-oriented API makes the call sites read in storage terms instead of consensus terms.
- Moving checkpoint policy to `Session` keeps transaction-safety decisions with the transaction
  owner instead of hiding them in the durability transport layer.

**Notes**
- This is a structural refactor; Raft replication and WAL semantics are intentionally unchanged.
- `RaftCommand` remains an internal replication detail; non-Raft runtime code now speaks in
  `DurableOp`.

## 2026-03-05: Split startup recovery from runtime durability/replication

**Decision**
- Keep `RecoveryManager` as a startup-only type:
  - replays the unapplied WAL tail
  - resolves transactional recovery via staged replay
  - checkpoints recovered stores
- Move long-lived WAL/Raft ownership into a separate `DurabilityManager`:
  - owns the Raft service handle
  - accepts runtime writes and transaction markers
  - exposes Raft status/helpers
  - handles checkpoint + truncate operations
- Share table-apply logic through a single `StoreCommandApplier` used by both recovery and
  runtime durability paths.

**Why**
- The previous `RecoveryManager` name no longer matched the type's real lifetime and
  responsibilities once it also owned runtime Raft/WAL behavior.
- Splitting by startup vs runtime makes the call graph read at one level of abstraction and
  confines `wal_enabled` to the runtime durability subsystem.

**Notes**
- This is a structural refactor only; recovery semantics and runtime Raft/WAL behavior are
  intentionally unchanged.
- `wal_enabled` remains on `DurabilityManager` for now; removing that runtime smell is a later
  refactor.

## 2026-03-05: Recovery replays transactional changes at commit time

**Decision**
- Replace the previous two-pass recovery flow with one-pass staged replay:
  - transactional `Put/Delete` records are buffered by `txn_id`
  - `TxnCommit` releases and applies the buffered changes, then finalizes commit visibility
  - `TxnAbort` and end-of-log discard buffered changes
- Keep recovery owned by `RecoveryManager`; do not introduce a separate top-level recovery pipeline type.
- Keep recovery on the existing executor/local-apply path so runtime and recovery still use the
  same mutation boundary when a change is actually applied.

**Why**
- The recovery story should match transaction semantics directly: a transaction's writes become
  effective when its durable commit marker is replayed, not when its earlier data records are seen.
- This makes `recover()` readable at a single abstraction level:
  read -> apply/stage/commit/discard -> checkpoint.

**Notes**
- This supersedes the earlier recovery-specific assumption of pass-1 txn classification plus
  pass-2 replay in original WAL record order.
- Interleaved writes may now recover according to commit-time transaction visibility rather than
  original WAL position.

## 2026-03-02: Remove public `WrongoDB`/`Collection` layer and keep only `Connection`/`Session`/`Cursor`

**Decision**
- Remove the public high-level wrapper API:
  - deleted `WrongoDB`, `WrongoDBConfig`, and `Collection` types from crate exports.
  - removed `src/engine/` wrapper module.
- Standardize public usage around low-level storage primitives only:
  - `Connection` for shared infrastructure and open/close lifecycle.
  - `Session` for request-scoped mutable state and transaction ownership.
  - `Cursor` for table/index reads and writes.
- Introduce internal-only document orchestration in `src/document_ops/`:
  - free functions for insert/find/update/delete/count/distinct/index operations.
  - keeps document semantics out of the public API while preserving server behavior.
- Rewire Mongo wire server and command handlers to depend on `&Connection` directly
  and call internal `document_ops` instead of constructing `Collection` objects.
- Move Raft configuration types (`RaftMode`, `RaftPeerConfig`) into `src/api/connection.rs`
  so API/recovery do not depend on the removed engine wrapper module.

**Why**
- `Collection` represented a higher abstraction level than the core storage/session API and
  mixed concerns in both public API and server internals.
- Keeping one public abstraction level makes ownership and layering explicit:
  connection lifetime, session transaction context, and cursor I/O.
- This mirrors storage-engine style boundaries (WiredTiger/MongoDB internals):
  operation context and cursors are primary; document orchestration is an internal adapter.

**Notes**
- This is an intentional breaking change with no compatibility shim.
- Integration coverage moved to:
  - low-level connection/session/cursor tests,
  - wire-protocol command tests,
  - internal `document_ops` unit tests.

## 2026-02-24: Unify runtime apply and recovery replay behind a single committed-command executor

**Decision**
- Standardize internal mutation flow around committed commands:
  - runtime path: `API -> propose -> Raft commit -> executor -> local apply -> ACK`
  - recovery path: `WAL replay -> executor -> local apply`
- Add `CommittedCommand` and `CommittedCommandExecutor` as the only Raft-service apply boundary.
- Keep Raft node apply loop authoritative for WAL ordering:
  - `RaftNodeCore::apply_committed_entries` appends WAL and enqueues committed commands.
  - `RaftService` drains and executes committed commands in-order.
- Change proposal ACK semantics to require both:
  - command index committed in Raft
  - same index successfully executed locally by the executor.
- Add fail-stop behavior on executor failure:
  - first apply error latches fatal state
  - subsequent propose/leadership/sync/truncate requests fail with explicit fatal error until restart.
- Keep recovery WAL-only:
  - pass-1 classification (`RecoveryTxnTable`)
  - pass-2 replay to the same executor used at runtime
  - no Raft-tail catch-up replay in recovery.
- Add startup invariant check:
  - fail open if `wal_last_raft_index > protocol_last_log_index`
  - keep conservative clamp `commit_index = min(protocol_last_log_index, wal_last_raft_index)`.
- Move storage handle ownership to `StoreRegistry`:
  - storage-owned `store_name -> Table` mapping and alias resolution
  - runtime/recovery resolve by `store_name`
  - API cache becomes a thin adapter over registry.

**Why**
- A single mutator authority removes leader/follower write-path divergence and avoids double-apply behavior.
- ACK-after-execute aligns client success with local durability/visibility on the serving node.
- Reusing the same executor for runtime and recovery reduces coupling and replay drift risk.
- Storage-owned registry removes prior ownership cycles and keeps dependency direction explicit.

**Notes**
- WAL-disabled mode retains local transaction finalization in `SessionTxn` because no Raft executor is present.
- Snapshot install, dynamic membership, and read-index linearizability remain out of scope.

## 2026-02-22: Add dedicated Raft TCP transport and actor-owned runtime for multi-node quorum writes

**Decision**
- Introduce `src/raft/transport.rs` with a framed Raft wire protocol:
  - frame format: `u32_le payload_len` + bincode payload
  - message envelope: `{ from, to, msg }`
  - message variants mirror runtime RPCs (`RequestVote`, `AppendEntries`, and responses)
- Introduce `src/raft/service.rs` as the single owner of `RaftRuntime`:
  - actor loop drives logical ticks every 50ms
  - actor loop handles inbound RPCs, outbound sends, and proposal commit waiting
  - proposal timeout set to 5s for quorum commit
  - service exposes sync/truncate operations needed by recovery paths
- Replace `RecoveryManager`’s shared `Arc<Mutex<RaftRuntime>>` with `RaftServiceHandle`.
- Extend cluster config shape from peer IDs to static peer topology:
  - `RaftMode::Cluster { local_node_id, local_raft_addr, peers: Vec<RaftPeerConfig> }`
  - each peer has `{ node_id, raft_addr }`
  - validate non-empty/parseable addresses, unique peer IDs, unique peer addresses, and no local-node self peer.
- Keep client semantics for this slice:
  - follower writes rejected with `NotLeader` / `NotWritablePrimary`
  - follower reads remain allowed (potentially stale)

**Why**
- Multi-node quorum commit cannot progress without background ticking and network transport.
- Single runtime ownership removes lock-based races and centralizes commit progression.
- Static `id+addr` topology is the smallest decision-complete step for real multi-process clusters.

**Notes**
- Delivery is fire-and-forget (connect/send/close per outbound message); failed sends are retried by Raft heartbeat/election behavior.
- Dynamic membership, read-index/linearizable follower reads, and snapshot installation remain out of scope.

## 2026-02-22: Require quorum-committed Raft proposals before acknowledging WAL mutations

**Decision**
- Introduce replicated state-machine commands in `src/raft/command.rs`:
  - `Put`, `Delete`, `TxnCommit`, `TxnAbort`, `Checkpoint`
  with deterministic binary `encode/decode`.
- Make `RaftNodeCore` proposal-driven for writes:
  - add `propose_command` (leader-only)
  - persist protocol log entry first
  - emit immediate replication effects via role engine.
- Add explicit apply loop in `RaftNodeCore`:
  - `apply_committed_entries` decodes committed protocol entries and applies them to WAL in
    strict log-index order.
  - track `last_applied_protocol_index` separately from protocol `commit_index`.
- Set restart commit boundary conservatively:
  - `commit_index = min(protocol_log_last_index, wal.last_raft_index())`
  - never auto-commit protocol-log tail on open.
- Extend `RaftRuntime` with proposal/commit waiting APIs:
  - `propose`
  - `wait_for_commit`
  - `wait_for_commit_with_driver`
  - `step_until_quiescent`
  and return explicit timeout errors when quorum commit is not reached.
- Route `RecoveryManager` WAL mutation APIs through replicated proposals instead of direct WAL appends:
  - `log_put`, `log_delete`, `log_txn_commit_sync`, `log_txn_abort`, `checkpoint_and_truncate_if_safe`.
- Keep static membership only (`peer_ids`), no dynamic membership or transport daemon in this slice.

**Why**
- Leader-local append ACKs can acknowledge data that is lost on leader failover.
- Protocol log must be the authoritative source for commit/apply ordering, with WAL as the applied
  state-machine side effect.
- Conservative restart commit reconstruction avoids incorrectly applying uncommitted tails after crash.

**Notes**
- Standalone mode remains immediately writable (majority = 1) and commits locally.
- Cluster mode without a transport driver now fails proposals with an explicit quorum-timeout error
  instead of silently local-acking writes.

## 2026-02-22: Add Raft runtime bridge and leader-gated write path with wire-protocol errors

**Decision**
- Add `src/raft/runtime.rs`, a transport-agnostic runtime adapter around `RaftNodeCore` that:
  - converts internal `RaftEffect` outputs into outbound request envelopes
  - accepts inbound request/response envelopes
  - emits outbound response envelopes for inbound RPC requests
- Keep runtime stepping manual in this slice (`tick`, `handle_inbound`, `drain_outbound`); no
  background scheduler or network transport is introduced yet.
- Add a minimal public cluster-mode selector:
  - `RaftMode::Standalone` (default)
  - `RaftMode::Cluster { local_node_id, peer_ids }`
  wired through `WrongoDBConfig` and `ConnectionConfig`.
- Enforce leader-only writes in `RecoveryManager` for all WAL mutation calls:
  - `log_put`, `log_delete`, `log_txn_commit_sync`, `log_txn_abort`, `checkpoint_and_truncate_if_safe`
  return `WrongoDBError::NotLeader { leader_hint }` when this node is not leader.
- Bootstrap zero-peer standalone nodes to leader during `RaftNodeCore::open_with_config` so
  local single-node writes remain immediately available.
- Surface leadership to clients via Mongo wire protocol:
  - `hello/isMaster` now returns dynamic `isMaster`/`isWritablePrimary` and `readOnly`
  - optional `primary` hint is included when known
  - command execution failures are converted to error documents (no socket drop), with
    `NotLeader` mapped to code `10107` and `codeName: "NotWritablePrimary"`.

**Why**
- We need a runnable integration boundary between pure Raft logic and future transport code
  without committing to networking decisions yet.
- Leader gating protects Raft safety by preventing follower local divergence at the durability
  boundary.
- Single-node auto-election preserves current developer ergonomics.
- Returning wire-protocol command errors keeps clients connected and retry-capable.

**Notes**
- This slice does not add RPC transport, background ticking, or multi-node discovery.
- Follower write proxying is intentionally out of scope; writes are hard-rejected off-leader.

## 2026-02-22: Add deterministic Raft role/timer engine with outbound effects (no transport)

**Decision**
- Add `src/raft/role_engine.rs` with a pure role/timer engine that manages:
  - node role (`Follower`, `Candidate`, `Leader`)
  - election and heartbeat logical tick counters
  - deterministic pseudo-random election timeout selection in `[min,max]`
  - candidate vote tracking
  - leader replication progress (`next_index`, `match_index`)
- Define outbound transport-agnostic effects:
  - `SendRequestVote`
  - `SendAppendEntries`
- Keep membership static and in-memory for this phase (`local_node_id`, `peer_ids`).
- Wire `RaftNodeCore` to delegate role/timer behavior to the engine and persist only protocol
  state deltas (hard state + protocol log) after tick/response handling.

**Why**
- Separating role/timer logic from transport lets us test consensus behavior deterministically
  before introducing network failure modes.
- Logical ticks make election behavior reproducible in unit/integration harnesses.
- Static membership keeps this slice focused on core consensus lifecycle.

**Notes**
- Runtime/server background scheduling is intentionally out of scope.
- Volatile role/timer/leader-tracking state remains non-persistent across restart in this slice.

## 2026-02-22: Add durable Raft protocol adapter layer in RaftNodeCore

**Decision**
- Add `src/raft/log_store.rs` with `RaftLogStore`, a dedicated durable log for protocol-core
  entries (`raft_log.bin`) that supports:
  - `last_log_index_term`
  - `term_at(index)`
  - `truncate_from(index)` with Raft 1-based semantics
  - `append_entries`
- Keep protocol payload opaque (`Vec<u8>`) and independent from WAL `WalRecord`.
- Extend `RaftNodeCore` with adapter methods:
  - `handle_request_vote_rpc`
  - `handle_append_entries_rpc`
- Route inbound RPCs through pure handlers in `src/raft/protocol.rs`, then persist effects:
  - hard state updates (`current_term`, `voted_for`) via `RaftHardStateStore`
  - log truncation/append via `RaftLogStore`
- Initialize in-memory protocol state on open from persisted hard state + persisted protocol log.

**Why**
- The pure protocol core is now executable against real durable state without introducing
  networking or timers.
- Raft log conflict repair requires truncation by logical index, which existing append-only WAL
  APIs do not provide.

**Notes**
- Existing WAL/recovery write path for database mutations remains unchanged.
- Commit index remains volatile in this slice (reconstructed from persisted log tail on open).

## 2026-02-22: Add pure Raft protocol handlers for RequestVote and AppendEntries

**Decision**
- Add a new internal module `src/raft/protocol.rs` that contains deterministic,
  in-memory-only handlers:
  - `handle_request_vote`
  - `handle_append_entries`
- Define protocol-only state and RPC request/response types in that module:
  - `RaftProtocolState` (`current_term`, `voted_for`, `log`, `commit_index`)
  - `ProtocolLogEntry` (`term`, opaque `payload`)
  - `RequestVote*` and `AppendEntries*` message structs
- Keep protocol log indexing explicitly 1-based with index `0` as the sentinel
  "before first entry".
- Implement full Raft voting and log-matching rules, including:
  - term monotonicity and vote reset on newer term
  - vote granting based on prior vote and log up-to-date checks
  - append conflict detection, suffix truncation, and append
  - commit advance rule `commit_index = max(commit_index, min(leader_commit, last_log_index))`
- Keep all existing WAL/recovery integration in `RaftNodeCore` unchanged in this slice.

**Why**
- Isolating protocol semantics from IO/network/persistence makes behavior easy to test
  and reuse when we add transport adapters and persistent integration later.
- A pure core reduces coupling and lets us validate Raft safety rules now without
  introducing distributed-system runtime complexity.

**Notes**
- APIs remain internal-only (`pub(crate)`); no public crate API changes.
- Leader election timers, role transitions, networking, and persistence adapters are
  explicitly out of scope for this slice.

## 2026-02-22: Add Raft hard-state sidecar and internal Raft node core

**Decision**
- Introduce an internal `raft` module with:
  - `RaftHardStateStore` for persisted hard state (`current_term`, `voted_for`)
  - `RaftNodeCore` wrapping `GlobalWal` and in-memory progress (`commit_index`, `last_applied`)
- Persist hard state in `<db_dir>/raft_state.bin` using a binary format with:
  - magic/version
  - flags + optional UTF-8 `voted_for`
  - CRC32 checksum
- Apply fail-fast safety policy for hard-state corruption:
  - missing file initializes defaults
  - invalid/corrupt/unsupported file fails startup
- Route WAL appends in `RecoveryManager` through `RaftNodeCore` so `raft_term` is sourced from persisted hard state instead of a bootstrap constant.
- Keep commit/apply runtime behavior unchanged in this slice by advancing in-memory `commit_index` and `last_applied` to the WAL tail after each append.

**Why**
- Raft election safety requires durable `current_term` and `voted_for`.
- Separating protocol state from WAL byte-format code keeps responsibilities explicit and reduces future coupling.
- Corrupt hard state should not be silently reset because that can violate Raft voting invariants after restart.

**Notes**
- APIs remain internal-only; no new public exports.
- Multi-node RPC/election/apply-loop behavior remains out of scope for this slice.

## 2026-02-22: WAL v2 adds Raft term/index identity and snapshot boundary metadata

**Decision**
- Bump WAL format to v2 and make each WAL record carry explicit Raft identity in the record header:
  - `raft_term`
  - `raft_index`
- Keep WAL logical payloads unchanged (`Put/Delete/TxnCommit/TxnAbort/Checkpoint`).
- Extend WAL file header with Raft truncation state:
  - `last_raft_index` (monotonic, never reset by truncation)
  - `snapshot_last_included_index`
  - `snapshot_last_included_term`
- Keep checkpoint truncation enabled, but when truncating:
  - update snapshot boundary fields to the latest appended entry
  - truncate records to header
  - preserve `last_raft_index` so post-truncate appends continue from `N+1`.
- Require WAL append APIs to accept explicit `raft_term` now; current non-Raft callers pass bootstrap term `0`.
- Hard cutover policy remains: unsupported WAL versions are rejected (no v1 compatibility path).

**Why**
- Raft log matching requires stable `(term, index)` identity per entry.
- LSN is a physical byte-position identifier and is unsuitable as the sole Raft index across truncation/snapshot boundaries.
- Persisting snapshot boundary metadata allows truncation to remain active without violating monotonic Raft index semantics.

**Notes**
- Recovery semantics are unchanged: replay still filters by transaction commit markers and ignores Raft metadata for now.
- Persistent Raft hard-state (`current_term`, `voted_for`) is intentionally out of scope for this slice.

## 2026-02-20: Make BTree pure by splitting out PageStore module

**Decision**
- Move page-storage abstractions and implementation out of `src/storage/btree/` into a new
  `src/storage/page_store/` module:
  - `traits.rs`: `PageRead`, `PageWrite`, `RootStore`, `CheckpointStore`, composed as `PageStore`
  - `types.rs`: `PinnedPage`, `PinnedPageMut`
  - `store.rs`: file-backed `PageStore` implementation (formerly `Pager`)
  - `page_cache.rs`: page-cache implementation reused by the store
- Remove path-based constructors from `BTree` (`create/open`) and keep a single constructor:
  `BTree::new(Box<dyn PageStoreTrait>)`.
- Move root initialization responsibility out of `BTree` and into callers (`Table` and test helpers).

**Why**
- Keep `BTree` focused on tree algorithms and page-format logic, independent from storage backend
  wiring and filesystem path concerns.
- Make page storage reusable by other structures (non-BTree users) and easier to mock in tests.
- Clarify architecture boundaries: callers construct storage, then inject it into tree logic.

**Notes**
- `Table::open_or_create_btree` now constructs the file-backed page store and performs root
  bootstrapping/checkpoint for newly-created files.
- Integration tests that previously used `BTree::create/open` now construct trees via
  `PageStore + BTree::new`.

## 2026-02-20: Detangle BTree from MVCC and WAL via TxnManager

**Decision**
- Make `BTree` a pure physical key/value structure:
  - remove `global_txn`, `global_wal`, and internal MVCC state
  - remove transaction-aware write APIs and WAL logging hooks from `BTree`
  - keep only physical byte operations (`put/get/delete/range/checkpoint`)
- Move MVCC chain storage to `src/storage/mvcc.rs` as `MvccState`.
- Introduce `src/txn/txn_manager.rs` with `TxnManager` as the coordinator for:
  - global transaction state (`GlobalTxnState`)
  - optional global WAL handle (`GlobalWal`)
  - per-store MVCC chain maps
- Route `Table` read/write/commit-abort bookkeeping and checkpoint materialization through `TxnManager`.
- Replay crash recovery through `Table::put_recovery` / `Table::delete_recovery` instead of writing to `BTree` directly in `Connection`.

**Why**
- Remove circular coupling where the physical access method carried transaction visibility and durability logic.
- Centralize MVCC + WAL semantics in one place so `Table` and `Session` can delegate policy while `BTree` stays format/IO-focused.
- Align recovery path with logical table APIs rather than bypassing them.

**Notes**
- Transactional writes continue to stage in MVCC chains and become physically materialized at checkpoint/recovery boundaries.
- Non-transactional writes continue to write directly to `BTree` and can still be WAL-logged by `TxnManager`.

## 2026-02-20: Hard split between TransactionManager and RecoveryManager

**Decision**
- Rename `TxnManager` to `TransactionManager` and scope it to transaction visibility + MVCC only:
  - begin/commit/abort transaction state
  - per-store MVCC update chains
  - snapshot reads, commit/abort marking, GC, checkpoint materialization
- Introduce `src/recovery/manager.rs` with `RecoveryManager` as the only owner of:
  - global WAL open/init
  - WAL record append (`Put/Delete/TxnCommit/TxnAbort`)
  - commit-marker sync semantics
  - checkpoint marker + WAL truncation gating
  - startup two-pass WAL replay
- Introduce `WalSink` in `src/storage/wal.rs` so `Table` logs writes without depending on recovery internals.
- Move `RecoveryTxnTable` from `src/txn/` to `src/recovery/txn_table.rs`.
- Remove `TxnManager` from public crate exports.

**Why**
- Enforce a strict ownership boundary: transaction policy and recovery/durability policy evolve independently.
- Eliminate the previous mixed responsibility class that carried both MVCC state and WAL lifecycle.
- Keep table write paths explicit: log through a durability interface, then apply through transaction state.

**Notes**
- Recovery replay applies directly through `Table::put_recovery/delete_recovery` and is WAL-sink free (no re-log during replay).
- Commit ordering remains unchanged: WAL commit marker + fsync before visibility flip.
- Checkpoint WAL truncation remains blocked while any transaction is active.

## 2026-02-07: Wire-protocol A/B benchmark gate for concurrency refactor decisions

**Decision**
- Add a dedicated benchmark binary `bench_wire_ab` that drives both WrongoDB and MongoDB through the MongoDB wire protocol.
- Run MongoDB in Docker (pinned image default `mongo:7.0`) and WrongoDB as `wrongodb-server` with isolated `--db-path`.
- Standardize MVP workload defaults:
  - scenarios: `insert_unique`, `update_hotspot`
  - concurrencies: `1,4,8,16,32,64`
  - warmup/measure: `15s/60s`
  - repetitions: `3`
- Emit benchmark artifacts in `target/benchmarks/wire_ab/`:
  - `results.csv` (per-point latency/throughput/error rows)
  - `gate.json` (classification + scaling metrics)
  - `summary.md` (human-readable table and rationale)
- Classify with a scaling-based gate from `insert_unique` medians:
  - `RECOMMEND_REFACTOR` when WrongoDB scaling is flat while MongoDB scales and Wrongo p95 grows sharply
  - `DEFER_REFACTOR` when Wrongo scales adequately or scale gap is small
  - `INCONCLUSIVE` for missing required points or elevated error rates

**Why**
- We need data before paying complexity cost for lock-granularity refactors.
- A wire-protocol benchmark isolates server + concurrency behavior in the exact path clients use.
- Structured outputs make decisions reproducible and comparable across iterations.

**Notes**
- This benchmark is local-first and not CI-gated in the MVP phase.
- Durability remains backend-default for now; relaxed durability comparisons can be added later if needed.

## 2026-02-07: Normalize repository layout around domain modules and suite entrypoints

**Decision**
- Introduce `src/api/` and move connection/session/cursor/handle-cache modules under it.
- Rename `src/engine/db.rs` to `src/engine/database.rs`.
- Rename `src/storage/global_wal.rs` to `src/storage/wal.rs`.
- Replace test `#[path = ...]` shims with explicit suite entrypoints:
  - `tests/engine_suite.rs`
  - `tests/storage_suite.rs`
  - `tests/server_suite.rs`
  - `tests/smoke_suite.rs`
  - `tests/connection_suite.rs`
- Keep domain folders under `tests/` (`engine/`, `storage/`, `server/`, `smoke/`, `connection/`) with clear file names.
- Move perf workload from integration tests to Criterion benches (`benches/main_table_vs_append_only.rs`), removing `tests/perf`.

**Why**
- Root-level module sprawl made ownership boundaries unclear; `src/api/` clarifies runtime API concerns.
- `database.rs` and `wal.rs` are more discoverable and consistent with existing naming.
- Explicit suite crates avoid hidden test wiring and remove orphan-file risk.
- Domain folders plus descriptive names (`split_root`, `multi_level_range`, `block_file`) are easier to navigate than slice labels.
- Benchmarks should live under `benches/` so integration test suites stay behavior-focused.

**Notes**
- Public crate API remains stable through `lib.rs` re-exports.
- Internal module paths now use `crate::api::*` and `crate::storage::wal::*`.

## 2026-02-07: WAL truncation requires no active txns; indexes use MVCC writes

**Decision**
- `Session::checkpoint_all` must not advance/truncate the global WAL while any transaction is active.
- Secondary index maintenance (`add_doc`/`remove_doc`) uses MVCC writes for transactional updates.
- Transaction finalize now propagates `mark_updates_committed/aborted` to both main tables and index tables.

**Why**
- Prevents checkpoint-time WAL truncation from discarding uncommitted transaction records needed for later commit recovery.
- Keeps index visibility aligned with transaction boundaries so older snapshots are not hidden by uncommitted index deletes.

**Notes**
- Global WAL truncation is intentionally conservative under concurrent transactions.
- Rollback-on-abort for index raw writes is superseded by MVCC commit/abort marking for index tables.

## 2026-02-07: Global connection-level WAL + hard cutover

**Decision**
- Move WAL ownership from per-BTree files to one connection-level file: `<db_dir>/global.wal`.
- Route all `Put/Delete` WAL records through the global WAL and include `store_name` in each record.
- Run recovery once at `Connection::open` (two-pass: txn-table build, then logical replay).
- Make `SessionTxn::commit` write and sync exactly one `TxnCommit` marker before visibility flip.
- Make `SessionTxn::abort` write one `TxnAbort` marker; no mandatory sync.
- Keep `Collection::checkpoint` API, but implement it as a global checkpoint coordinator:
  - checkpoint all open table handles
  - write one WAL `Checkpoint` record
  - sync + truncate global WAL.
- Hard cutover: legacy per-table `*.wal` files are ignored and only warned about.

**Why**
- Ensures deterministic cross-collection crash recovery with one ordered log stream.
- Removes per-table commit-marker coordination complexity.
- Centralizes durability boundaries and replay semantics in one subsystem.

**Notes**
- Per-table WAL codepaths and controls (`sync_wal`, `set_wal_sync_threshold`, per-BTree recovery) are removed.
- Legacy `*.wal` cleanup is an offline maintenance step after successful open/checkpoint.

## 2026-02-02: Table-owned index catalog + Session-only transactions

**Decision**
- Move secondary index ownership into `Table` via a persisted per-collection catalog.
- Expose read-only index cursors (`index:<collection>:<index>`) via `Session::open_cursor`.
- Remove `CollectionTxn`/`MultiCollectionTxn`; all transactions are `SessionTxn`.
- Switch to directory-based storage layout:
  - Main table: `<db_dir>/<collection>.main.wt`
  - Index: `<db_dir>/<collection>.<index>.idx.wt`
  - Catalog: `<db_dir>/<collection>.meta.json`
- No migration for legacy `{base}.{collection}.main.wt` layouts.

**Why**
- Aligns with WiredTiger’s model: tables own indexes, sessions own transactions, and cursors are the access path.
- Reduces duplication and complexity in `collection/` by pushing storage/index concerns into `Table`.
- Establishes a stable on-disk catalog for index discovery and `index:` URI mapping.

**Notes**
- Index cursors are read-only; writes flow through the table/collection path.
- Index updates remain immediate with rollback-on-abort until MVCC index writes are implemented.

## 2026-02-01: MVCC includes history store for older versions

**Decision**
- The MVCC design will include a dedicated history store (HS) table to retain older committed versions.
- Read path will consult update chains, then on-disk base value, then HS.

**Why**
- Snapshot isolation must remain correct across eviction/checkpoint; without HS, long-running readers can lose older versions.
- Matches the WiredTiger/MongoDB model and avoids unbounded in-memory version chains.

**Notes**
- HS is an internal B-tree keyed by `(btree_id, user_key, start_ts, counter)` with values carrying stop/durable timestamps and value.
- GC can drop HS entries once `stop_ts < pinned_ts` (pinned = min active read timestamp and configured oldest).

## 2026-02-01: Phase 1 keeps update chains in memory only

**Decision**
- Phase 1 MVCC update chains are stored in-memory and are not persisted or reconciled into pages.
- MVCC BTree APIs are additive and not wired into engine CRUD yet.

**Why**
- Keeps Phase 1 focused on core MVCC primitives and visibility logic.
- Avoids changing on-disk formats or WAL semantics before transaction lifecycle is implemented.

**Notes**
- `get_mvcc/put_mvcc/delete_mvcc` are internal building blocks for later phases.
- Persistence, WAL markers, and history store integration are deferred to Phase 2+.

## 2026-02-01: MVCC WAL recovery filters by commit markers

**Decision**
- WAL recovery will apply logical operations only for transactions with a `TxnCommit` record.
- Transactions without a commit record at end-of-log are treated as aborted.

**Why**
- Preserves atomicity during crash recovery and matches WiredTiger’s logical replay model.
- Avoids partially applying uncommitted writes when using per-txn WAL grouping.

**Notes**
- Recovery still starts from the checkpoint LSN.
- Prepared transactions (future) will be rolled back to stable during recovery unless explicitly made durable.

## 2026-02-01: Multi-file atomic commit via txn visibility + WAL commit marker

**Decision**
- Multi-document transactions are made atomic across multiple files by global transaction visibility.
- `TxnCommit` in the WAL is the durability boundary; only committed txns are applied during recovery.

**Why**
- Avoids the need for cross-file atomic writes while still providing all-at-once visibility.
- Mirrors WiredTiger/MongoDB’s logical recovery model and preserves crash safety.

**Notes**
- Checkpoints may flush some files earlier than others; WAL replay reconciles them to the same committed state.
- Collection main tables and indexes all participate in the same transaction context.

## 2026-01-31: Explicit collections, no default "test"

**Decision**
- Remove top-level implicit CRUD on `WrongoDB` (no more default "test" collection operations).
- Add `WrongoDB::collection(name) -> &mut Collection` and move collection-scoped operations onto the collection.
- Stop auto-creating the `"test"` collection in `WrongoDB::open`; collections are created on explicit `collection(...)` access.

**Why**
- Prevent accidental writes/reads against a magic default collection.
- Make the collection choice explicit at call sites and keep a single, consistent API shape.

**Notes**
- Wire-protocol commands still default to `"test"` when the client omits a collection name.
- Collection file naming is unchanged: `"test"` uses the base path, other collections use `{base}.{collection}.db`.

## 2026-01-31: Insert fast-path + preallocation knob

**Decision**
- Add `insert_one_doc` / `insert_one_doc_into` API to accept a `serde_json::Map` directly (avoid `Value` wrapper + clone).
- Add `BTree::insert_unique` to enforce `_id` uniqueness without a separate `get` traversal.
- Add optional `WRONGO_PREALLOC_PAGES` env var to preallocate free extents when creating new BTree files.

**Why**
- Reduce per-insert overhead (fewer traversals and conversions) for latency benchmarks.
- Avoid ftruncate/file-growth in hot paths by preallocating space.

**Notes**
- Preallocation extends the file and records the extra blocks as **avail** extents; it does not change the on-disk format.
- The env var only affects **new** data files on create; existing files are unchanged.

## 2026-01-31: Configurable server listen address

**Decision**
- Allow the server listen address to be overridden via (in order): CLI arg, `WRONGO_ADDR`, `WRONGO_PORT`.
- Default remains `127.0.0.1:27017` when no overrides are provided.

**Why**
- Avoid port collisions during local development and benchmarks.
- Enable multiple local instances without code changes.

**Notes**
- `WRONGO_PORT` only changes the port and still binds on `127.0.0.1`.

## 2026-01-31: Collection-owned checkpoint scheduling

**Decision**
- Move automatic checkpoint scheduling (threshold + counters) to `Collection`.
- Remove `BTree::request_checkpoint_after_updates` and pager-level checkpoint counters.
- Remove `WrongoDB::checkpoint`; callers checkpoint collections directly.
- Move checkpoint configuration to `WrongoDBConfig::checkpoint_after_updates`, removing the runtime `request_checkpoint_after_updates` method.

**Why**
- Only the collection can guarantee that main table and secondary indexes checkpoint together.
- Avoids a top-level durability API that hides which data is being flushed.
- Configuration belongs in the config object, not runtime method calls—this makes durability behavior explicit at database open time.

**Notes**
- `Collection::checkpoint()` remains the explicit durability boundary.
- Auto-checkpointing now counts collection-level mutations (document updates), not page-level writes.
- Checkpoint configuration is set once at `WrongoDB::open_with_config()` and applies to all collections.

## 2026-01-30: BTree pager abstraction via `PageStore`

**Decision**
- Introduce a `PageStore` trait in the B-tree layer to abstract pager operations.
- Store the pager in `BTree` as a `Box<dyn PageStore>` and route range iteration through the trait.
- Add a `data_path()` accessor on the trait to avoid direct `BlockFile` exposure in recovery.

**Why**
- Decouple B-tree algorithms/iteration from the concrete pager implementation (DIP).
- Enable alternative page-store implementations (tests, in-memory, future backends) without changing B-tree logic.
- Keep the public `BTree` API unchanged while still enforcing the abstraction internally.

**Notes**
- The trait uses the existing pinned page types (`PinnedPage`, `PinnedPageMut`) to keep the refactor small.
- Dynamic dispatch is limited to pager calls; behavior and persistence semantics are unchanged.

## 2026-01-30: Split pager abstraction into smaller traits

**Decision**
- Replace the monolithic `PageStore` with smaller traits (`PageRead`, `PageWrite`, `RootStore`,
  `CheckpointStore`, `WalStore`, `DataPath`) and compose them via `BTreeStore`.
- Keep `BTree` storing a boxed `dyn BTreeStore` while `BTreeRangeIter` only depends on `PageRead`.

**Why**
- Reduce interface surface per use site (ISP/SRP) while preserving the existing public `BTree` API.
- Allow future components (e.g., iterators/tests) to depend on smaller capability sets.

**Notes**
- This is a refactor-only change; storage semantics and behavior remain the same.

## 2026-01-30: Move WAL ownership out of Pager

**Decision**
- Introduce a `Wal` handle owned by `BTree` for logging/sync policy.
- Remove WAL state and methods from `Pager`; `Pager` is now page + checkpoint only.

**Why**
- Keep the pager focused on page IO/COW/checkpointing (SRP).
- Make WAL lifecycle/policy independent and easier to replace or disable.

**Notes**
- `Wal` is optional (`None` when disabled) and created in `BTree::create/open`.
- Recovery temporarily detaches the `Wal` handle to avoid logging during replay.

## 2026-01-28: Extent metadata in header payload + main table naming

**Decision**
- Replace the free-list head with persisted extent lists (alloc/avail/discard) stored in the header payload.
- Track discarded extents with a generation tag and reclaim them into avail after checkpoint commit.
- Store the main table as a dedicated B-tree file at `{collection_path}.main.wt`.
- Encode `_id` keys and document values as BSON; secondary index keys append a length-prefixed BSON `_id`.
- Use two skiplists per extent list (by-offset and by-size) instead of WiredTiger’s size-bucketed list.

**Why**
- Keep allocator metadata persistent without adding extra metadata files while aligning with COW + checkpoint semantics.
- Separate main-table data from index files using a predictable naming convention.
- BSON provides a deterministic binary representation for Mongo-like documents and ids.

**Notes**
- The alloc list reflects blocks reachable from the stable checkpoint; discard extents are reclaimed and coalesced into avail on checkpoint.
- WiredTiger uses a by-size skiplist of size buckets, each pointing to a by-offset skiplist of extents of that size; we use a flat by-size skiplist keyed by `(size, offset)` plus a separate by-offset skiplist.

**Skiplist Shapes (WT vs ours)**
```
WiredTiger (avail list):
  by-size skiplist
    size=4  -> size=8 -> size=16
       |        |        |
       v        v        v
    by-offset by-offset by-offset
    10->20    7->40     100

Ours (avail list):
  by-size skiplist (key = (size, offset))
    (4,10) -> (4,20) -> (8,7) -> (8,40) -> (16,100)

  by-offset skiplist (key = offset)
    7 -> 10 -> 20 -> 40 -> 100
```

**Implication**
- Allocation policy is the same (best-fit, then lowest offset within that size), but WT’s size buckets keep the top-level size list small when many extents share the same size. Our flat list is simpler to serialize but its size index scales with total extents.

## 2026-01-27: Logical WAL replay (WiredTiger-style)

**Decision**
- Replace page-level WAL records with logical put/delete records.
- Recovery replays logical ops through BTree writes with WAL disabled.
- Bump WAL format version; incompatible WAL is rejected during recovery (open continues with warning). No migration/backwards compatibility.
- Recovery does not auto-checkpoint; WAL is retained until an explicit checkpoint.

**Why**
- Avoid COW page-id drift and split ordering failures during recovery.
- Align with WiredTiger's logical WAL approach (key-based logging, normal B-tree replay).

**Notes**
- Replay is idempotent: put is upsert; delete missing keys is OK.

## 2026-01-11: Agentic image generation loop for blog diagrams

**Decision**
- Add an opt-in agentic loop to `blog/generate_image.py` that drafts prompts, critiques images, and iterates with a cap.
- Use `gemini-3-pro-image-preview` for draft, critique, and image generation steps.
- Emit a sidecar JSON file next to the output image in agentic mode with prompts and critique summaries.

**Why**
- Improve story effectiveness, visual consistency, and overall aesthetic quality for diagrams.
- Preserve iteration context for later tuning without manual logging.

**Notes**
- Default single-shot behavior remains unchanged when `--agentic` is not used.

## 2026-01-02: Commit/abort semantics for mutable pinned pages

**Decision**
- Introduce commit/abort semantics for mutable page pins:
  - `pin_page_mut` records the original (stable) page id when first-write COW happens.
  - `unpin_page_mut_commit` writes the updated payload into the cache and retires the original page.
  - `unpin_page_mut_abort` discards the working page and keeps the original page reachable.

**Why**
- Avoid retiring blocks that are still reachable from the stable root when a mutation fails.
- Make COW safe under error paths while preserving the “coalesce writes until checkpoint” behavior.

**Notes**
- Only first-write COW paths carry an `original_page_id`; subsequent writes to the same working page commit without retiring anything.
- The stable root pointer remains unchanged until checkpoint, so aborting a mutation must never invalidate stable pages.

## 2025-12-13: `BlockFile::write_block` does not auto-allocate

**Decision**
- `BlockFile::write_block(block_id, payload)` requires `block_id` to already exist in the file (i.e., `block_id < num_blocks()`).
- New blocks are obtained via `allocate_block()` (and optionally written via `write_new_block(payload)`).

**Why**
- Prevent accidental sparse/implicit file growth via caller-chosen ids, which can hide bugs and produce partially-initialized blocks.
- Keep a clear invariant boundary: allocation/freeing is explicit (needed for B+tree no-overwrite updates, checkpoints, and later recovery/WAL work).
- Closer to WiredTiger’s model: the block manager write path allocates space and returns an opaque “address cookie”, rather than allowing arbitrary overwrites by id.

**Notes**
- We still allow overwriting existing blocks (needed for the header page and for free-list metadata in this simplified engine).
- Future slices (WAL/checkpoint) will likely move callers to “write-new + root swap” patterns, and tighten in-place writes further.
- On-disk header uses `u64` block pointers with `0` meaning “none” (block 0 is reserved for the header page).
- Free space reuse is currently a persisted **singly-linked free list** of block IDs (simple Slice B mechanism).

**Current on-disk layout (checkpoint metadata v2)**
```
====================== file ======================+
| page 0 | page 1 | page 2 | page 3 | ... | page N|
+--------+--------+--------+--------+-----+-------+

page 0 (header page):
  +--------------------+-------------------------------+
  | CRC32 (4 bytes)    | header payload (padded)       |
  +--------------------+-------------------------------+
                       | magic[8]                      |
                       | version(u16)                  |
                       | page_size(u32)                |
                       | free_list_head(u64)           |
                       | slot0.root_block_id(u64)      |
                       | slot0.generation(u64)         |
                       | slot0.crc32(u32)              |
                       | slot1.root_block_id(u64)      |
                       | slot1.generation(u64)         |
                       | slot1.crc32(u32)              |
                       | ...zero padding...            |
                       +-------------------------------+

page k (k > 0):
  +--------------------+-------------------------------+
  | CRC32 (4 bytes)    | payload bytes (padded)        |
  +--------------------+-------------------------------+
```

**Free list encoding (Slice B)**
- `free_list_head == 0` means “no free blocks”.
- If `free_list_head == X`, page `X` is a free block; its payload begins with `next_free(u64)`:
```
header.free_list_head = 7

page 7 payload:
  +----------------------+
  | next_free = 12 (u64) |
  +----------------------+
  | unused/padding...    |
  +----------------------+

page 12 payload:
  +----------------------+
  | next_free = 3 (u64)  |
  +----------------------+

page 3 payload:
  +----------------------+
  | next_free = 0 (u64)  |
  +----------------------+
```

**Allocation / freeing behavior**
- `allocate_block()`:
  - If `free_list_head != 0`, pop the head free block and advance `free_list_head` to `next_free`.
  - Otherwise, append a new page by extending the file length.
- `free_block(id)`:
  - Write the current `free_list_head` into `id`’s payload as `next_free`, then set `free_list_head = id`.

**How this differs from WiredTiger**
- WiredTiger’s block manager is checkpoint-aware and uses extent lists (e.g., alloc/avail/discard) that are snapshotted and merged across checkpoints; it is not a single persisted linked list of block IDs.
- We’re intentionally starting with the simplest persisted allocator; later phases (checkpointing) can evolve toward extent-based free space management.

**References**
- WiredTiger Block Manager docs: https://source.wiredtiger.com/develop/arch-block.html
- WiredTiger file growth tuning: https://source.wiredtiger.com/develop/tune_file_alloc.html

## 2025-12-14: Slotted leaf-page format for in-page KV (Slice C)

**Decision**
- Implement the Slice C “single-page KV” as a **slotted leaf page** stored inside one `BlockFile` payload.
- The page uses a small fixed header + a slot directory at the beginning and variable-size records packed from the end of the page.
- Keys are ordered by the **slot directory order** (lexicographic byte ordering), enabling binary search.

**Why**
- Variable-sized K/V records need indirection; a slot directory allows inserts/deletes to mostly shift small fixed-size slot entries instead of moving record bytes on every change.
- This matches the common “slotted page” pattern used by B-tree implementations and sets us up for later slices (splits, scans).

**On-page layout**
- All integers are little-endian.
- Offsets/lengths are `u16`, so the page payload must be `<= 65535` bytes (true for the default 4KB pages).

Header (`HEADER_SIZE = 8` bytes):
```
byte 0: page_type (u8) = 1 (leaf)
byte 1: flags (u8)     = 0 (reserved)
byte 2: slot_count (u16)
byte 4: lower (u16)  = HEADER_SIZE + slot_count * SLOT_SIZE
byte 6: upper (u16)  = start of packed record bytes (grows downward)
```

Slot (`SLOT_SIZE = 4` bytes), stored in sorted key order:
```
offset (u16): record start within the page payload
len    (u16): record length in bytes
```

Record bytes, packed from the end of the page toward the front:
```
klen (u16)
vlen (u16)
key bytes (klen)
value bytes (vlen)
```

**Free space / fragmentation**
- Contiguous free space is `upper - lower`.
- `delete(key)` removes the slot entry but leaves record bytes as unreachable garbage.
- `put(key, value)` performs `compact()` automatically if `upper - lower` is insufficient; if still insufficient it returns `PageFull`.
- `compact()` rewrites the page into a tightly packed form (no garbage), preserving slot order and updating offsets.

**Notes**
- This is intentionally a simplified page format for Slice C; later slices can add page ids, overflow handling, and internal pages.

**References**
- PostgreSQL storage page layout: https://www.postgresql.org/docs/current/storage-page-layout.html
- SQLite database file format (B-tree pages): https://www.sqlite.org/fileformat2.html

## 2025-12-14: 2-level B+tree internal-root format + root persistence (Slice D)

**Decision**
- Introduce an **internal page** format (`page_type = 2`) for Slice D (root + leaf pages only).
- Store the tree’s current root block id in the file header’s `root_block_id`, and persist updates via a new API: `BlockFile::set_root_block_id(u64)`.
- Root can point at either a **leaf page** (`page_type = 1`) or an **internal page** (`page_type = 2`); on the first leaf split we *promote* the root to an internal page.

**Why**
- Slice D needs durable routing from root → leaf pages, so an on-disk internal node format is required.
- Persisting `root_block_id` in the header provides a stable “entry point” for recovery and later slices (WAL/checkpoint will evolve this to root-swap patterns).
- Keeping the initial tree as a single leaf until the first split keeps the bootstrapping simple.

**Internal page semantics (separator keys)**
- The internal page stores **separator keys** that represent the **minimum key** of each child *after the first*.
- Header stores `first_child` (child pointer for keys smaller than the first separator).
- Each slot stores `(sep_key_i, child_{i+1})` and applies for keys in `[sep_key_i, next_sep_key)` (or to the end for the last separator).
- When a leaf splits into left+right, we insert a new separator key equal to the **first key of the new right leaf**, with `child = right_leaf_block_id`.

**On-page layout**
- Like leaf pages, the internal page uses a slotted layout: slot directory grows forward, records pack from the end.
- Header (`HEADER_SIZE = 16` bytes):
```
byte 0: page_type (u8) = 2 (internal)
byte 1: flags (u8)     = 0 (reserved)
byte 2: slot_count (u16)
byte 4: lower (u16)  = HEADER_SIZE + slot_count * SLOT_SIZE
byte 6: upper (u16)  = start of packed record bytes (grows downward)
byte 8: first_child (u64) - child pointer for keys < key_at(0)
```
- Slot (`SLOT_SIZE = 4` bytes), stored in sorted separator-key order:
```
offset (u16): record start within the page payload
len    (u16): record length in bytes
```
- Record bytes:
```
klen (u16)
vlen (u16) = 8
key bytes (klen)     // separator key
child (u64 LE)       // block id of child_{i+1}
```

**Notes**
- Slice D intentionally does **not** split internal pages; if the root internal page runs out of space for more separators, inserts must fail with a “root full” error until Slice E adds height growth.

## 2025-12-14: Slice E height growth (internal splits) + range scan API

**Decision**
- Implement Slice E B+tree height growth via **recursive insert** that can split both leaf and internal pages, with **root growth** when the current root splits.
- Define internal split promotion using the existing Slice D separator semantics (“separator key is the minimum key of the child to its right”):
  - When splitting an internal node, we choose a **promoted separator** `(k_promote, child_promote)`.
  - The left internal page keeps the original `first_child` and all separator entries strictly **before** `k_promote`.
  - The right internal page uses `first_child = child_promote` and keeps all separator entries strictly **after** `k_promote`.
  - The parent inserts `k_promote` pointing to the **right internal page**.
- Add a public ordered scan API: `BTree::range(start, end)` with `start` **inclusive** and `end` **exclusive**, returning keys in ascending lexicographic byte order.

**Why**
- Recursive splits + root growth are the standard B+tree mechanism that guarantees the tree stays height-balanced: only root growth increases depth, and it increases depth for **all** leaves equally.
- Reusing the Slice D “min-of-right-child” separator invariant avoids redesigning routing semantics and keeps internal-page encoding stable.
- `range(start, end)` with inclusive/exclusive bounds matches common database iterator conventions and is easy to compose (adjacent ranges don’t overlap).

**Notes**
- Slice E range scanning is implemented without leaf sibling pointers (no on-disk format change); the iterator finds the “next leaf” using a parent stack and in-order traversal.
- A future slice may add leaf sibling pointers to make scans O(1) per leaf transition and more cache-friendly.

## 2025-12-14: Export `NONE_BLOCK_ID` sentinel constant

**Decision**
- Expose `NONE_BLOCK_ID` as a public constant so code can avoid using raw `0` when representing “no block / null pointer”.

**Why**
- Eliminates "magic number" checks (`== 0`) around root pointers and free-list links, making intent explicit and reducing the chance of accidentally treating block `0` (the header page) as a data block.

## 2025-12-14: Use `RefCell<BTree>` for primary `_id` lookups (Slice F)

**Decision**
- Keep `WrongoDB::find_one(&self, ...)` as-is and wrap the primary B+tree handle in `RefCell<BTree>` so primary lookups can call `BTree::get(&mut self, ...)` without making read APIs `&mut self`.

**Why**
- `BTree::get` requires `&mut self` today because the pager ultimately uses `std::fs::File::seek`, and `seek` is `&mut File` in Rust (moving the file cursor is a mutation).
- Slice F wants `_id` lookups to hit the primary index while keeping the existing engine API ergonomics (read operations should not require `mut db`).
- This is the smallest change that preserves the current public API while we wire in a persistent primary index.

**Tradeoffs**
- **Pros**
  - No public API break: callers keep using `find_one(&self, ...)`.
  - Minimal code churn: no need to redesign `BlockFile`/`Pager` I/O yet.
- **Cons**
  - Borrowing rules move from compile-time to runtime for the primary tree:
    - `RefCell` will panic on illegal re-entrant borrows (e.g., nested mutable borrows).
    - We should keep `borrow_mut()` scopes tight (borrow, do the `get`, drop).
  - Not thread-safe: if we ever want to share a `WrongoDB` across threads, we'll need a `Mutex`/`RwLock` (or a different I/O model).

**Alternatives considered**
- Change read APIs to `&mut self` (e.g., `find_one(&mut self, ...)`): simplest mechanically, but breaks ergonomics and public API expectations.
- Switch paging reads to positioned I/O (pread-style) or `mmap`: could allow `BTree::get(&self)`, but is a larger redesign and more platform-sensitive.
- Wrap the pager/tree in `Mutex`/`RwLock`: thread-safe, but heavier and unnecessary for the current single-threaded design.

**Mental model**
```
WrongoDB owns the BTree,
but find_one(&self) only has a shared reference to WrongoDB.

We need:  &mut BTree  (because File::seek mutates the cursor)
We have:  &WrongoDB

RefCell lets us do a runtime-checked temporary &mut to the BTree.
```

## 2025-12-14: Mongo-like `_id` defaults and uniqueness (Slice F)

**Decision**
- Default `_id` generation uses an **ObjectId-like** 24-hex string (12 bytes rendered as lower-case hex) instead of a UUID string.
- Enforce `_id` uniqueness:
  - `insert_one` returns an error if the `_id` already exists.
  - `open()` fails if the append-only log contains duplicate `_id` values.
- Primary-key encoding preserves embedded document key order (no key sorting) to better match MongoDB's "field order matters" semantics.

**Why**
- MongoDB uses `_id` as the primary key with a unique index; duplicate inserts should fail.
- MongoDB's default `_id` is `ObjectId`, not UUID.
- MongoDB's comparison and equality semantics treat embedded-document field order as significant; key-sorting would incorrectly merge distinct `_id` values.

**Tradeoffs**
- We store documents as JSON, not BSON, so this is still a best-effort approximation:
  - JSON number types don't preserve BSON numeric types (int32/int64/double/decimal), so our "1 vs 1.0" normalization is heuristic.
  - We represent ObjectId as a hex string (no dedicated ObjectId type in the document model yet).
- Failing `open()` on duplicate `_id` is strict; it treats such logs as corrupted/invalid (matching MongoDB invariants).

## 2025-12-14: Remove `MiniMongo` public alias

**Decision**
- Remove the public `MiniMongo` type alias and expose only `WrongoDB` as the engine entry point.

**Why**
- The crate and engine are named `wrongodb`; keeping a `MiniMongo` alias was confusing and no longer reflects the project naming.
- Reduces API surface area and avoids "two names for the same thing" in examples and docs.

## 2025-12-16: MongoDB wire protocol server with extensible command dispatch

**Decision**
- Implement MongoDB wire protocol (OP_MSG with document sequences, OP_QUERY/OP_REPLY for legacy) with a **Command trait + CommandRegistry** for O(1) dispatch.
- Support multi-collection CRUD: `insert` (auto `_id`), `find` (filter/skip/limit), `update` (`$set`/`$unset`/`$inc`/`$push`/`$pull`), `delete`, plus `count`, `distinct`, and basic aggregation (`$match`/`$limit`/`$skip`/`$project`).
- Command handlers in `src/commands/handlers/{connection,database,crud,index,cursor,aggregation}.rs`.

**Why**
- Enables mongosh and Rust driver connectivity with proper OP_MSG framing and `_id` materialization.
- Trait-based dispatch replaces a monolithic if-else chain, making new commands easy to add.

**Gaps** (future work): query operators (`$gt`/`$lt`/`$in`), sorting, projection, cursor batching, persistent indexes, transactions, auth.

## 2025-12-21: Proposed checkpoint-first durability via copy-on-write (Option B)

**Status**
- Proposed (not yet approved).

**Decision (proposed)**
- Use copy-on-write for page updates and keep a stable on-disk root until checkpoint commit.
- Persist checkpoints via an atomic root swap using dual header slots with generation + CRC.
- Recovery uses the last completed checkpoint; no WAL in this phase.
- Do not persist retired-block lists yet; space leaks on crash are acceptable for now.
- Checkpoints are explicit API calls in this phase (no implicit checkpoint per write).
- For metadata/turtle integration, keep the header root pointing at the primary B+tree for now; when metadata/turtle is added, repoint the header root to the metadata root and store the primary root in the metadata table.

**Why**
- Delivers crash-consistent snapshots without introducing WAL complexity at this stage.

**Notes**
- Requires a file format bump and new rules for reclaiming retired blocks.

## 2025-12-21: Header checkpoint slots (file format v2)

**Decision**
- Bump the blockfile header version to `2`.
- Replace the single `root_block_id` header field with **two checkpoint slots**.
- Each slot encodes `{root_block_id(u64), generation(u64), crc32(u32)}`; the slot CRC covers the little-endian bytes of `root_block_id` + `generation`.
- Initialize slot 0 at generation `1` with `root_block_id = 0`, and slot 1 at generation `0` with `root_block_id = 0`.

**Why**
- Per-slot CRCs + generation counters enable safe root selection and fallback during torn header writes.

## 2025-12-22: Copy-on-write B+tree updates allocate new blocks

**Decision**
- B+tree mutations never overwrite existing pages: every modified leaf/internal page is written to a new block.
- Each insert returns a new subtree root id; the tree’s header root pointer is updated on every write.
- When a node splits, both left and right siblings are new blocks; the parent rewrites its own page to point at the new child ids.
- Old blocks are left in place (no reuse) until checkpoint retirement is implemented.

**Why**
- Preserves the last durable checkpoint’s reachable pages while allowing in-progress mutations to proceed safely.
- Establishes the invariant needed for later “stable vs working root” and checkpoint commits.

## 2025-12-23: Stable root vs working root with checkpoint API

**Decision**
- Introduce two root pointers in `Pager`:
  - **stable root**: root stored in the active checkpoint slot on disk (last completed checkpoint).
  - **working root**: in-memory root used for ongoing mutations (advances with each write).
- `BTree::create()` now writes an initial checkpoint after creating the first leaf root.
- Add `BTree::checkpoint()` API that atomically swaps roots by calling `BlockFile::set_root_block_id()` and syncing.
- Mutations update the working root in memory but do not affect the stable root until `checkpoint()` is called.
- On `BTree::open()`, the working root is initialized from the stable root on disk.

**Why**
- Provides crash-consistent snapshots: the stable root always points to a valid tree state, while mutations proceed without risking corruption of the on-disk checkpoint.
- Atomic root swap ensures that a crash during a checkpoint either sees the old checkpoint or the new checkpoint, never a partially-written state.
- Separates durability boundaries (checkpoint) from write amplification (multiple mutations can be batched before a checkpoint).

## 2025-12-23: Retired block recycling only after checkpoint

**Decision**
- Track retired (replaced) page block IDs in memory during copy-on-write updates.
- Recycle retired blocks only after a successful checkpoint root swap.
- Retired block lists are not persisted yet; crashes may leak space.

**Why**
- Prevents reuse of blocks still reachable from the last durable checkpoint.
- Keeps the initial implementation simple while matching the "checkpoint-first" durability model.

**Ordering**
- Checkpoint root metadata is synced before any retired blocks are added to the free list, so the on-disk stable root never points at reclaimed blocks.

## 2025-12-23: Proposed in-memory page cache for COW coalescing

**Status**
- Proposed (not yet approved).

**Decision (proposed)**
- Add a bounded in-memory page cache between `Pager` and `BlockFile`.
- Dirty pages are written back on eviction and before checkpoint commit.
- The first mutation of a page reachable from the last checkpoint allocates a new block id; subsequent mutations reuse that working block id until the next checkpoint.
- Cache supports pin/unpin so iterators and scans can prevent eviction.

**Why**
- Reduce write amplification and block churn by coalescing repeated updates to the same pages.
- Provide a stable foundation for scan performance by keeping in-use pages resident.

**Notes**
- Eviction policy and cache sizing are finalized during implementation (LRU vs clock).

## 2025-12-23: Page cache entry shape, API, and eviction policy

**Decision**
- Define cache entries as `{ page_id, payload, dirty, pin_count, last_access }`, where `payload` is the page payload buffer and `last_access` is a monotonic counter for LRU.
- Pager cache API uses explicit pin/unpin calls:
  - `pin_page(page_id) -> PinnedPage` (read intent)
  - `pin_page_mut(page_id) -> PinnedPageMut` (write intent; marks dirty)
  - `unpin_page(PinnedPage)` and `unpin_page_mut(PinnedPageMut)` to release pins and (for mutable pins) write back the updated payload into the cache entry.
  - `flush_cache()` writes all dirty cached pages to disk and marks them clean.
- Eviction policy is LRU among unpinned pages; if all pages are pinned at capacity, cache admission returns an error.
- Cache sizing is fixed in pages with a default of 256; configuration is via `PageCacheConfig { capacity_pages, eviction_policy }`, with `Pager::create/open` using defaults and `Pager::create_with_cache/open_with_cache` enabling override.

**Why**
- Keeps cache bookkeeping straightforward and consistent with the single-threaded engine while meeting the spec’s pin/dirty/eviction requirements.
- LRU with a simple access counter is easy to implement and adequate for the expected cache sizes in this phase.

## 2025-12-23: Flush skips dirty pinned pages

**Decision**
- `Pager::flush_cache()` returns an error if it encounters a dirty cache entry with `pin_count > 0`.
- Dirty pinned pages are not flushed or evicted; callers must unpin them before checkpoint or explicit flush.

**Why**
- The mutable pinned handle owns the latest payload until `unpin_page_mut` writes it back to the cache entry.
- Flushing while a dirty page is pinned risks writing stale data and losing in-memory updates.

**Notes**
- Read-only pins remain flushable (they are not marked dirty).
- Eviction already skips pinned pages; flush uses the same rule for dirty pages.

## 2026-01-17: Pinning semantics for page cache (Slice G1)

**Decision**
- Define pin lifetime invariants for the page cache:
  - **Pinned pages cannot be evicted**: Pages with `pin_count > 0` are never selected for eviction by the LRU policy.
  - **Dirty pinned pages cannot be flushed**: `flush_cache()` returns an error if it encounters a dirty page with `pin_count > 0`.
  - **Pin/unpin must be balanced**: Every `pin_page()` or `pin_page_mut()` must have a corresponding `unpin_page()` or `unpin_page_mut_*()` call.
- Pin lifetimes:
  - **Point operations** (get/put): Pin is held only for the duration of the single page access. Unpin immediately after read/modify.
  - **Range scans/iterators**: Pin current leaf page during iteration, unpin when advancing to next leaf. Parent pages are unpinned after routing to child (safe to re-read with COW).
  - **Tree modifications**: `pin_page_mut()` holds pin until `unpin_page_mut_commit()` or `unpin_page_mut_abort()` is called.

**Why**
- Prevents use-after-free bugs where pages are evicted while still being accessed.
- Ensures flush/writeback doesn't race with in-memory modifications.
- Provides clear rules for iterator safety and concurrent access patterns.

**Iterator safety (current leaf-only pinning)**
- `BTreeRangeIter` pins only the current leaf page during iteration.
- Parent pages are unpinned after navigating down to the leaf (they may be re-read safely due to COW semantics).
- This is safe because:
  - Internal pages are immutable from the stable root perspective.
  - If a parent is evicted and re-read, the same stable page content is retrieved.
  - The iterator only needs stable access to the current leaf; mutations create new working pages that don't affect the in-memory pinned leaf.

**Tradeoffs**
- **Leaf-only pinning** minimizes pin count but may re-read parent pages during iteration if they're evicted.
- **Full-path pinning** would keep the entire path from root to leaf pinned, preventing any re-reads but consuming more cache capacity.
- Current implementation uses leaf-only pinning for simplicity; full-path pinning can be added later if needed for performance.

## 2026-03-06: `Session` stays WT-like; write orchestration moves above it

**Decision**
- Keep the public `Session` API focused on WT-style context duties:
  - `create("table:...")`
  - `open_cursor(uri)`
  - `transaction()`
  - `checkpoint()`
- Remove one-store write branching and index creation/backfill behavior from `Session`.
- Introduce an internal `store_write_path` layer for one-store write semantics and local-vs-deferred durability branching.
- Keep `collection_write_path` as the Mongo-style controller for document and index writes.
- `Session::create("index:...")` is no longer supported; index creation stays internal to the collection write/schema path.

**Why**
- This keeps `Session` closer to `WT_SESSION` and Mongo's `OperationContext`.
- It prevents low-level session APIs from accumulating collection/index behavior.
- It keeps write-path policy out of `Cursor` while also keeping it out of `Session`.

## 2026-03-08: Split raw page images from in-memory page runtime state

**Decision**
- Rename the byte-oriented page payload object to `RawPage`.
- Introduce `Page` as the in-memory page object carried by the page cache and edit path.
- Keep dormant row-store modify state on `Page` as:
  - `RowModify { row_updates, row_inserts }`
  - `RowInsert { key, updates }`
- Do not derive `Clone` for `Page`; instead expose `Page::clone_for_edit()` so copy-on-write paths
  duplicate the raw page image and current modify state explicitly.

**Why**
- This matches the WT mental model more closely: the runtime page object owns modify state, while
  the raw page image remains a separate serialized representation.
- It keeps clone semantics explicit at the page-store boundary instead of letting a blanket
  `Clone` silently define how runtime update state should be copied.

**Notes**
- `RawPage` remains the cloneable serialized image used for block I/O and page-format parsing.
- `Page` keeps only a narrow public surface; raw mutation lives behind crate-private helpers.
- `RowModify` is scaffold-only in this pass; transaction and reconciliation logic still live in
  `TransactionManager`.

## 2026-03-08: B-tree search and leaf updates move out of page views

**Decision**
- Treat `LeafPage` and `InternalPage` as low-level page-format views.
- Move binary-search and child-routing helpers into `src/storage/btree/search.rs`.
- Move leaf `get` / `contains` / `put` / `delete` / `compact` behavior into
  `src/storage/btree/leaf_ops.rs`.
- Move internal separator-update / compaction behavior into
  `src/storage/btree/internal_ops.rs`.
- Keep `BTreeCursor` as the orchestration layer that calls those helpers.

**Why**
- Page-view types should expose slot, key, and value primitives, not own tree search semantics.
- Centralizing search in the B-tree layer makes the future page-local MVCC lookup cleaner because
  the cursor/tree code can work with slot and insertion positions directly.
- Reusable helper modules avoid duplicating binary-search and leaf mutation logic across the cursor,
  range iterator, and page-layout builders.

## 2026-03-08: B-tree point reads consume a visibility object instead of transaction-manager state

**Decision**
- Introduce `txn::ReadVisibility` as the read-time visibility context passed into `BTreeCursor::get`.
- Keep `TransactionManager` responsible for transaction lifecycle and update-chain bookkeeping, but
  move point-read visibility checks into the B-tree layer.
- Have `Table::get_version` build a `ReadVisibility` from the bound transaction id and pass it down.

**Why**
- The B-tree should resolve visibility from page-local update chains without depending directly on
  `TransactionManager`.
- A small visibility object preserves a clean boundary: transaction code produces read context, and
  storage code consumes it.
- This is the right API shape for the later move from global MVCC maps to page-owned update chains.

## 2026-03-08: B-tree writes split MVCC update attachment from base-page materialization

**Decision**
- Introduce `txn::WriteContext` as the write-time context consumed by `BTreeCursor::put` and
  `BTreeCursor::delete`.
- Treat those public B-tree write methods as page-local MVCC operations that attach updates to
  `Page::row_modify`.
- Demote the old physical tree rewrite path to crate-private `materialize_put` /
  `materialize_delete` helpers used by reconciliation and other base-image materialization paths.
- Reuse the normal pin/unpin read path to mutate page-local runtime state in place via a mutable
  pinned-page accessor, instead of routing MVCC updates through the page-image COW edit path.

**Why**
- Public B-tree writes should reflect the logical write model: user writes create update chains,
  they do not immediately rewrite raw pages.
- The existing `PageEdit` / `pin_page_mut` lifecycle is for physical page-image mutation, which may
  allocate a new page id. That is the wrong semantic for page-local runtime MVCC state.
- Keeping materialization private makes the boundary clearer: `BTreeCursor` owns write policy,
  while the page store still provides the lower-level mechanisms needed by reconciliation and
  structural page rewrites.

## 2026-03-08: Transaction-bound cursors carry a live transaction handle for page-local MVCC writes

**Decision**
- Make `ActiveWriteUnit` own the transaction behind `Arc<Mutex<Transaction>>`.
- Bind transaction-scoped cursors to a `Weak<Mutex<Transaction>>` instead of only a raw `txn_id`.
- Route session/write-unit cursor writes through `Table::local_apply_put_in_txn` /
  `local_apply_delete_in_txn`, so the B-tree returns `UpdateRef`s and the live `Transaction`
  records `TxnOp::PageUpdate(...)`.
- Keep non-transactional cursors, durability appliers, and the remaining legacy paths on the old
  transaction-manager APIs for now.
- During checkpoint with no active transactions, sweep page-local committed updates into the base
  tree before flushing and then clear runtime page-local modify state from the reachable tree.

**Why**
- Transaction-owned operation bookkeeping is cleaner than a manager-side `txn_id -> updates` map:
  commit/abort already operate on the `Transaction`, so the write path should record ops there.
- A weak transaction handle makes stale transaction-bound cursors fail cleanly after commit/abort
  instead of continuing to write with a dead transaction id.
- The checkpoint sweep is the minimal bridge needed to keep the new session-bound MVCC write path
  compatible with the existing persisted base-page/checkpoint model while scans and full
  reconciliation are still being migrated.

## 2026-03-08: Range scans merge page-local row updates and inserts inside the B-tree

**Decision**
- Change `BTreeCursor::range` to take `ReadVisibility`, matching the point-read boundary.
- Rewrite `BTreeRangeIter` so each pinned leaf is materialized into a visible row view before
  iteration continues.
- Merge three sources in sorted order inside that leaf-local view:
  base rows, `row_updates`, and `row_inserts`.
- Make `Table::scan_range` consume `BTreeCursor::range(...)` directly and remove the old
  `TransactionManager::mvcc_keys_in_range` scan indirection.

**Why**
- Scan visibility should follow the same ownership boundary as point reads: the B-tree owns
  search/iteration, while transaction code only supplies visibility context.
- Once updates are page-local, the old "scan base keys, union global MVCC keys, then re-read every
  key through TransactionManager" path is both redundant and structurally wrong.
- Materializing one visible leaf view at a time keeps the traversal logic simple while preserving
  the WT-style split between on-page row updates and inserted in-memory rows.

## 2026-03-08: Table-level reads and txn-id writes now go straight to page-local B-tree MVCC

**Decision**
- Make `Table::get_version` call `BTreeCursor::get(...)` directly instead of routing point reads
  through `TransactionManager`.
- Make `Table::local_apply_put_with_txn` / `local_apply_delete_with_txn` use `BTreeCursor`
  directly for both `TXN_NONE` and transactional writes.
- Add page-local `mark_updates_committed(txn_id)`, `mark_updates_aborted(txn_id)`, and
  `reconcile_page_local_updates(...)` operations on the B-tree so legacy txn-id-based callers
  such as durability apply can keep their shape without using the old store-global MVCC map.
- Reduce `TransactionManager` back down to global transaction lifecycle and timestamp/snapshot
  access; it no longer owns per-store MVCC chains.

**Why**
- After scans moved into the B-tree, leaving point reads and txn-id-based local writes on the old
  `TransactionManager` map would keep two competing MVCC ownership models alive.
- Durability apply and similar callers still speak in terms of `txn_id`, so the minimal clean port
  is to teach the B-tree how to commit/abort/reconcile page-local chains by transaction id.
- This keeps the runtime data model coherent: page-local chains are the source of truth, and the
  transaction layer only manages transaction state.

## 2026-03-08: Transactional page-local update tracking now uses live `Transaction` ops only

**Decision**
- Treat `TxnOp::PageUpdate(UpdateRef)` on the live [`Transaction`] as the only transactional
  commit/abort bookkeeping for page-local MVCC updates.
- Remove the temporary B-tree and table helpers that walked page-local chains by `txn_id` to mark
  updates committed or aborted.
- Restrict `Table::local_apply_put_with_txn` / `local_apply_delete_with_txn` to the non-transaction
  `TXN_NONE` path; transactional callers must pass a live `Transaction` and record ops there.
- Distinguish replay transactions from snapshot transactions so replay apply can resolve recorded
  page-local ops without pretending those transactions were ever registered in global active-txn
  state.

**Why**
- Once page-local updates are recorded directly on the live transaction, the old `txn_id` finalizer
  path is redundant and semantically weaker: it reintroduces store-wide scans and bypasses the
  transaction object that already owns commit/abort lifecycle.
- Rejecting txn-id-only transactional table writes keeps the API honest: transactional mutation now
  requires a real transaction boundary everywhere, including durability replay.
- Replay transactions need the same page-local op resolution logic as live transactions, but they
  should not distort snapshot bookkeeping such as active transaction registration or oldest-active
  calculations.

## 2026-03-08: Non-transactional writes now use implicit transactions

**Decision**
- Remove the remaining `Table::local_apply_put_with_txn` / `local_apply_delete_with_txn` runtime
  API.
- Make non-transactional cursor writes run through short-lived implicit transactions using the same
  page-local `TxnOp` path as explicit transactions.
- Make durability replay apply `TXN_NONE` write operations through the same autocommit helpers
  instead of keeping a separate txn-id-shaped fast path.

**Why**
- WT-style autocommit is still MVCC: it is an implicit one-operation transaction, not a direct
  base-page rewrite path.
- Removing the `*_with_txn(TXN_NONE)` shape eliminates the last public write API that suggested
  `TXN_NONE` was a distinct MVCC/storage mode.
- With this change, explicit transactions, implicit autocommit writes, and replayed autocommit
  writes all converge on the same page-local update-chain model; only checkpoint/reconcile keeps
  the internal materialization path.
