# Server Stack

This document describes the current architecture above the storage API.

## Startup flow

Server startup currently looks like this:

1. parse `ReplicationConfig` in `src/bin/server.rs`
2. open a `Connection`
3. run `audit_catalog`
4. build a `DatabaseContext`
5. spawn the secondary replicator when the configured role is `secondary`
6. accept MongoDB wire-protocol requests
7. dispatch each command through `CommandRegistry`

The startup reconciliation step matters because the server depends on both storage metadata and the durable collection catalog being internally consistent.

During `DatabaseContext` construction, the server also ensures the namespace-keyed catalog exists,
bootstraps the reserved `local.oplog.rs` namespace, and seeds the next oplog index from durable state.
The secondary replication runtime is assembled separately and returned to `src/server/mod.rs`
when the configured role is `secondary`.

## Main server services

### `DatabaseContext`

File:
- `src/api/database_context.rs`

Role:
- groups the server-only services layered on top of one storage `Connection` and keeps command
  execution namespace-aware
- is built from explicit replication dependencies, but it does not retain the `ReplicationCoordinator` as a service locator

It currently owns:

- `CollectionCatalog`
- `DocumentQuery`
- `DdlPath`
- `WriteOps`
- `OplogAwaitService`

This keeps the MongoDB command handlers thin and prevents server-only policy from leaking into the storage API.
The replication coordinator remains an explicit constructor input so write admission and `hello`
state are policy-driven without becoming part of the context object.

### `CollectionCatalog`

Files:
- `src/catalog/catalog_store.rs`
- `src/catalog/collection_catalog.rs`

Role:
- durable Mongo-visible catalog stored in `file:_catalog.wt` and keyed by full `db.collection`
  namespace

It persists collection definitions that include:

- full namespace (`db.collection`)
- collection name
- table URI
- UUID
- options document
- declared `storageColumns`
- durable secondary index definitions and readiness

This is intentionally separate from `MetadataStore`. The server catalog answers collection-level questions; it does not replace storage metadata.

### `DocumentQuery`

File:
- `src/document_query.rs`

Role:
- read path for collection queries

Current behavior:

- resolves the committed collection definition from the namespace-keyed catalog
- opens a storage `TableCursor`
- performs direct `_id` lookup when possible
- performs a simple single-index equality plan when a ready secondary index matches the filter
- falls back to a full table scan otherwise

This is still a deliberately small query planner.

### `DdlPath`

File:
- `src/api/ddl_path.rs`

Role:
- top-level DDL path above the storage API

Current behavior:

- gates collection and index creation on writable-primary state
- creates collections with explicit `storageColumns`
- registers secondary indexes in the durable collection catalog
- repairs missing storage metadata for already-ready indexes
- delegates physical index build/backfill to `Session::create_index` and then marks the durable index ready

### `WriteOps`

File:
- `src/write_ops/executor.rs`
- `src/write_ops/mod.rs`

Role:
- top-level CRUD executor modeled after MongoDB's `write_ops_exec`

Current behavior:

- gates writes on writable-primary state
- opens one `Session`
- runs one `Session::with_transaction(...)` per logical write operation from the command layer
- calls `CollectionWritePath` only through its in-transaction mutation methods
- appends oplog rows through `ReplicationObserver`
- notifies oplog waiters after commit so oplog cursors can wake promptly

### `OplogAwaitService`

Files:
- `src/replication/await_service.rs`

Role:
- process-local wakeup service for oplog `awaitData` cursors

Current behavior:

- tracks the highest committed oplog index visible on this node
- lets waiting `getMore` calls sleep until that index advances
- is notified by `WriteOps` after committed oplog appends

### `CollectionWritePath`

File:
- `src/collection_write_path.rs`

Role:
- low-level in-transaction collection mutator

Current behavior:

- validates and normalizes documents
- encodes storage rows
- performs insert, update, and delete through the storage API
- stays oplog-agnostic; `WriteOps` owns oplog emission and wakeups

### `ReplicationObserver` and oplog

Files:
- `src/replication/observer.rs`
- `src/replication/oplog.rs`

Role:
- append one logical oplog row per replicated document mutation

Current behavior:

- stores oplog entries in the reserved internal namespace `local.oplog.rs`
- reserves `OpTime { term, index }` through `ReplicationCoordinator`
- writes oplog rows inside the same storage transaction as the user-data mutation
- supports `GenerateOplog` and `SuppressOplog` modes so `OplogApplier` can reuse `CollectionWritePath` without recursively re-oplogging

### `SecondaryReplicator`

Files:
- `src/replication/secondary.rs`
- `src/replication/applier.rs`
- `src/replication/state_store.rs`

Role:
- background runtime that keeps a secondary caught up with one primary

Current behavior:

- reads committed oplog entries through the public `find` / `getMore` path against `local.oplog.rs`
- persists fetched oplog rows locally before applying them
- replays CRUD and DDL entries through `OplogApplier`
- stores durable `lastApplied` markers in `local.repl_state`
- reports `lastWritten` and `lastApplied` progress back to the primary with `replSetUpdatePosition`

## Current write layering

The primary write path deliberately crosses five layers:

1. command handler dispatch in `src/server/*`
2. top-level write orchestration in `WriteOps`
3. document-local mutation in `CollectionWritePath`
4. logical oplog capture in `ReplicationObserver`
5. row/table/index mutation in `Session` and `TableCursor`

That split is important:

- the command/write layer owns primary admission and transaction scope
- the collection write layer owns BSON/document mutation semantics
- the replication layer owns oplog shape and position allocation
- the catalog owns committed collection/index definitions
- the storage API owns transactional row mutation and checkpoint/recovery behavior

## Current read layering

The read path is simpler:

1. command handler asks `DocumentQuery`
2. `DocumentQuery` resolves catalog state
3. `DocumentQuery` executes against `Session` cursors
4. storage rows are decoded back into documents

The server layer never reads raw files directly.

## Important invariants

- MongoDB command handlers should stay thin and delegate to `DatabaseContext` services
- mutating commands should call `WriteOps` or `DdlPath`, not `CollectionWritePath` directly
- document semantics should not move down into the storage engine
- storage metadata and collection catalog must remain distinct persistence planes
- index readiness is a server-level concern even though index stores are built through `Session`
- collection creation is explicit; writes do not auto-create collections
- the oplog is logical replication state in `local.oplog.rs`, not part of `storage/wal`
- `CollectionWritePath` should not open its own top-level transactions

## Extension guidelines

When adding server features:

- extend `CollectionCatalog` if the change affects durable collection metadata
- extend `DocumentQuery` if the change affects read planning or result materialization
- extend `WriteOps` if the change affects top-level CRUD orchestration
- extend `DdlPath` if the change affects top-level DDL semantics
- extend `CollectionWritePath` if the change affects low-level document mutation
- extend `src/replication/*` if the change affects oplog shape, write admission, or future follower apply
- avoid adding BSON-specific rules to `Session` or lower storage modules unless the storage abstraction itself is changing

For the current user-visible command surface and query limitations, see [`command-query-capabilities.md`](command-query-capabilities.md).
