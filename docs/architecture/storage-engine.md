# Storage Engine

This document describes the current storage architecture below the MongoDB command layer.

## Core model

WrongoDB separates storage objects into three logical URI classes:

- `file:<name>`: owns a physical `*.wt` store file and a numeric `store_id`
- `table:<collection>`: describes the primary row schema for one collection
- `index:<collection>:<name>`: describes one secondary index and its indexed columns

`metadata.wt` is the storage-layer catalog that persists those mappings. It is distinct from the server-facing collection catalog in `file:_catalog.wt`.

## Runtime ownership

### `Connection`

`Connection` owns the shared engine state:

- `MetadataStore`
- opened `BTreeCursor` handles
- `GlobalTxnState`
- `LogManager`

This keeps sessions cheap and avoids rebuilding storage/runtime state per request.

### `Session`

`Session` owns request-local state:

- the active transaction, if any
- the set of open cursor runtime states
- table/file/index cursor creation
- explicit table/index creation
- checkpoint coordination

`Session` is the boundary between shared engine state and request-scoped transactional work.

## Data layout

### Primary rows

Primary tables use structured rows rather than opaque BSON blobs. `table:` metadata declares:

- `row_format=wt_row_v1`
- key columns, currently `_id`
- value columns, taken from `storageColumns`

`src/storage/row.rs` owns the encoding and decoding of those typed row values.

### Secondary indexes

Secondary indexes are separate physical stores. The index metadata row declares the indexed columns, and the index key encoding lives in `src/index/key.rs`.

Non-unique logical indexes are represented as composite physical keys so duplicate logical values can coexist in sorted order.

## Write path

The storage write path is layered like this:

1. `Session` opens a `TableCursor` or `FileCursor`
2. `TableCursor` keeps the primary store and configured secondary indexes in sync
3. `BTreeCursor` attaches page-local MVCC updates and buffers WAL operations on the active transaction
4. `Transaction::commit` publishes visibility and hands the buffered log ops to the runtime log manager

The important boundary is that document semantics stay above this layer. Storage understands rows, tables, indexes, and transactions; it does not own BSON-level collection rules.

## Checkpointing

Checkpointing is coordinated in `src/storage/checkpoint/mod.rs`.

The current flow is:

1. pin the GC threshold in `GlobalTxnState`
2. reconcile committed page-local MVCC updates into base rows
3. flush each opened store
4. release the GC pin
5. append a checkpoint record to the WAL
6. truncate the WAL if no active transactions remain

This is a multi-store checkpoint, not a per-file isolated flush.

## Recovery

Recovery is split into two related pieces.

### WAL replay

`src/storage/recovery/recover_from_wal.rs`:

- reads committed transactions from the global WAL
- replays metadata-affecting operations first
- rebuilds the store-id to store-name map from `metadata.wt`
- replays non-metadata operations
- checkpoints the recovered stores

Replay uses the normal low-level B+tree write path in replay mode rather than a separate bespoke mutator.

### Startup catalog consistency check

`src/server/recovery/catalog.rs` provides `audit_catalog()`, which checks that:

- referenced store files actually exist on disk
- unexpected `*.wt` files are reported as orphaned stores

That keeps the server-facing catalog and the physical storage layout from silently drifting apart.

## Important invariants

- `metadata.wt` is the only storage metadata source of truth
- `file:` rows own store ids; `table:` and `index:` rows point at `file:` rows
- only `metadata.wt` is a reserved store outside normal metadata lookup
- checkpoints must see a stable GC threshold for reconciliation
- recovery must replay metadata before non-metadata store operations
- storage rows are schema-aware; collection creation is explicit and must declare `storageColumns`

## Where to extend next

Likely future evolution points:

- richer row formats and schema evolution
- broader index support than the current single-field ascending subset
- more advanced query planning on top of the existing index metadata
- background maintenance such as compaction or better space reuse

If any of those change the boundaries above, update this document in the same change.
