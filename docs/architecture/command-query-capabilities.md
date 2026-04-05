# Command And Query Capabilities

This document describes the MongoDB wire-protocol command surface that WrongoDB implements today.

It is intentionally a current-state capability map, not a compatibility promise.

## Implemented command groups

### Connection and handshake

Implemented commands:

- `hello`
- `isMaster`
- `ismaster`
- `ping`
- `buildInfo`
- `buildinfo`
- `serverStatus`
- `connectionStatus`

Current behavior:

- enough handshake and metadata is returned for clients such as `mongosh`
- `isWritablePrimary`, `isMaster`, `readOnly`, and optional `primary` reflect the configured replication role
- the rest of the response is mostly static apart from timestamps
- there is no authentication model behind `connectionStatus`

### Database and collection management

Implemented commands:

- `listDatabases`
- `listCollections`
- `createCollection`
- `create`
- `dbStats`
- `collStats`

Current behavior:

- `listDatabases` is admin-only and enumerates database names from the namespace-keyed durable
  catalog; the reserved `local` database is present for oplog and replication-state namespaces
- `listCollections` is backed by the durable collection catalog for the requested database
- `createCollection` requires `storageColumns`
- collection creation is explicit; writes do not auto-create collections
- `dbStats` and `collStats` compute counts, but several size-related fields are placeholders

Important constraint:

- `createCollection` fails unless `storageColumns` is provided as an array of strings

### CRUD

Implemented commands:

- `insert`
- `find`
- `update`
- `delete`

Current behavior:

- `insert` accepts `documents`
- `_id` is generated when missing
- writes go through the document-layer write path and then the storage API
- `update` supports batched updates through `updates`
- `delete` supports batched deletes through `deletes`

Current update/delete semantics:

- `update` respects the `multi` flag
- `delete` treats `limit == 1` as delete-one and everything else as delete-many
- upsert is not implemented
- replacement-vs-operator behavior is intentionally narrow and follows the current `apply_update` implementation rather than full MongoDB semantics

### Index management

Implemented commands:

- `listIndexes`
- `createIndexes`

Current behavior:

- `_id_` is always reported as the implicit primary index
- secondary indexes are persisted in the namespace-keyed durable collection catalog and storage metadata
- index build/backfill is triggered through `Session::create_index`

Important constraints:

- only single-field ascending indexes are supported
- the indexed field must be declared in the collection's `storageColumns`
- `createIndexes` does not auto-create the collection

### Aggregation and derived queries

Implemented commands:

- `count`
- `distinct`
- `aggregate`

Current behavior:

- `count` uses the same document query path as `find`
- `distinct` computes distinct values in the server layer after reading matching documents
- `aggregate` is an in-memory pipeline over fetched documents and now uses the same
  server-side cursor continuation model as `find`

Supported aggregation stages today:

- `$match`
- `$limit`
- `$skip`
- `$count`
- `$project`

This is not a general aggregation engine. The current implementation fetches documents first and then applies the supported stages in process.

### Cursor commands

Implemented commands:

- `getMore`
- `killCursors`

Current behavior:

- server-side cursor state is process-local and keyed by non-zero cursor ids
- `find` returns a non-zero cursor id when more results remain after `firstBatch`
- `listCollections`, `listIndexes`, and `aggregate` page through saved server-side cursor state
- `getMore` resumes the saved cursor and returns `nextBatch`
- `killCursors` removes saved cursor state
- cursor state is not durable; a server restart drops live cursors

### Replication

Implemented commands:

- `replSetUpdatePosition`

Current behavior:

- secondary members report `lastWritten` and `lastApplied` progress by node name
- the primary stores that progress in the in-memory replication coordinator
- this is used by the secondary apply loop after it persists and applies oplog entries

## Query capabilities

The read path is implemented in [`src/document_query.rs`](../../src/document_query.rs).

### Supported filter shape

Current filters are intentionally small:

- no filter, meaning full collection scan
- exact `_id` lookup
- `_id: { "$gt": scalar }`
- `_id: { "$gte": scalar }`
- exact equality on one indexed field when a ready secondary index exists
- exact field equality checks evaluated document-by-document

The matching model is still intentionally narrow. Only `_id.$gt` and `_id.$gte`
were added for oplog-style continuation. There is still no general operator
engine for `$lt`, `$in`, `$or`, nested expression trees, or sort-aware
planning.

### Query planning

Current planning is:

1. if the filter is empty, scan the table
2. if the filter includes `_id`, do a direct primary-key lookup
3. if the filter includes `_id.$gt` or `_id.$gte`, do a primary-key range scan
4. otherwise, if there is a ready secondary index whose field appears in the filter, do an equality range scan on that index
5. otherwise, fall back to a full table scan

This is deliberately narrow and easy to reason about.

### Skip and limit

`find` currently supports:

- `skip`
- `limit`
- `batchSize`
- `cursor.batchSize`
- `singleBatch`

`skip` is applied on the initial `find` only. `limit` is a total result budget
across the lifetime of the cursor. Server-side continuation resumes from the
last emitted raw key rather than materializing the full result eagerly.

### Distinct

`distinct`:

- uses the current filter support described above
- deduplicates values in memory
- does not use a separate distinct-aware index strategy

### Aggregation

`aggregate` currently:

- reads the collection through the document query path with no pushed-down pipeline planning
- applies supported stages in memory
- may return a live server-side cursor when the first batch is shortened

### Tailable oplog reads

WrongoDB now supports a narrow oplog-only slice of MongoDB's tailable cursor
behavior:

- `find` on `local.oplog.rs` accepts `tailable: true`
- `find` on `local.oplog.rs` accepts `awaitData: true`
- `getMore` on a tailable oplog cursor can wait for new committed oplog rows
- oplog continuation is driven by `_id` order, where `_id` is the oplog index

Important constraints:

- `tailable` and `awaitData` are only supported on `local.oplog.rs`
- there is no general capped-collection subsystem
- there are no exhaust cursors
- ordinary collection cursors are bookmark-based, not one frozen snapshot across
  all batches

## Major limitations

This is the short list that matters when deciding whether to extend the current server path or redesign it.

- explicit `storageColumns` required at collection creation time
- only single-field ascending secondary indexes
- indexed fields must be declared up front in `storageColumns`
- no sort support in `find`
- no generalized query operator engine
- no upsert semantics
- aggregation is intentionally partial and in-memory
- `tailable` and `awaitData` are oplog-only
- cursor state is in-memory only and disappears on restart
- several stats responses include placeholder size metrics

## Where to extend

If you add new capabilities, update this document in the same change and place the work in the right layer:

- new command surface or response semantics: `src/server/commands/*`
- new collection/query behavior: `src/document_query.rs`
- new document write semantics: `src/collection_write_path.rs`
- new durable metadata for collections or indexes: `src/catalog/*`
- new storage/index primitives needed by the server: `src/storage/*`
