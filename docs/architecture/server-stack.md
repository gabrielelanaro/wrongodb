# Server Stack

This document describes the current architecture above the storage API.

## Startup flow

Server startup currently looks like this:

1. open a `Connection`
2. run `CatalogRecovery::reconcile`
3. build a `DatabaseContext`
4. accept MongoDB wire-protocol requests
5. dispatch each command through `CommandRegistry`

The startup reconciliation step matters because the server depends on both storage metadata and the durable collection catalog being internally consistent.

## Main server services

### `DatabaseContext`

File:
- `src/api/database_context.rs`

Role:
- groups the server-only services layered on top of one storage `Connection`

It currently owns:

- `CollectionCatalog`
- `DocumentQuery`
- `CollectionWritePath`

This keeps the MongoDB command handlers thin and prevents server-only policy from leaking into the storage API.

### `CollectionCatalog`

Files:
- `src/catalog/catalog_store.rs`
- `src/catalog/collection_catalog.rs`

Role:
- durable Mongo-visible catalog stored in `file:_catalog.wt`

It persists collection definitions that include:

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

- resolves the committed collection definition from the catalog
- opens a storage `TableCursor`
- performs direct `_id` lookup when possible
- performs a simple single-index equality plan when a ready secondary index matches the filter
- falls back to a full table scan otherwise

This is still a deliberately small query planner.

### `CollectionWritePath`

File:
- `src/collection_write_path.rs`

Role:
- document-level write orchestration for collections and indexes

Current behavior:

- creates collections with explicit `storageColumns`
- validates and normalizes documents
- encodes storage rows
- performs insert, update, and delete through the storage API
- registers secondary indexes in both storage metadata and the durable collection catalog
- delegates index build/backfill to `CollectionCatalog::build_and_mark_index_ready`, which calls `Session::create_index`

## Current write layering

The write path deliberately crosses three layers:

1. server/document semantics in `CollectionWritePath`
2. durable collection state in `CollectionCatalog`
3. row/table/index mutation in `Session` and `TableCursor`

That split is important:

- the server layer owns BSON/document semantics
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
- document semantics should not move down into the storage engine
- storage metadata and collection catalog must remain distinct persistence planes
- index readiness is a server-level concern even though index stores are built through `Session`
- collection creation is explicit; writes do not auto-create collections

## Extension guidelines

When adding server features:

- extend `CollectionCatalog` if the change affects durable collection metadata
- extend `DocumentQuery` if the change affects read planning or result materialization
- extend `CollectionWritePath` if the change affects document mutation or index registration
- avoid adding BSON-specific rules to `Session` or lower storage modules unless the storage abstraction itself is changing

For the current user-visible command surface and query limitations, see [`command-query-capabilities.md`](command-query-capabilities.md).
