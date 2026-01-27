# Notes: Duplicate keys in B+tree indexes (WiredTiger + minimongo)

## Evidence from the WiredTiger reference code
- `wiredtiger/src/docs/file-formats.dox`: WiredTiger requires unique key/value pairs; duplicate keys are not supported at the engine level.
- `wiredtiger/src/cursor/cur_table.c`: inserts detect duplicate keys and treat them as duplicate-key/overwrite cases.

## Implication for non-unique logical indexes (e.g., tenant_id)
Since the storage engine requires unique keys, non-unique logical indexes must be made unique in the *physical* key.
The common pattern is a composite key:

```
(logical_key, record_id) -> record reference
```

This keeps entries ordered by the logical key while still guaranteeing uniqueness. To answer "all docs for tenant X",
you do a range scan over the prefix `(tenant_id = X, *)` and read each record id.

## minimongo design choice (matches the above)
- `minimongo/openspec/changes/add-persistent-index-storage/design.md`: we already decided to encode the index key as
  `(scalar key, record offset)` to support duplicates without posting lists.

## Posting lists alternative (not chosen here)
An alternative is to store a single key and point it at a posting list of record ids. We explicitly avoided this for now
in favor of the composite-key approach above.

# Notes: WiredTiger WAL recovery (logical replay)

## Evidence from the WiredTiger reference code
- WAL logs logical row/column operations (put/remove/modify) using file ids + keys/recnos, not page ids.
  - `wiredtiger/src/txn/txn_log.c`
- Recovery replays log records by opening cursors with overwrite and calling normal insert/remove paths.
  - `wiredtiger/src/txn/txn_recover.c`
- Recovery is bounded by checkpoint LSNs and skips missing files; not-found removes are tolerated.
  - `wiredtiger/src/txn/txn_recover.c`

## Implications for minimongo
- Prefer logical WAL records and replay via BTree writes to avoid page-id mapping and split logging.
- Make replay idempotent (put as upsert, delete missing keys ok).
