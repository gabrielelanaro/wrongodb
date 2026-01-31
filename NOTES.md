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
- We encode the index key as `(scalar key, record offset)` to support duplicates without posting lists.

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

# Notes: Insert latency benchmark (WrongoDB server vs MongoDB)

Date: 2026-01-31

Setup
- WrongoDB server already running on 127.0.0.1:27017 (buildInfo version 0.0.1-wrongodb). Not restarted; existing data not cleared.
- MongoDB docker container mongo:7 on 127.0.0.1:27018 via colima.
- Client: pymongo with retryWrites=False, directConnection=True, default write concern.
- Workload: 100B payload, 100 warmup inserts, 1000 measured inserts, sequential single-thread.

Results (ms)
- WrongoDB runs: avg 0.356 / 0.352 / 0.362; p50 0.350 / 0.347 / 0.352; p95 0.523 / 0.504 / 0.515; p99 0.631 / 0.597 / 0.711
- MongoDB runs:  avg 0.238 / 0.218 / 0.238; p50 0.229 / 0.212 / 0.201; p95 0.306 / 0.286 / 0.284; p99 0.342 / 0.396 / 0.903
- Medians: WrongoDB avg 0.356 ms; MongoDB avg 0.238 ms; ratio ~1.50x

Command sketch
- Start MongoDB: docker run --rm -d --name mongo-bench -p 27018:27017 mongo:7
- Bench: Python script using pymongo insert_one timing (see terminal log for exact script).
- Stop MongoDB: docker rm -f mongo-bench
