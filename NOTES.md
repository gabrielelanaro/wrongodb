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

# Notes: Sampling profiler (WrongoDB insert path)

Date: 2026-01-31

Setup
- Profiler: /usr/bin/sample, 10s @ 1ms.
- Workload: 200k inserts, 100B payload, single-threaded client (pymongo), ~12.8k ops/s.
- Server: `cargo run --release --bin server -- 127.0.0.1:27019`.

Top stacks seen during insert load (qualitative)
- Server path: `handle_connection -> CommandRegistry::execute -> InsertCommand::execute -> WrongoDB::insert_one_into -> MainTable::insert -> BTree::put -> BTree::insert_recursive`.
- B-tree inner work: `InternalPage::put_separator`, memmove/memcmp, and `build_internal_page` during splits.
- Storage I/O: `BlockFile::allocate_extent` (ftruncate) and `write_header` show up, implying file growth/header writes during inserts.
- WAL: `Wal::log_put` / `WalFile::append_record` appears in hot stacks.
- Serialization: BSON decode (OP_MSG parsing) and BSON encode (`encode_document`, `encode_id_value`) + `bson_to_value` conversions show up.

Notes
- Sample includes idle time in tokio runtime, but active samples consistently include B-tree insert + BSON/WAL work.

# Notes: Insert latency benchmark after low-hanging changes

Date: 2026-01-31

Changes applied
- BTree unique insert avoids extra `get` traversal (`insert_unique`).
- Insert path avoids `Value` wrapper clone via `insert_one_doc_into`.
- Preallocation enabled via `WRONGO_PREALLOC_PAGES=20000` when creating new files.

Setup
- WrongoDB: `cargo run --release --bin server -- 127.0.0.1:27019` with `WRONGO_PREALLOC_PAGES=20000` and fresh `test.db` files.
- MongoDB 7: docker `mongo:7` on 127.0.0.1:27018 via colima.
- Client: pymongo, retryWrites=False, directConnection=True.
- Workload: 100B payload, 100 warmup inserts, 1000 measured inserts.

Results (ms)
- WrongoDB runs: avg 0.076 / 0.067 / 0.068; p50 0.068 / 0.062 / 0.062; p95 0.106 / 0.092 / 0.096; p99 0.164 / 0.166 / 0.172
- MongoDB runs:  avg 0.258 / 0.257 / 0.241; p50 0.245 / 0.244 / 0.233; p95 0.337 / 0.341 / 0.295; p99 0.633 / 0.630 / 0.545
- Medians: WrongoDB avg 0.068 ms; MongoDB avg 0.257 ms; ratio ~0.26x

Notes
- This run includes preallocation and a fresh DB, so it is not directly comparable to earlier runs with an existing server process.

# Notes: MVCC / transactions (WiredTiger + MongoDB)

## Evidence from WiredTiger docs
- Transactions are per-session; a transaction id is assigned on first write, and snapshot isolation uses a snapshot of concurrent txns (max/min + active list).
  - `wiredtiger/src/docs/arch-transaction.dox`, `wiredtiger/src/docs/arch-snapshot.dox`
- Each key has an in-memory update chain (newest first). Reads walk the chain; if no visible update, they check the on-disk value, then the history store.
  - `wiredtiger/src/docs/arch-transaction.dox`
- Older versions are stored in a dedicated history store table. Keys include (btree id, user key, start ts, counter); values include stop ts, durable ts, type, value.
  - `wiredtiger/src/docs/arch-hs.dox`
- GC / stability relies on oldest/stable timestamps; rollback-to-stable removes updates newer than stable.
  - `wiredtiger/src/docs/arch-timestamp.dox`, `wiredtiger/src/docs/arch-rts.dox`

## Evidence from MongoDB storage API
- Storage engines expose `RecoveryUnit` + `WriteUnitOfWork` for snapshot-isolated storage transactions; snapshots open lazily on first read/write.
  - `mongodb/src/mongo/db/storage/README.md`
- Timestamped reads use a `ReadSource` to return data committed at or before a read timestamp; write conflicts are surfaced as `WriteConflictException`.
  - `mongodb/src/mongo/db/storage/README.md`

## Implications for minimongo
- A WT-like MVCC core implies per-key update chains in memory, a history store for older versions, and snapshot visibility based on txn ids + timestamps.
- A Mongo-like API surface suggests RAII write units of work and lazy snapshot creation, with retryable write conflicts.

# Notes: Engine scaling investigation (lock contention), 2026-02-08

## What was changed in WrongoDB
- Added an engine-level benchmark lock-artifact fix so insert/update groups no longer delete each other's lock stats (`benches/engine_concurrency.rs`).
- Sharded MVCC chain locking to 256 shards (`src/storage/btree/mvcc.rs`).
- Deferred transactional WAL `put/delete` writes into per-transaction pending buffers (`src/txn/global_txn.rs`) and flushed them in `SessionTxn::commit` (`src/api/session.rs`).

## Measured evidence (engine_insert_unique_scaling)
- Representative run (criterion mean throughput from `target/criterion/.../new/estimates.json`):
  - c1: ~290k ops/s
  - c16: ~213k ops/s
  - ratio c16/c1: ~0.735
- Lock stats (`target/bench-data-engine-concurrency/lock-stats-engine_insert_unique_scaling.json`) still show WAL wait dominating:
  - wal.wait_ns: ~1.78e11
  - mvcc_shard.wait_ns: ~3.41e8
  - table.wait_ns: ~4.95e8

## Profiling evidence
- Fresh c16 sample + flamegraph:
  - `/tmp/wrongo-profile/iter3-20260208-123552-current/current_c16.sample.txt`
  - `/tmp/wrongo-profile/iter3-20260208-123552-current/current_c16.svg`
  - `/tmp/wrongo-profile/iter3-20260208-123552-current/diff_vs_baseline.svg`
- Dominant stacks include:
  - `SessionTxn::commit`
  - `parking_lot::RawMutex::lock_slow`
  - `GlobalWal::log_put` / `WalFile::append_record`

## MongoDB / WiredTiger references used
- MongoDB lock hierarchy and intent locking:
  - `mongodb/src/mongo/db/shard_role/lock_manager/README.md`
    - Global/DB/Collection intent locks (IS/IX), document-level locking delegated to storage engine.
- MongoDB `WriteUnitOfWork` context and 2PL behavior:
  - `mongodb/src/mongo/db/shard_role/lock_manager/README.md`
  - `mongodb/src/mongo/db/shard_role/lock_manager/d_concurrency.h`
- WiredTiger logging slots / consolidation model:
  - `wiredtiger/src/log/log_private.h`
  - `wiredtiger/src/log/log_mgr.c`
  - key point: slot-based write consolidation and log worker processing to avoid a naive per-write global critical section.

## Current conclusion
- MVCC global lock pressure was reduced, but scaling is still bounded by WAL serialization.
- Next meaningful step should be WT-inspired WAL slot/group-commit style buffering (or equivalent background writer batching), not more broad table-lock tuning.
