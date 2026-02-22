# Global WAL Architecture

Date: 2026-02-22
Status: Active

## Summary

WrongoDB now uses a **single connection-level WAL** per database directory:

- WAL file path: `<db_dir>/global.wal`
- Scope: all main tables (`*.main.wt`) and index tables (`*.idx.wt`)
- Recovery: one pass at `Connection::open`

## Record model

Global WAL records are logical and include the target store name for row operations:

- `Put { store_name, key, value, txn_id }`
- `Delete { store_name, key, txn_id }`
- `TxnCommit { txn_id, commit_ts }`
- `TxnAbort { txn_id }`
- `Checkpoint`

Each physical WAL record header (v2) also carries Raft identity metadata:

- `raft_term`
- `raft_index`

`raft_index` is explicit and monotonic; it is not derived from LSN offsets.

## WAL header (v2)

WAL file header persists both physical and Raft snapshot state:

- `last_lsn`
- `checkpoint_lsn`
- `last_raft_index`
- `snapshot_last_included_index`
- `snapshot_last_included_term`

## Durability boundary

`TxnCommit` is the transaction durability boundary.

- On commit: write `TxnCommit`, then `fsync` WAL, then flip MVCC visibility.
- On abort: write `TxnAbort` (no mandatory `fsync`).

## Recovery flow

Recovery runs once at connection open:

1. Pass 1: scan WAL and build transaction table (`committed`, `aborted`, pending).
2. Pass 2: replay `Put/Delete` only for `TXN_NONE` or committed transactions.
3. Checkpoint replayed stores to persist new roots.

## Checkpoint and truncation

Collection checkpoint now coordinates global durability:

1. Flush/checkpoint all open table handles.
2. Write one global `Checkpoint` record.
3. Sync WAL and truncate to header.

On truncation:

- `snapshot_last_included_index/term` are set to the latest appended record.
- WAL records are removed from disk.
- `last_raft_index` is preserved, so the next append receives `last_raft_index + 1`.

## Legacy per-table WAL files

Per-table `*.wal` files are no longer used.

- They are ignored during startup.
- Startup logs a warning if legacy files are found.
- Optional cleanup (after successful open/checkpoint):

```bash
find <db_dir> -type f -name '*.wal' ! -name 'global.wal' -delete
```
