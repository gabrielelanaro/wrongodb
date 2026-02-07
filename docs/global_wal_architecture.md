# Global WAL Architecture

Date: 2026-02-07
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
3. Sync WAL and truncate to header (reset log).

## Legacy per-table WAL files

Per-table `*.wal` files are no longer used.

- They are ignored during startup.
- Startup logs a warning if legacy files are found.
- Optional cleanup (after successful open/checkpoint):

```bash
find <db_dir> -type f -name '*.wal' ! -name 'global.wal' -delete
```
