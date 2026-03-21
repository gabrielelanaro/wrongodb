# Global WAL Architecture

Date: 2026-03-21
Status: Active

## Summary

WrongoDB now uses a **single connection-level WAL** per database directory:

- WAL file path: `<db_dir>/global.wal`
- Raft hard-state sidecar: `<db_dir>/raft_state.bin`
- Replication consistency sidecar: `<db_dir>/raft_consistency.bin`
- Scope: all main tables (`*.main.wt`) and index tables (`*.idx.wt`)
- Recovery: one pass at `Connection::open`

When WAL is enabled, startup also loads Raft hard state (`current_term`, `voted_for`) from
`raft_state.bin`.

## Record model

Global WAL records are logical storage records and include the target logical URI for row operations:

- `Put { uri, key, value, txn_id }`
- `Delete { uri, key, txn_id }`
- `TxnCommit { txn_id, commit_ts }`
- `TxnAbort { txn_id }`
- `Checkpoint`

The WAL is storage-only. Record headers do not carry RAFT term/index identity.

## WAL header (v4)

WAL file header persists only storage-native recovery state:

- `last_lsn`
- `checkpoint_lsn`

## Raft hard state sidecar

`raft_state.bin` stores:

- `current_term`
- `voted_for`

with magic/version and CRC32 validation.

Startup policy:

- Missing file: initialize default hard state.
- Corrupt/invalid file: fail startup (fail-fast safety).

## Replication consistency sidecar

`raft_consistency.bin` stores replication-owned logical restart markers:

- `applied_through_index`
- `oplog_truncate_after` (reserved placeholder)

This file is versioned and CRC-protected. It is owned by the replication layer,
not by the WT-like storage layer.

## Durability boundary

`TxnCommit` is the transaction durability boundary.

- On commit: write `TxnCommit`, then `fsync` WAL, then flip MVCC visibility.
- On abort: write `TxnAbort` (no mandatory `fsync`).

## Raft leader gate

When WAL is enabled and Raft mode is active, WAL mutation APIs are leader-gated:

- Leader node: append proceeds normally.
- Non-leader node: mutation APIs fail with `NotLeader` (`code=10107`, `NotWritablePrimary` on wire protocol paths).

This applies to row mutations, txn commit/abort markers, and checkpoint-truncate WAL operations.

## Recovery flow

Physical recovery runs once at connection open:

1. Read WAL records from the checkpoint boundary forward.
2. Apply `TXN_NONE` `Put/Delete` immediately.
3. Stage transactional `Put/Delete` by `txn_id`.
4. On `TxnCommit`, replay staged changes for that transaction and finalize commit visibility.
5. On `TxnAbort` or EOF, discard staged changes for that transaction.
6. Checkpoint replayed stores to persist new roots.

Replication restart is a later startup phase:

1. Open the RAFT protocol log.
2. Read `raft_consistency.bin`.
3. Validate `applied_through_index <= protocol_last_log_index`.
4. Resume logical replay from `applied_through_index + 1`.

## Checkpoint and truncation

Collection checkpoint now coordinates global durability:

1. Flush/checkpoint all open table handles.
2. Write one global `Checkpoint` record.
3. Sync WAL and truncate to header.

On truncation:

- WAL records are removed from disk.
- The file header keeps only storage recovery metadata.

## Legacy per-table WAL files

Per-table `*.wal` files are no longer used.

- They are ignored during startup.
- Startup logs a warning if legacy files are found.
- Optional cleanup (after successful open/checkpoint):

```bash
find <db_dir> -type f -name '*.wal' ! -name 'global.wal' -delete
```
