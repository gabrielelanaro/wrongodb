# Session + Transaction Architecture

Date: 2026-02-07
Status: Active

This document summarizes the active session/transaction architecture after the
global WAL migration.

## Session role

`Session` owns transaction lifecycle and table handle access.

```text
Session
  ├─ DataHandleCache (open tables)
  ├─ GlobalTxnState (shared visibility state)
  └─ GlobalWal handle (optional)
```

## Commit flow (current)

```text
1) Write TxnCommit(txn_id) to global.wal
2) fsync global.wal
3) Commit transaction state
4) Mark touched table MVCC updates committed
```

## Abort flow (current)

```text
1) Roll back recorded index ops
2) Write TxnAbort(txn_id) to global.wal
3) Mark transaction aborted and mark table MVCC updates aborted
```

## Checkpoint flow (current)

`Collection::checkpoint` delegates to a session-level global checkpoint coordinator:

1. Checkpoint all open table handles.
2. Emit one WAL `Checkpoint` record.
3. Sync and truncate `global.wal`.

## Recovery ownership

Recovery is not session-owned.

- Runs once at `Connection::open`
- Uses WAL commit/abort markers to filter replay
- Replays logical ops across all stores
