# WAL Architecture Comparison

Date: 2026-02-07
Status: Active

## Current WrongoDB architecture

WrongoDB now uses a **connection-level global WAL**.

```text
Connection
  ├─ global.wal
  ├─ users.main.wt
  ├─ users.name.idx.wt
  └─ orders.main.wt
```

All table/index operations are logged into `global.wal` with `store_name`.

## WiredTiger comparison

WiredTiger also uses connection-scoped logging.

Shared design points:
- Global ordering of operations
- One transaction commit marker durability boundary
- Single recovery pass over one log stream

## Differences

- WrongoDB currently uses a single WAL file with checkpoint truncation.
- WiredTiger uses rotating log files and more advanced slot/consolidation logic.

## Why this migration matters

Compared to historical per-table WAL behavior, global WAL provides:

1. Simpler crash semantics for multi-collection transactions.
2. One place to validate durability guarantees.
3. One recovery pass rather than per-table coordination.

## Legacy note

Historical per-table WAL design is deprecated and no longer active.
