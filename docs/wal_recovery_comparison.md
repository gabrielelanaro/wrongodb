# WiredTiger vs Minimongo Recovery

Date: 2026-02-07
Status: Active

## Alignment

Both systems use logical WAL replay with transaction marker filtering.

```text
WAL scan -> txn table (commit/abort) -> replay logical ops in order
```

## Minimongo (current)

- One global WAL file per database directory (`global.wal`)
- Data records include `store_name` to target main/index B-trees
- Recovery runs at connection open
- Only committed txns are replayed

## WiredTiger (reference shape)

- One connection-level log subsystem
- File identifiers in log records route operations to btrees
- Recovery builds transaction visibility state, then reapplies logical records

## Practical result

Moving from per-table WAL to global WAL removes multi-file recovery coordination
ambiguity and makes cross-collection transaction replay deterministic.
