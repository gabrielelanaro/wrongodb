# WiredTiger vs Minimongo Recovery

Date: 2026-02-07
Status: Active

## Alignment

Both systems use logical WAL replay with transaction marker filtering, but WrongoDB now stages
transactional changes until commit replay instead of doing a separate preclassification pass.

```text
WAL scan -> stage txn changes -> commit/abort decides replay or discard
```

## Minimongo (current)

- One global WAL file per database directory (`global.wal`)
- Data records include `store_name` to target main/index B-trees
- Recovery runs at connection open
- Only committed txns are replayed
- Transactional changes become effective when `TxnCommit` is replayed

## WiredTiger (reference shape)

- One connection-level log subsystem
- File identifiers in log records route operations to btrees
- Recovery builds transaction visibility state, then reapplies logical records

## Practical result

Moving from per-table WAL to global WAL removes multi-file recovery coordination
ambiguity and makes cross-collection transaction replay deterministic.
