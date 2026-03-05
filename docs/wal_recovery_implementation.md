# WAL Recovery Implementation Summary

Date: 2026-02-07
Status: Active

This document summarizes the **current** recovery implementation for WrongoDB's
connection-level global WAL. See `docs/global_wal_architecture.md` for the full
architecture and invariants.

## Current behavior

- Single WAL file per database: `<db_dir>/global.wal`
- Recovery runs once at `Connection::open`
- Replay is logical (`Put` / `Delete`), routed by `store_name`
- Replay applies non-transactional records immediately and transactional records only when their
  `TxnCommit` marker is replayed

## Recovery algorithm

1. Open `global.wal` and validate header/CRC.
2. Read unapplied records once from the checkpoint boundary forward.
3. Apply `TXN_NONE` `Put/Delete` records immediately.
4. Stage transactional `Put/Delete` records by `txn_id`.
5. On `TxnCommit`, replay the staged changes for that transaction and finalize commit visibility.
6. On `TxnAbort` or end-of-log, discard staged changes for that transaction.
7. Checkpoint replayed stores to persist roots.

## Corruption handling

- Header corruption: recovery is skipped with warning.
- Corrupt tail/partial record: replay stops at last valid record.
- No panic path is expected from WAL parse failures.

## Notes

- Per-table `*.wal` files are ignored (hard cutover policy).
- WAL replay does not use per-table recovery paths anymore.
- Recovery now follows commit-time transaction visibility, which may differ from original WAL
  position for interleaved writes.
