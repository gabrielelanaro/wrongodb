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
- Replay applies only committed transactions (plus `TXN_NONE` ops)

## Recovery algorithm

1. Open `global.wal` and validate header/CRC.
2. First pass: build `RecoveryTxnTable` from `TxnCommit` / `TxnAbort` markers.
3. Finalize pending transactions as aborted.
4. Second pass: replay eligible `Put/Delete` records in WAL order.
5. Checkpoint replayed stores to persist roots.

## Corruption handling

- Header corruption: recovery is skipped with warning.
- Corrupt tail/partial record: replay stops at last valid record.
- No panic path is expected from WAL parse failures.

## Notes

- Per-table `*.wal` files are ignored (hard cutover policy).
- WAL replay does not use per-table recovery paths anymore.
