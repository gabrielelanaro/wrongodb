# WAL Recovery: COW Bypass (Deprecated)

This document described a prior plan to bypass COW during recovery by adding a `recovery_mode` flag and
`pin_page_mut_no_cow`. The recovery implementation now uses **logical WAL replay** (WiredTiger-style), so
COW bypass is no longer needed and those mechanisms have been removed.

See:
- `docs/wal_recovery_implementation.md`
- `docs/wal_recovery_comparison.md`
