## 1. Specification
- [x] 1.1 Add wal-recovery spec with logical WAL requirements and scenarios
- [x] 1.2 Record WAL format decision in docs/decisions.md
- [x] 1.3 Add WiredTiger comparison notes to NOTES.md

## 2. WAL format and logging
- [x] 2.1 Introduce logical record types (Put/Delete/Checkpoint) and bump WAL version
- [x] 2.2 Remove page-id split/insert record serialization and readers
- [x] 2.3 Update WAL unit tests for logical records

## 3. Recovery pipeline
- [x] 3.1 Replace RecoveryEngine with logical replay using BTree::put/delete while WAL is disabled
- [x] 3.2 Remove recovery_mode and page_allocation_map usage
- [x] 3.3 Decide and implement recovery checkpoint + WAL reset behavior

## 4. Tests and docs
- [x] 4.1 Update tests/btree_recovery.rs expectations for logical replay
- [x] 4.2 Update docs/wal_recovery_implementation.md and docs/wal_recovery_comparison.md
- [x] 4.3 Run cargo test (btree_wal, btree_recovery)
