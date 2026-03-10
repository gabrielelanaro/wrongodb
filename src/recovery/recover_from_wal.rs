use std::collections::HashMap;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::durability::{CommittedDurableOp, DurableOp, StoreCommandApplier};
use crate::storage::wal::{RecoveryError, WalReader, WalRecord, WalRecordHeader};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

#[derive(Debug)]
enum RecoveryAction {
    ApplyChange(CommittedDurableOp),
    StageTransactionChange {
        txn_id: TxnId,
        change: BufferedRecoveryChange,
    },
    ApplyBufferedTransaction {
        txn_id: TxnId,
        commit: CommittedDurableOp,
    },
    DiscardBufferedTransaction {
        txn_id: TxnId,
    },
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BufferedRecoveryChange {
    index: u64,
    term: u64,
    op: DurableOp,
}

impl BufferedRecoveryChange {
    fn into_committed_op(self) -> CommittedDurableOp {
        CommittedDurableOp {
            index: self.index,
            term: self.term,
            op: self.op,
        }
    }
}

#[derive(Debug, Default)]
struct StagedRecoveryChanges {
    changes_by_txn: HashMap<TxnId, Vec<BufferedRecoveryChange>>,
}

impl StagedRecoveryChanges {
    fn stage(&mut self, txn_id: TxnId, change: BufferedRecoveryChange) {
        self.changes_by_txn.entry(txn_id).or_default().push(change);
    }

    fn release(&mut self, txn_id: TxnId) -> Vec<BufferedRecoveryChange> {
        self.changes_by_txn.remove(&txn_id).unwrap_or_default()
    }

    fn discard(&mut self, txn_id: TxnId) {
        self.changes_by_txn.remove(&txn_id);
    }

    fn discard_all(&mut self) {
        self.changes_by_txn.clear();
    }

    fn is_empty(&self) -> bool {
        self.changes_by_txn.is_empty()
    }
}

pub(crate) fn recover_from_wal(
    applier: Arc<StoreCommandApplier>,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    let mut staged_changes = StagedRecoveryChanges::default();
    while let Some(action) = read_next_recovery_action(&mut reader)? {
        match action {
            RecoveryAction::ApplyChange(command) => apply_change(&applier, command)?,
            RecoveryAction::StageTransactionChange { txn_id, change } => {
                stage_transaction_change(&mut staged_changes, txn_id, change);
            }
            RecoveryAction::ApplyBufferedTransaction { txn_id, commit } => {
                apply_buffered_transaction(&applier, &mut staged_changes, txn_id, commit)?;
            }
            RecoveryAction::DiscardBufferedTransaction { txn_id } => {
                discard_buffered_transaction(&mut staged_changes, txn_id);
            }
            RecoveryAction::Ignore => {}
        }
    }

    discard_incomplete_transactions(&mut staged_changes);
    checkpoint_open_stores(&applier)?;

    Ok(())
}

fn read_next_recovery_action(
    reader: &mut dyn WalReader,
) -> Result<Option<RecoveryAction>, WrongoDBError> {
    let Some((header, record)) = next_recovery_record(reader, "recovery")? else {
        return Ok(None);
    };

    Ok(Some(classify_recovery_action(header, record)))
}

fn apply_change(
    applier: &Arc<StoreCommandApplier>,
    command: CommittedDurableOp,
) -> Result<(), WrongoDBError> {
    applier.apply(command)
}

fn stage_transaction_change(
    staged_changes: &mut StagedRecoveryChanges,
    txn_id: TxnId,
    change: BufferedRecoveryChange,
) {
    staged_changes.stage(txn_id, change);
}

fn apply_buffered_transaction(
    applier: &Arc<StoreCommandApplier>,
    staged_changes: &mut StagedRecoveryChanges,
    txn_id: TxnId,
    commit: CommittedDurableOp,
) -> Result<(), WrongoDBError> {
    for change in staged_changes.release(txn_id) {
        apply_change(applier, change.into_committed_op())?;
    }
    apply_change(applier, commit)
}

fn discard_buffered_transaction(staged_changes: &mut StagedRecoveryChanges, txn_id: TxnId) {
    staged_changes.discard(txn_id);
}

fn discard_incomplete_transactions(staged_changes: &mut StagedRecoveryChanges) {
    if staged_changes.is_empty() {
        return;
    }
    staged_changes.discard_all();
}

fn checkpoint_open_stores(applier: &Arc<StoreCommandApplier>) -> Result<(), WrongoDBError> {
    applier.checkpoint_open_stores()
}

fn next_recovery_record(
    reader: &mut dyn WalReader,
    pass: &str,
) -> Result<Option<(WalRecordHeader, WalRecord)>, WrongoDBError> {
    match reader.read_record() {
        Ok(Some((header, record))) => Ok(Some((header, record))),
        Ok(None) => Ok(None),
        Err(
            err @ (RecoveryError::ChecksumMismatch { .. }
            | RecoveryError::BrokenLsnChain { .. }
            | RecoveryError::CorruptRecordHeader { .. }
            | RecoveryError::CorruptRecordPayload { .. }),
        ) => {
            eprintln!("Stopping global WAL replay during {pass} at corrupted tail: {err}");
            Ok(None)
        }
        Err(err) => Err(StorageError(format!("failed reading WAL during {pass}: {err}")).into()),
    }
}

fn classify_recovery_action(header: WalRecordHeader, record: WalRecord) -> RecoveryAction {
    match record {
        WalRecord::Put {
            store_name,
            key,
            value,
            txn_id,
        } if txn_id == TXN_NONE => RecoveryAction::ApplyChange(CommittedDurableOp {
            index: header.raft_index,
            term: header.raft_term,
            op: DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            },
        }),
        WalRecord::Put {
            store_name,
            key,
            value,
            txn_id,
        } => RecoveryAction::StageTransactionChange {
            txn_id,
            change: BufferedRecoveryChange {
                index: header.raft_index,
                term: header.raft_term,
                op: DurableOp::Put {
                    store_name,
                    key,
                    value,
                    txn_id,
                },
            },
        },
        WalRecord::Delete {
            store_name,
            key,
            txn_id,
        } if txn_id == TXN_NONE => RecoveryAction::ApplyChange(CommittedDurableOp {
            index: header.raft_index,
            term: header.raft_term,
            op: DurableOp::Delete {
                store_name,
                key,
                txn_id,
            },
        }),
        WalRecord::Delete {
            store_name,
            key,
            txn_id,
        } => RecoveryAction::StageTransactionChange {
            txn_id,
            change: BufferedRecoveryChange {
                index: header.raft_index,
                term: header.raft_term,
                op: DurableOp::Delete {
                    store_name,
                    key,
                    txn_id,
                },
            },
        },
        WalRecord::TxnCommit { txn_id, commit_ts } => RecoveryAction::ApplyBufferedTransaction {
            txn_id,
            commit: CommittedDurableOp {
                index: header.raft_index,
                term: header.raft_term,
                op: DurableOp::TxnCommit { txn_id, commit_ts },
            },
        },
        WalRecord::TxnAbort { txn_id } => RecoveryAction::DiscardBufferedTransaction { txn_id },
        WalRecord::Checkpoint => RecoveryAction::Ignore,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::durability::StoreCommandApplier;
    use crate::recovery::recover_from_wal;
    use crate::storage::table_cache::TableCache;
    use crate::storage::wal::{GlobalWal, WalFileReader};
    use crate::txn::{GlobalTxnState, TransactionManager, TXN_NONE};

    const TEST_STORE: &str = "test.main.wt";

    fn new_applier(base_path: &Path) -> Arc<StoreCommandApplier> {
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_cache = Arc::new(TableCache::new(
            base_path.to_path_buf(),
            transaction_manager.clone(),
        ));
        Arc::new(StoreCommandApplier::new(table_cache, transaction_manager))
    }

    #[test]
    fn test_recover_from_wal_replays_autocommit_writes_to_persisted_store() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();
        wal.log_put(TEST_STORE, b"k1", b"v1", TXN_NONE, 0).unwrap();
        wal.sync().unwrap();

        let reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        recover_from_wal(new_applier(dir.path()), reader).unwrap();

        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table = TableCache::new(dir.path().to_path_buf(), transaction_manager)
            .get_or_open_store(TEST_STORE)
            .unwrap();
        assert_eq!(
            table.write().get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn test_recover_from_wal_applies_only_committed_transactional_changes() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();

        wal.log_put(TEST_STORE, b"committed", b"v1", 7, 0).unwrap();
        wal.log_txn_commit(7, 7, 0).unwrap();
        wal.log_put(TEST_STORE, b"aborted", b"v2", 8, 0).unwrap();
        wal.log_txn_abort(8, 0).unwrap();
        wal.log_put(TEST_STORE, b"incomplete", b"v3", 9, 0).unwrap();
        wal.sync().unwrap();

        let reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        recover_from_wal(new_applier(dir.path()), reader).unwrap();

        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table = TableCache::new(dir.path().to_path_buf(), transaction_manager)
            .get_or_open_store(TEST_STORE)
            .unwrap();
        let mut table = table.write();

        assert_eq!(
            table.get_version(b"committed", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(table.get_version(b"aborted", TXN_NONE).unwrap(), None);
        assert_eq!(table.get_version(b"incomplete", TXN_NONE).unwrap(), None);
    }
}
