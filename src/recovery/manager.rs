use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::durability::{CommittedDurableOp, DurableOp, StoreCommandApplier};
use crate::storage::wal::{GlobalWal, RecoveryError, WalReader, WalRecord, WalRecordHeader};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

#[derive(Debug)]
pub(crate) struct RecoveryManager {
    applier: Arc<StoreCommandApplier>,
}

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

impl RecoveryManager {
    pub(crate) fn new(applier: Arc<StoreCommandApplier>) -> Self {
        Self { applier }
    }

    pub(crate) fn recover(&self, base_path: &Path) -> Result<(), WrongoDBError> {
        let Some(mut reader) = self.read_unapplied_records(base_path)? else {
            return Ok(());
        };

        let mut staged_changes = StagedRecoveryChanges::default();
        while let Some(action) = self.read_next_recovery_action(&mut reader)? {
            match action {
                RecoveryAction::ApplyChange(command) => self.apply_change(command)?,
                RecoveryAction::StageTransactionChange { txn_id, change } => {
                    self.stage_transaction_change(&mut staged_changes, txn_id, change);
                }
                RecoveryAction::ApplyBufferedTransaction { txn_id, commit } => {
                    self.apply_buffered_transaction(&mut staged_changes, txn_id, commit)?;
                }
                RecoveryAction::DiscardBufferedTransaction { txn_id } => {
                    self.discard_buffered_transaction(&mut staged_changes, txn_id);
                }
                RecoveryAction::Ignore => {}
            }
        }

        self.discard_incomplete_transactions(&mut staged_changes);
        self.checkpoint_open_stores()?;

        Ok(())
    }

    fn read_unapplied_records(&self, base_path: &Path) -> Result<Option<WalReader>, WrongoDBError> {
        let wal_path = GlobalWal::path_for_db(base_path);
        if !wal_path.exists() {
            return Ok(None);
        }

        match WalReader::open(&wal_path) {
            Ok(reader) => Ok(Some(reader)),
            Err(err) => {
                eprintln!("Skipping global WAL recovery (failed to open WAL): {err}");
                Ok(None)
            }
        }
    }

    fn read_next_recovery_action(
        &self,
        reader: &mut WalReader,
    ) -> Result<Option<RecoveryAction>, WrongoDBError> {
        let Some((header, record)) = next_recovery_record(reader, "recovery")? else {
            return Ok(None);
        };

        Ok(Some(classify_recovery_action(header, record)))
    }

    fn apply_change(&self, command: CommittedDurableOp) -> Result<(), WrongoDBError> {
        self.applier.apply(command)
    }

    fn stage_transaction_change(
        &self,
        staged_changes: &mut StagedRecoveryChanges,
        txn_id: TxnId,
        change: BufferedRecoveryChange,
    ) {
        staged_changes.stage(txn_id, change);
    }

    fn apply_buffered_transaction(
        &self,
        staged_changes: &mut StagedRecoveryChanges,
        txn_id: TxnId,
        commit: CommittedDurableOp,
    ) -> Result<(), WrongoDBError> {
        for change in staged_changes.release(txn_id) {
            self.apply_change(change.into_committed_op())?;
        }
        self.apply_change(commit)
    }

    fn discard_buffered_transaction(
        &self,
        staged_changes: &mut StagedRecoveryChanges,
        txn_id: TxnId,
    ) {
        staged_changes.discard(txn_id);
    }

    fn discard_incomplete_transactions(&self, staged_changes: &mut StagedRecoveryChanges) {
        if staged_changes.is_empty() {
            return;
        }
        staged_changes.discard_all();
    }

    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        self.applier.checkpoint_open_stores()
    }
}

pub(crate) fn warn_legacy_per_table_wal_files(base_path: &Path) {
    let mut legacy_wals = Vec::new();

    if let Ok(entries) = fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name == "global.wal" {
                continue;
            }
            if name.ends_with(".wal") {
                legacy_wals.push(name.to_string());
            }
        }
    }

    if !legacy_wals.is_empty() {
        eprintln!(
            "Found {} legacy per-table WAL file(s); they are ignored after global WAL cutover: {}",
            legacy_wals.len(),
            legacy_wals.join(", ")
        );
    }
}

fn next_recovery_record(
    reader: &mut WalReader,
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
    use super::*;

    fn buffered_put_change(txn_id: TxnId, key: &[u8], value: &[u8]) -> BufferedRecoveryChange {
        BufferedRecoveryChange {
            index: txn_id,
            term: 1,
            op: DurableOp::Put {
                store_name: "test.main.wt".to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            },
        }
    }

    #[test]
    fn staged_recovery_changes_release_preserves_transaction_order() {
        let mut staged = StagedRecoveryChanges::default();
        let first = buffered_put_change(7, b"k1", b"v1");
        let second = buffered_put_change(7, b"k2", b"v2");

        staged.stage(7, first.clone());
        staged.stage(7, second.clone());

        assert_eq!(staged.release(7), vec![first, second]);
        assert!(staged.is_empty());
    }

    #[test]
    fn staged_recovery_changes_discard_removes_only_target_transaction() {
        let mut staged = StagedRecoveryChanges::default();
        let keep = buffered_put_change(8, b"k8", b"v8");

        staged.stage(7, buffered_put_change(7, b"k7", b"v7"));
        staged.stage(8, keep.clone());
        staged.discard(7);

        assert!(staged.release(7).is_empty());
        assert_eq!(staged.release(8), vec![keep]);
        assert!(staged.is_empty());
    }

    #[test]
    fn staged_recovery_changes_discard_all_clears_incomplete_transactions() {
        let mut staged = StagedRecoveryChanges::default();

        staged.stage(7, buffered_put_change(7, b"k7", b"v7"));
        staged.stage(8, buffered_put_change(8, b"k8", b"v8"));
        staged.discard_all();

        assert!(staged.is_empty());
    }
}
