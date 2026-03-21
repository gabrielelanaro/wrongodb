use std::collections::HashMap;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::durability::{CommandApplier, DurableOp};
use crate::storage::wal::{RecoveryError, WalReader, WalRecord, WalRecordHeader};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

// ============================================================================
// Recovery Planning
// ============================================================================

#[derive(Debug)]
enum RecoveryAction {
    ApplyChange(DurableOp),
    StageTransactionChange {
        txn_id: TxnId,
        change: BufferedRecoveryChange,
    },
    ApplyBufferedTransaction {
        txn_id: TxnId,
        commit: DurableOp,
    },
    DiscardBufferedTransaction {
        txn_id: TxnId,
    },
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BufferedRecoveryChange {
    op: DurableOp,
}

impl BufferedRecoveryChange {
    fn into_op(self) -> DurableOp {
        self.op
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

// ============================================================================
// Public API
// ============================================================================

/// Replays committed WAL operations into the storage command applier.
///
/// Recovery buffers transactional changes until a matching commit record is
/// observed, discards incomplete transactions at the end of the log, and
/// checkpoints any stores opened during replay.
pub(crate) fn recover_from_wal(
    applier: Arc<impl CommandApplier>,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    let mut staged_changes = StagedRecoveryChanges::default();
    while let Some(action) = read_next_recovery_action(&mut reader)? {
        match action {
            RecoveryAction::ApplyChange(op) => {
                apply_change(applier.as_ref(), op)?;
            }
            RecoveryAction::StageTransactionChange { txn_id, change } => {
                stage_transaction_change(&mut staged_changes, txn_id, change);
            }
            RecoveryAction::ApplyBufferedTransaction { txn_id, commit } => {
                apply_buffered_transaction(applier.as_ref(), &mut staged_changes, txn_id, commit)?;
            }
            RecoveryAction::DiscardBufferedTransaction { txn_id } => {
                discard_buffered_transaction(&mut staged_changes, txn_id);
            }
            RecoveryAction::Ignore => {}
        }
    }

    discard_incomplete_transactions(&mut staged_changes);
    checkpoint_open_stores(applier.as_ref())?;

    Ok(())
}

// ============================================================================
// Private Helpers
// ============================================================================

fn read_next_recovery_action(
    reader: &mut dyn WalReader,
) -> Result<Option<RecoveryAction>, WrongoDBError> {
    let Some((header, record)) = next_recovery_record(reader, "recovery")? else {
        return Ok(None);
    };

    Ok(Some(classify_recovery_action(header, record)))
}

fn apply_change(applier: &impl CommandApplier, op: DurableOp) -> Result<(), WrongoDBError> {
    applier.apply(op)
}

fn stage_transaction_change(
    staged_changes: &mut StagedRecoveryChanges,
    txn_id: TxnId,
    change: BufferedRecoveryChange,
) {
    staged_changes.stage(txn_id, change);
}

fn apply_buffered_transaction(
    applier: &impl CommandApplier,
    staged_changes: &mut StagedRecoveryChanges,
    txn_id: TxnId,
    commit: DurableOp,
) -> Result<(), WrongoDBError> {
    for change in staged_changes.release(txn_id) {
        apply_change(applier, change.into_op())?;
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

fn checkpoint_open_stores(applier: &impl CommandApplier) -> Result<(), WrongoDBError> {
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

fn classify_recovery_action(_header: WalRecordHeader, record: WalRecord) -> RecoveryAction {
    match record {
        WalRecord::Put {
            uri,
            key,
            value,
            txn_id,
        } if txn_id == TXN_NONE => RecoveryAction::ApplyChange(DurableOp::Put {
            uri,
            key,
            value,
            txn_id,
        }),
        WalRecord::Put {
            uri,
            key,
            value,
            txn_id,
        } => RecoveryAction::StageTransactionChange {
            txn_id,
            change: BufferedRecoveryChange {
                op: DurableOp::Put {
                    uri,
                    key,
                    value,
                    txn_id,
                },
            },
        },
        WalRecord::Delete { uri, key, txn_id } if txn_id == TXN_NONE => {
            RecoveryAction::ApplyChange(DurableOp::Delete { uri, key, txn_id })
        }
        WalRecord::Delete { uri, key, txn_id } => RecoveryAction::StageTransactionChange {
            txn_id,
            change: BufferedRecoveryChange {
                op: DurableOp::Delete { uri, key, txn_id },
            },
        },
        WalRecord::TxnCommit { txn_id, commit_ts } => RecoveryAction::ApplyBufferedTransaction {
            txn_id,
            commit: DurableOp::TxnCommit { txn_id, commit_ts },
        },
        WalRecord::TxnAbort { txn_id } => RecoveryAction::DiscardBufferedTransaction { txn_id },
        WalRecord::Checkpoint => RecoveryAction::Ignore,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use serde::Serialize;
    use tempfile::tempdir;

    use crate::durability::{CommandApplier, DurableOp, StoreCommandApplier};
    use crate::recovery::recover_from_wal;
    use crate::replication::RaftMode;
    use crate::storage::api::{Connection, ConnectionConfig};
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::{MetadataCatalog, METADATA_URI};
    use crate::storage::table::Table;
    use crate::storage::wal::{
        GlobalWal, Lsn, RecoveryError, WalFileReader, WalReader, WalRecord, WalRecordHeader,
    };
    use crate::txn::{GlobalTxnState, TransactionManager, TXN_NONE};

    const TEST_URI: &str = "table:test";

    #[derive(Debug, Default)]
    struct RecordingCommandApplier {
        applied: Mutex<Vec<DurableOp>>,
        checkpoint_count: Mutex<usize>,
    }

    impl RecordingCommandApplier {
        fn applied(&self) -> Vec<DurableOp> {
            self.applied.lock().clone()
        }

        fn checkpoint_count(&self) -> usize {
            *self.checkpoint_count.lock()
        }
    }

    impl CommandApplier for RecordingCommandApplier {
        fn apply(&self, op: DurableOp) -> Result<(), crate::WrongoDBError> {
            self.applied.lock().push(op);
            Ok(())
        }

        fn checkpoint_open_stores(&self) -> Result<(), crate::WrongoDBError> {
            *self.checkpoint_count.lock() += 1;
            Ok(())
        }
    }

    struct TestWalReader {
        records: VecDeque<(WalRecordHeader, WalRecord)>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "lowercase")]
    enum EncodedMetadataKind {
        Table,
    }

    #[derive(Serialize)]
    struct EncodedMetadataRecord {
        kind: EncodedMetadataKind,
        source: String,
    }

    impl TestWalReader {
        fn new(records: Vec<(WalRecordHeader, WalRecord)>) -> Self {
            Self {
                records: records.into(),
            }
        }
    }

    impl WalReader for TestWalReader {
        fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError> {
            Ok(self.records.pop_front())
        }
    }

    fn wal_entry(index: u64, record: WalRecord) -> (WalRecordHeader, WalRecord) {
        let record_type = record.record_type() as u8;
        (
            WalRecordHeader {
                record_type,
                flags: 0,
                payload_len: 0,
                lsn: Lsn::new(0, index),
                prev_lsn: Lsn::new(0, index.saturating_sub(1)),
                crc32: 0,
            },
            record,
        )
    }

    fn encode_metadata_value(source: &str) -> Vec<u8> {
        bson::to_vec(&EncodedMetadataRecord {
            kind: EncodedMetadataKind::Table,
            source: source.to_string(),
        })
        .unwrap()
    }

    fn recover_store_from_wal(base_path: &Path, store_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_handles = Arc::new(HandleCache::<String, parking_lot::RwLock<Table>>::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.to_path_buf(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));
        let applier = Arc::new(StoreCommandApplier::new(
            base_path.to_path_buf(),
            metadata_catalog,
            table_handles.clone(),
            transaction_manager.clone(),
        ));
        let reader = WalFileReader::open(GlobalWal::path_for_db(base_path)).unwrap();
        recover_from_wal(applier, reader).unwrap();

        let table = table_handles
            .get_or_try_insert_with(store_name.to_string(), |store_name| {
                let path = base_path.join(store_name);
                Ok(parking_lot::RwLock::new(Table::open_or_create_store(
                    path,
                    transaction_manager.clone(),
                )?))
            })
            .unwrap();
        let recovered = table.write().get_version(key, TXN_NONE).unwrap();
        recovered
    }

    #[test]
    fn test_recover_from_wal_replays_autocommit_writes() {
        let applier = Arc::new(RecordingCommandApplier::default());
        let reader = TestWalReader::new(vec![wal_entry(
            1,
            WalRecord::Put {
                uri: TEST_URI.to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: TXN_NONE,
            },
        )]);

        recover_from_wal(applier.clone(), reader).unwrap();

        assert_eq!(
            applier.applied(),
            vec![DurableOp::Put {
                uri: TEST_URI.to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: TXN_NONE,
            }]
        );
        assert_eq!(applier.checkpoint_count(), 1);
    }

    #[test]
    fn test_recover_from_wal_applies_only_committed_transactional_changes() {
        let applier = Arc::new(RecordingCommandApplier::default());
        let reader = TestWalReader::new(vec![
            wal_entry(
                1,
                WalRecord::Put {
                    uri: TEST_URI.to_string(),
                    key: b"committed".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: 7,
                },
            ),
            wal_entry(
                2,
                WalRecord::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                },
            ),
            wal_entry(
                3,
                WalRecord::Put {
                    uri: TEST_URI.to_string(),
                    key: b"aborted".to_vec(),
                    value: b"v2".to_vec(),
                    txn_id: 8,
                },
            ),
            wal_entry(4, WalRecord::TxnAbort { txn_id: 8 }),
            wal_entry(
                5,
                WalRecord::Put {
                    uri: TEST_URI.to_string(),
                    key: b"incomplete".to_vec(),
                    value: b"v3".to_vec(),
                    txn_id: 9,
                },
            ),
        ]);

        recover_from_wal(applier.clone(), reader).unwrap();

        assert_eq!(
            applier.applied(),
            vec![
                DurableOp::Put {
                    uri: TEST_URI.to_string(),
                    key: b"committed".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: 7,
                },
                DurableOp::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                },
            ]
        );
        assert_eq!(applier.checkpoint_count(), 1);
    }

    #[test]
    fn test_recover_from_wal_skips_aborted_write_from_real_wal_file() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();
        wal.log_put("table:items", b"abort-me", b"v1", 7).unwrap();
        wal.log_txn_abort(7).unwrap();
        wal.sync().unwrap();

        let recovered = recover_store_from_wal(dir.path(), "items.main.wt", b"abort-me");
        assert_eq!(recovered, None);
    }

    #[test]
    fn test_recover_from_wal_replays_metadata_before_data_within_same_transaction() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();

        wal.log_put(
            METADATA_URI,
            b"table:items",
            &encode_metadata_value("items.main.wt"),
            7,
        )
        .unwrap();
        wal.log_put("table:items", b"k1", b"v1", 7).unwrap();
        wal.log_txn_commit(7, 7).unwrap();
        wal.sync().unwrap();

        let conn = Connection::open(
            dir.path(),
            ConnectionConfig::new(true, RaftMode::Standalone),
        )
        .unwrap();

        assert_eq!(
            conn.metadata_catalog()
                .lookup_source("table:items")
                .unwrap(),
            Some("items.main.wt".to_string())
        );

        let mut session = conn.open_session();
        let mut txn = session.transaction().unwrap();
        let mut cursor = txn.open_cursor("table:items").unwrap();
        assert_eq!(cursor.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        txn.commit().unwrap();
    }
}
