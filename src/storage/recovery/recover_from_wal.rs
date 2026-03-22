use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::storage::recovery::RecoveryApplier;
use crate::storage::wal::{RecoveryError, WalReader, WalRecord, WalRecordHeader};
use crate::WrongoDBError;

#[derive(Debug)]
enum RecoveryAction {
    ApplyCommittedTransaction {
        txn_id: crate::txn::TxnId,
        ops: Vec<crate::txn::TxnLogOp>,
    },
    Ignore,
}

/// Replays committed WAL operations into the storage recovery executor.
pub(crate) fn recover_from_wal(
    applier: Arc<impl RecoveryApplier>,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    while let Some(action) = read_next_recovery_action(&mut reader)? {
        match action {
            RecoveryAction::ApplyCommittedTransaction { txn_id, ops } => {
                applier.apply_committed_transaction(txn_id, &ops)?;
            }
            RecoveryAction::Ignore => {}
        }
    }

    applier.checkpoint_open_stores()?;
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
        WalRecord::TxnCommit { txn_id, ops, .. } => {
            RecoveryAction::ApplyCommittedTransaction { txn_id, ops }
        }
        WalRecord::Checkpoint => RecoveryAction::Ignore,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use serde::Serialize;
    use tempfile::tempdir;

    use crate::storage::api::{Connection, ConnectionConfig};
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::{MetadataCatalog, METADATA_URI};
    use crate::storage::recovery::{recover_from_wal, RecoveryApplier, RecoveryExecutor};
    use crate::storage::table::{get_version, open_or_create_btree};
    use crate::storage::wal::{
        GlobalWal, Lsn, RecoveryError, WalFileReader, WalReader, WalRecord, WalRecordHeader,
    };
    use crate::txn::{GlobalTxnState, TxnLogOp, TXN_NONE};

    const TEST_URI: &str = "table:test";

    #[derive(Debug, Default)]
    struct RecordingCommandApplier {
        applied: Mutex<Vec<(u64, Vec<TxnLogOp>)>>,
        checkpoint_count: Mutex<usize>,
    }

    impl RecordingCommandApplier {
        fn applied(&self) -> Vec<(u64, Vec<TxnLogOp>)> {
            self.applied.lock().clone()
        }

        fn checkpoint_count(&self) -> usize {
            *self.checkpoint_count.lock()
        }
    }

    impl RecoveryApplier for RecordingCommandApplier {
        fn apply_committed_transaction(
            &self,
            txn_id: u64,
            ops: &[TxnLogOp],
        ) -> Result<(), crate::WrongoDBError> {
            self.applied.lock().push((txn_id, ops.to_vec()));
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
        let global_txn = Arc::new(GlobalTxnState::new());
        let store_handles =
            Arc::new(HandleCache::<String, parking_lot::RwLock<BTreeCursor>>::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.to_path_buf(),
            store_handles.clone(),
        ));
        let applier = Arc::new(RecoveryExecutor::new(
            base_path.to_path_buf(),
            metadata_catalog,
            store_handles.clone(),
            global_txn,
        ));
        let reader = WalFileReader::open(GlobalWal::path_for_db(base_path)).unwrap();
        recover_from_wal(applier, reader).unwrap();

        let store = store_handles
            .get_or_try_insert_with(store_name.to_string(), |source| {
                let path = base_path.join(source);
                Ok(parking_lot::RwLock::new(open_or_create_btree(path)?))
            })
            .unwrap();
        let recovered = get_version(&mut store.write(), key, TXN_NONE).unwrap();
        recovered
    }

    #[test]
    fn test_recover_from_wal_replays_autocommit_writes() {
        let applier = Arc::new(RecordingCommandApplier::default());
        let reader = TestWalReader::new(vec![wal_entry(
            1,
            WalRecord::TxnCommit {
                txn_id: 7,
                commit_ts: 7,
                ops: vec![TxnLogOp::Put {
                    uri: TEST_URI.to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
        )]);

        recover_from_wal(applier.clone(), reader).unwrap();

        assert_eq!(
            applier.applied(),
            vec![(
                7,
                vec![TxnLogOp::Put {
                    uri: TEST_URI.to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            )]
        );
        assert_eq!(applier.checkpoint_count(), 1);
    }

    #[test]
    fn test_recover_from_wal_applies_only_committed_transactional_changes() {
        let applier = Arc::new(RecordingCommandApplier::default());
        let reader = TestWalReader::new(vec![
            wal_entry(
                1,
                WalRecord::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                    ops: vec![TxnLogOp::Put {
                        uri: TEST_URI.to_string(),
                        key: b"committed".to_vec(),
                        value: b"v1".to_vec(),
                    }],
                },
            ),
            wal_entry(2, WalRecord::Checkpoint),
        ]);

        recover_from_wal(applier.clone(), reader).unwrap();

        assert_eq!(
            applier.applied(),
            vec![(
                7,
                vec![TxnLogOp::Put {
                    uri: TEST_URI.to_string(),
                    key: b"committed".to_vec(),
                    value: b"v1".to_vec(),
                }],
            )]
        );
        assert_eq!(applier.checkpoint_count(), 1);
    }

    #[test]
    fn test_recover_from_wal_skips_incomplete_write_from_real_wal_file() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();
        wal.log_txn_commit(
            7,
            7,
            &[TxnLogOp::Put {
                uri: "table:items".to_string(),
                key: b"abort-me".to_vec(),
                value: b"v1".to_vec(),
            }],
        )
        .unwrap();
        wal.sync().unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let len = std::fs::metadata(&wal_path).unwrap().len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(len.saturating_sub(8)).unwrap();

        let recovered = recover_store_from_wal(dir.path(), "items.main.wt", b"abort-me");
        assert_eq!(recovered, None);
    }

    #[test]
    fn test_recover_from_wal_replays_metadata_before_data_within_same_transaction() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::open_or_create(dir.path()).unwrap();

        wal.log_txn_commit(
            7,
            7,
            &[
                TxnLogOp::Put {
                    uri: METADATA_URI.to_string(),
                    key: b"table:items".to_vec(),
                    value: encode_metadata_value("items.main.wt"),
                },
                TxnLogOp::Put {
                    uri: "table:items".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
            ],
        )
        .unwrap();
        wal.sync().unwrap();

        let conn = Connection::open(dir.path(), ConnectionConfig::new(true)).unwrap();

        assert_eq!(
            conn.metadata_catalog()
                .lookup_store_name("table:items")
                .unwrap(),
            Some("items.main.wt".to_string())
        );

        let mut session = conn.open_session();
        session
            .with_transaction(|session| {
                let mut cursor = session.open_table_cursor("table:items")?;
                assert_eq!(cursor.get(b"k1")?, Some(b"v1".to_vec()));
                Ok(())
            })
            .unwrap();
    }
}
