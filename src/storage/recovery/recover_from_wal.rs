use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::storage::recovery::RecoveryApplier;
use crate::storage::wal::{RecoveryError, WalReader, WalRecord};
use crate::txn::{TxnId, TxnLogOp};
use crate::WrongoDBError;

#[derive(Debug)]
struct CommittedTransaction {
    txn_id: TxnId,
    ops: Vec<TxnLogOp>,
}

/// Replays committed transactions from the global WAL into storage.
///
/// Recovery stops when it reaches a corrupted tail record. That treats a torn
/// final WAL write as end-of-log, which lets startup proceed after a crash that
/// interrupted the last append.
pub(crate) fn recover_from_wal(
    applier: Arc<impl RecoveryApplier>,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    while let Some(txn) = next_committed_transaction(&mut reader)? {
        applier.apply_committed_transaction(txn.txn_id, &txn.ops)?;
    }

    applier.checkpoint_open_stores()?;
    Ok(())
}

fn next_committed_transaction(
    reader: &mut dyn WalReader,
) -> Result<Option<CommittedTransaction>, WrongoDBError> {
    loop {
        let Some(record) = read_next_record_or_stop_at_corrupt_tail(reader)? else {
            return Ok(None);
        };

        match record {
            WalRecord::TxnCommit { txn_id, ops, .. } => {
                return Ok(Some(CommittedTransaction { txn_id, ops }));
            }
            WalRecord::Checkpoint => {}
        }
    }
}

fn read_next_record_or_stop_at_corrupt_tail(
    reader: &mut dyn WalReader,
) -> Result<Option<WalRecord>, WrongoDBError> {
    match reader.read_record() {
        Ok(Some((_header, record))) => Ok(Some(record)),
        Ok(None) => Ok(None),
        Err(
            err @ (RecoveryError::ChecksumMismatch { .. }
            | RecoveryError::BrokenLsnChain { .. }
            | RecoveryError::CorruptRecordHeader { .. }
            | RecoveryError::CorruptRecordPayload { .. }),
        ) => {
            // A crash can leave the final record partially written. Recovery
            // treats that damaged tail as the end of the durable log.
            eprintln!("Stopping global WAL replay at corrupted tail: {err}");
            Ok(None)
        }
        Err(err) => Err(StorageError(format!("failed reading WAL during recovery: {err}")).into()),
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
        LogFile, Lsn, RecoveryError, WalFileReader, WalReader, WalRecord, WalRecordHeader,
    };
    use crate::txn::{GlobalTxnState, TxnLogOp, TXN_NONE};

    const TEST_URI: &str = "table:test";

    #[derive(Debug, Default)]
    struct RecordingRecoveryApplier {
        applied: Mutex<Vec<(u64, Vec<TxnLogOp>)>>,
        checkpoint_count: Mutex<usize>,
    }

    impl RecordingRecoveryApplier {
        fn applied(&self) -> Vec<(u64, Vec<TxnLogOp>)> {
            self.applied.lock().clone()
        }

        fn checkpoint_count(&self) -> usize {
            *self.checkpoint_count.lock()
        }
    }

    impl RecoveryApplier for RecordingRecoveryApplier {
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

    fn test_wal_record(index: u64, record: WalRecord) -> (WalRecordHeader, WalRecord) {
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

    fn recover_value_from_wal(base_path: &Path, store_name: &str, key: &[u8]) -> Option<Vec<u8>> {
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
        let reader = WalFileReader::open(base_path.join("global.wal")).unwrap();
        recover_from_wal(applier, reader).unwrap();

        let store = store_handles
            .get_or_try_insert_with(store_name.to_string(), |store_name| {
                let path = base_path.join(store_name);
                Ok(parking_lot::RwLock::new(open_or_create_btree(path)?))
            })
            .unwrap();
        let recovered = get_version(&mut store.write(), key, TXN_NONE).unwrap();
        recovered
    }

    #[test]
    fn test_recover_from_wal_replays_autocommit_writes() {
        let applier = Arc::new(RecordingRecoveryApplier::default());
        let reader = TestWalReader::new(vec![test_wal_record(
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
        let applier = Arc::new(RecordingRecoveryApplier::default());
        let reader = TestWalReader::new(vec![
            test_wal_record(
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
            test_wal_record(2, WalRecord::Checkpoint),
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
        let mut wal = LogFile::open_or_create(dir.path().join("global.wal")).unwrap();
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

        let wal_path = dir.path().join("global.wal");
        let len = std::fs::metadata(&wal_path).unwrap().len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(len.saturating_sub(8)).unwrap();

        let recovered = recover_value_from_wal(dir.path(), "items.main.wt", b"abort-me");
        assert_eq!(recovered, None);
    }

    #[test]
    fn test_recover_from_wal_replays_metadata_before_data_within_same_transaction() {
        let dir = tempdir().unwrap();
        let mut wal = LogFile::open_or_create(dir.path().join("global.wal")).unwrap();

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

        let conn = Connection::open(dir.path(), ConnectionConfig::new()).unwrap();

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
