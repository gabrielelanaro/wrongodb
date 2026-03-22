use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_store::MetadataStore;
use crate::storage::reserved_store::{reserved_store_name_for_id, StoreId, METADATA_STORE_ID};
use crate::storage::table::{checkpoint_store, open_or_create_btree};
use crate::storage::wal::{RecoveryError, WalReader, WalRecord};
use crate::txn::{GlobalTxnState, Transaction, TxnId, TxnLogOp};
use crate::WrongoDBError;

type StoreHandleCache = HandleCache<String, RwLock<BTreeCursor>>;
type StoreNameById = HashMap<StoreId, String>;

#[derive(Debug, PartialEq, Eq)]
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
    base_path: &Path,
    metadata_store: &MetadataStore,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    let transactions = read_committed_transactions(&mut reader)?;

    apply_metadata_transactions(base_path, store_handles, global_txn, &transactions)?;
    let store_names_by_id = rebuild_store_name_map(metadata_store)?;
    apply_non_metadata_transactions(
        base_path,
        store_handles,
        global_txn,
        &store_names_by_id,
        &transactions,
    )?;

    checkpoint_recovered_stores(store_handles, global_txn)?;
    Ok(())
}

fn read_committed_transactions(
    reader: &mut dyn WalReader,
) -> Result<Vec<CommittedTransaction>, WrongoDBError> {
    let mut transactions = Vec::new();
    while let Some(txn) = next_committed_transaction(reader)? {
        transactions.push(txn);
    }
    Ok(transactions)
}

fn apply_metadata_transactions(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    transactions: &[CommittedTransaction],
) -> Result<(), WrongoDBError> {
    for txn in transactions {
        apply_committed_transaction(
            base_path,
            store_handles,
            global_txn,
            &HashMap::new(),
            txn.txn_id,
            &txn.ops,
            ReplayPass::MetadataOnly,
        )?;
    }
    Ok(())
}

fn rebuild_store_name_map(metadata_store: &MetadataStore) -> Result<StoreNameById, WrongoDBError> {
    Ok(metadata_store
        .scan_prefix("")?
        .into_iter()
        .map(|entry| (entry.store_id(), entry.source().to_string()))
        .collect())
}

fn apply_non_metadata_transactions(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    store_names_by_id: &StoreNameById,
    transactions: &[CommittedTransaction],
) -> Result<(), WrongoDBError> {
    for txn in transactions {
        apply_committed_transaction(
            base_path,
            store_handles,
            global_txn,
            store_names_by_id,
            txn.txn_id,
            &txn.ops,
            ReplayPass::NonMetadata,
        )?;
    }
    Ok(())
}

fn apply_committed_transaction(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    store_names_by_id: &StoreNameById,
    txn_id: TxnId,
    ops: &[TxnLogOp],
    replay_pass: ReplayPass,
) -> Result<(), WrongoDBError> {
    let mut txn = Transaction::replay(txn_id);

    for op in ops {
        if !replay_pass.includes(op) {
            continue;
        }

        apply_replayed_operation(base_path, store_handles, store_names_by_id, &mut txn, op)?;
    }

    txn.commit(global_txn)?;
    Ok(())
}

fn checkpoint_recovered_stores(
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
) -> Result<(), WrongoDBError> {
    for store in store_handles.all_handles() {
        checkpoint_store(&mut store.write(), global_txn)?;
    }
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
            eprintln!("Stopping global WAL replay at corrupted tail: {err}");
            Ok(None)
        }
        Err(err) => Err(StorageError(format!("failed reading WAL during recovery: {err}")).into()),
    }
}

fn apply_replayed_operation(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    store_names_by_id: &StoreNameById,
    txn: &mut Transaction,
    op: &TxnLogOp,
) -> Result<(), WrongoDBError> {
    match op {
        TxnLogOp::Put {
            store_id,
            key,
            value,
        } => {
            let store =
                open_store_for_recovery_id(base_path, store_handles, store_names_by_id, *store_id)?;
            store.write().put(*store_id, key, value, txn)?;
        }
        TxnLogOp::Delete { store_id, key } => {
            let store =
                open_store_for_recovery_id(base_path, store_handles, store_names_by_id, *store_id)?;
            let _ = store.write().delete(*store_id, key, txn)?;
        }
    }

    Ok(())
}

fn open_store_for_recovery_id(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    store_names_by_id: &StoreNameById,
    store_id: StoreId,
) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
    let store_name = reserved_store_name_for_id(store_id)
        .map(str::to_string)
        .or_else(|| store_names_by_id.get(&store_id).cloned())
        .ok_or_else(|| StorageError(format!("unknown store id during recovery: {store_id}")))?;
    open_store_by_name(base_path, store_handles, &store_name)
}

fn open_store_by_name(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    store_name: &str,
) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
    store_handles.get_or_try_insert_with(store_name.to_string(), |store_name| {
        let path = base_path.join(store_name);
        Ok(RwLock::new(open_or_create_btree(path)?))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayPass {
    MetadataOnly,
    NonMetadata,
}

impl ReplayPass {
    fn includes(&self, op: &TxnLogOp) -> bool {
        let store_id = match op {
            TxnLogOp::Put { store_id, .. } | TxnLogOp::Delete { store_id, .. } => *store_id,
        };

        match self {
            Self::MetadataOnly => store_id == METADATA_STORE_ID,
            Self::NonMetadata => store_id != METADATA_STORE_ID,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;
    use std::sync::Arc;

    use serde::Serialize;
    use tempfile::tempdir;

    use crate::storage::api::{Connection, ConnectionConfig};
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::LogManager;
    use crate::storage::metadata_store::MetadataStore;
    use crate::storage::recovery::recover_from_wal;
    use crate::storage::reserved_store::{StoreId, FIRST_DYNAMIC_STORE_ID, METADATA_STORE_ID};
    use crate::storage::table::{get_version, open_or_create_btree};
    use crate::storage::wal::{
        LogFile, Lsn, RecoveryError, WalFileReader, WalReader, WalRecord, WalRecordHeader,
    };
    use crate::txn::{GlobalTxnState, TxnLogOp, TXN_NONE};

    const TEST_STORE_ID: StoreId = FIRST_DYNAMIC_STORE_ID;

    struct TestWalReader {
        records: VecDeque<(WalRecordHeader, WalRecord)>,
    }

    #[derive(Serialize)]
    struct EncodedMetadataRecord {
        source: String,
        store_id: StoreId,
        row_format: &'static str,
        key_columns: Vec<String>,
        value_columns: Vec<String>,
        columns: Vec<String>,
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

    fn encode_metadata_value(source: &str, store_id: StoreId) -> Vec<u8> {
        bson::to_vec(&EncodedMetadataRecord {
            source: source.to_string(),
            store_id,
            row_format: "wt_row_v1",
            key_columns: vec!["_id".to_string()],
            value_columns: Vec::new(),
            columns: Vec::new(),
        })
        .unwrap()
    }

    fn recover_value_from_wal(base_path: &Path, store_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let global_txn = Arc::new(GlobalTxnState::new());
        let log_manager = Arc::new(LogManager::disabled());
        let store_handles =
            Arc::new(HandleCache::<String, parking_lot::RwLock<BTreeCursor>>::new());
        let metadata_store = Arc::new(
            MetadataStore::new(
                base_path.to_path_buf(),
                store_handles.clone(),
                global_txn.clone(),
                log_manager,
            )
            .unwrap(),
        );
        let reader = WalFileReader::open(base_path.join("global.wal")).unwrap();
        recover_from_wal(
            base_path,
            metadata_store.as_ref(),
            store_handles.as_ref(),
            global_txn.as_ref(),
            reader,
        )
        .unwrap();

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
    fn test_next_committed_transaction_returns_commit_record() {
        let mut reader = TestWalReader::new(vec![test_wal_record(
            1,
            WalRecord::TxnCommit {
                txn_id: 7,
                commit_ts: 7,
                ops: vec![TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
        )]);

        assert_eq!(
            super::next_committed_transaction(&mut reader).unwrap(),
            Some(super::CommittedTransaction {
                txn_id: 7,
                ops: vec![TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            })
        );
    }

    #[test]
    fn test_next_committed_transaction_skips_checkpoint_records() {
        let mut reader = TestWalReader::new(vec![
            test_wal_record(1, WalRecord::Checkpoint),
            test_wal_record(
                2,
                WalRecord::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                    ops: vec![TxnLogOp::Put {
                        store_id: TEST_STORE_ID,
                        key: b"committed".to_vec(),
                        value: b"v1".to_vec(),
                    }],
                },
            ),
        ]);

        assert_eq!(
            super::next_committed_transaction(&mut reader).unwrap(),
            Some(super::CommittedTransaction {
                txn_id: 7,
                ops: vec![TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"committed".to_vec(),
                    value: b"v1".to_vec(),
                }],
            })
        );
    }

    #[test]
    fn test_recover_from_wal_skips_incomplete_write_from_real_wal_file() {
        let dir = tempdir().unwrap();
        let mut wal = LogFile::open_or_create(dir.path().join("global.wal")).unwrap();
        wal.log_txn_commit(
            7,
            7,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
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
                    store_id: METADATA_STORE_ID,
                    key: b"table:items".to_vec(),
                    value: encode_metadata_value("items.main.wt", FIRST_DYNAMIC_STORE_ID),
                },
                TxnLogOp::Put {
                    store_id: FIRST_DYNAMIC_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
            ],
        )
        .unwrap();
        wal.sync().unwrap();

        let conn = Connection::open(dir.path(), ConnectionConfig::new()).unwrap();

        assert_eq!(
            conn.metadata_store()
                .get("table:items")
                .unwrap()
                .map(|entry| entry.source().to_string()),
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
