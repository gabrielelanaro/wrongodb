use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::history::HistoryStore;
use crate::storage::metadata_store::{MetadataStore, FILE_URI_PREFIX};
use crate::storage::reserved_store::{
    reserved_store_name_for_id, StoreId, METADATA_STORE_ID, METADATA_STORE_NAME,
};
use crate::storage::table::{checkpoint_store, open_or_create_btree, read_store_checkpoint_lsn};
use crate::storage::wal::{Lsn, RecoveryError, WalReader, WalRecord, WalRecordHeader};
use crate::txn::{GlobalTxnState, Transaction, TxnId, TxnLogOp};
use crate::WrongoDBError;

type StoreHandleCache = HandleCache<String, RwLock<BTreeCursor>>;
type StoreReplayStateById = HashMap<StoreId, StoreReplayState>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreReplayState {
    store_name: String,
    checkpoint_lsn: Lsn,
}

#[derive(Debug, PartialEq, Eq)]
struct CommittedTransaction {
    txn_id: TxnId,
    commit_lsn: Lsn,
    ops: Vec<TxnLogOp>,
}

/// Replays committed transactions from the global WAL into storage.
///
/// Recovery is intentionally split into two passes:
/// - replay metadata rows first so file-backed store ids resolve correctly
/// - replay non-metadata rows only for stores whose checkpoint LSN is older
///   than the transaction commit LSN
///
/// Recovery stops when it reaches a corrupted tail record. That treats a torn
/// final WAL write as end-of-log, which lets startup proceed after a crash that
/// interrupted the last append.
pub(in crate::storage) fn recover_from_wal(
    base_path: &Path,
    metadata_store: &MetadataStore,
    store_handles: &StoreHandleCache,
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    mut reader: impl WalReader,
) -> Result<(), WrongoDBError> {
    let transactions = collect_committed_transactions(&mut reader)?;
    let replay_end_lsn = reader.current_lsn();

    replay_committed_transactions(
        base_path,
        store_handles,
        global_txn,
        &StoreReplayStateById::new(),
        &transactions,
        ReplayStage::Metadata,
    )?;
    let store_states_by_id = load_store_replay_state(base_path, metadata_store)?;
    replay_committed_transactions(
        base_path,
        store_handles,
        global_txn,
        &store_states_by_id,
        &transactions,
        ReplayStage::DataStores,
    )?;

    checkpoint_replayed_state(
        base_path,
        metadata_store,
        store_handles,
        history_store,
        global_txn,
        replay_end_lsn,
    )?;
    Ok(())
}

fn collect_committed_transactions(
    reader: &mut dyn WalReader,
) -> Result<Vec<CommittedTransaction>, WrongoDBError> {
    let mut transactions = Vec::new();
    while let Some(txn) = next_committed_transaction(reader)? {
        transactions.push(txn);
    }
    Ok(transactions)
}

fn replay_committed_transactions(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    store_states_by_id: &StoreReplayStateById,
    transactions: &[CommittedTransaction],
    stage: ReplayStage,
) -> Result<(), WrongoDBError> {
    for txn in transactions {
        replay_committed_transaction(
            base_path,
            store_handles,
            global_txn,
            store_states_by_id,
            txn.txn_id,
            txn.commit_lsn,
            &txn.ops,
            stage,
        )?;
    }
    Ok(())
}

fn load_store_replay_state(
    base_path: &Path,
    metadata_store: &MetadataStore,
) -> Result<StoreReplayStateById, WrongoDBError> {
    metadata_store
        .scan_prefix(FILE_URI_PREFIX)?
        .into_iter()
        .map(|entry| {
            let store_name = entry.file_name()?.to_string();
            Ok((
                entry.store_id()?,
                StoreReplayState {
                    checkpoint_lsn: load_store_checkpoint_lsn(base_path, &store_name)?,
                    store_name,
                },
            ))
        })
        .collect::<Result<HashMap<_, _>, WrongoDBError>>()
}

fn replay_committed_transaction(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    global_txn: &GlobalTxnState,
    store_states_by_id: &StoreReplayStateById,
    txn_id: TxnId,
    commit_lsn: Lsn,
    ops: &[TxnLogOp],
    stage: ReplayStage,
) -> Result<(), WrongoDBError> {
    let mut txn = Transaction::replay(txn_id);

    for op in ops {
        if !stage.includes(op) {
            continue;
        }

        if stage == ReplayStage::DataStores
            && !store_needs_replay(store_states_by_id, commit_lsn, op)
        {
            continue;
        }

        apply_replayed_op(base_path, store_handles, store_states_by_id, &mut txn, op)?;
    }

    txn.commit(global_txn)?;
    Ok(())
}

fn checkpoint_replayed_state(
    base_path: &Path,
    metadata_store: &MetadataStore,
    store_handles: &StoreHandleCache,
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    checkpoint_lsn: Lsn,
) -> Result<(), WrongoDBError> {
    global_txn.begin_checkpoint();

    let checkpoint_result = checkpoint_replayed_store_files(
        base_path,
        metadata_store,
        store_handles,
        history_store,
        global_txn,
        checkpoint_lsn,
    );

    global_txn.end_checkpoint();
    checkpoint_result
}

fn checkpoint_replayed_store_files(
    base_path: &Path,
    metadata_store: &MetadataStore,
    store_handles: &StoreHandleCache,
    history_store: &mut HistoryStore,
    global_txn: &GlobalTxnState,
    checkpoint_lsn: Lsn,
) -> Result<(), WrongoDBError> {
    // `all_stores_with_id` returns file-backed user stores. `history.wt` and
    // `metadata.wt` are checkpointed explicitly below so the replay anchor is
    // written last into `metadata.wt`.
    for (store_id, store_name) in metadata_store.all_stores_with_id()? {
        checkpoint_store(
            &mut open_store_by_name(base_path, store_handles, &store_name)?.write(),
            global_txn,
            store_id,
            Some(&mut *history_store),
            checkpoint_lsn,
        )?;
    }

    history_store.checkpoint(global_txn, checkpoint_lsn)?;
    checkpoint_store(
        &mut open_store_by_name(base_path, store_handles, METADATA_STORE_NAME)?.write(),
        global_txn,
        METADATA_STORE_ID,
        None,
        checkpoint_lsn,
    )
}

fn next_committed_transaction(
    reader: &mut dyn WalReader,
) -> Result<Option<CommittedTransaction>, WrongoDBError> {
    loop {
        let Some((header, record)) = read_next_record_or_stop_at_corrupt_tail(reader)? else {
            return Ok(None);
        };

        match record {
            WalRecord::TxnCommit { txn_id, ops, .. } => {
                return Ok(Some(CommittedTransaction {
                    txn_id,
                    commit_lsn: header.lsn,
                    ops,
                }));
            }
        }
    }
}

fn read_next_record_or_stop_at_corrupt_tail(
    reader: &mut dyn WalReader,
) -> Result<Option<(WalRecordHeader, WalRecord)>, WrongoDBError> {
    match reader.read_record() {
        Ok(Some(record)) => Ok(Some(record)),
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

fn apply_replayed_op(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    store_states_by_id: &StoreReplayStateById,
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
                open_store_for_replay(base_path, store_handles, store_states_by_id, *store_id)?;
            store.write().put(*store_id, key, value, txn)?;
        }
        TxnLogOp::Delete { store_id, key } => {
            let store =
                open_store_for_replay(base_path, store_handles, store_states_by_id, *store_id)?;
            let _ = store.write().delete(*store_id, key, txn)?;
        }
    }

    Ok(())
}

fn open_store_for_replay(
    base_path: &Path,
    store_handles: &StoreHandleCache,
    store_states_by_id: &StoreReplayStateById,
    store_id: StoreId,
) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
    let store_name = reserved_store_name_for_id(store_id)
        .map(str::to_string)
        .or_else(|| {
            store_states_by_id
                .get(&store_id)
                .map(|state| state.store_name.clone())
        })
        .ok_or_else(|| StorageError(format!("unknown store id during recovery: {store_id}")))?;
    open_store_by_name(base_path, store_handles, &store_name)
}

fn load_store_checkpoint_lsn(base_path: &Path, store_name: &str) -> Result<Lsn, WrongoDBError> {
    let store_path = base_path.join(store_name);
    if !store_path.exists() {
        return Ok(Lsn::new(0, 0));
    }

    read_store_checkpoint_lsn(store_path)
}

fn store_needs_replay(
    store_states_by_id: &StoreReplayStateById,
    commit_lsn: Lsn,
    op: &TxnLogOp,
) -> bool {
    // Newly created stores may not exist on disk yet, so "missing state" means
    // "replay from the start".
    let checkpoint_lsn = store_states_by_id
        .get(&store_id_for(op))
        .map(|state| state.checkpoint_lsn)
        .unwrap_or_else(|| Lsn::new(0, 0));
    commit_lsn >= checkpoint_lsn
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
enum ReplayStage {
    Metadata,
    DataStores,
}

impl ReplayStage {
    fn includes(&self, op: &TxnLogOp) -> bool {
        match self {
            Self::Metadata => store_id_for(op) == METADATA_STORE_ID,
            Self::DataStores => store_id_for(op) != METADATA_STORE_ID,
        }
    }
}

fn store_id_for(op: &TxnLogOp) -> StoreId {
    match op {
        TxnLogOp::Put { store_id, .. } | TxnLogOp::Delete { store_id, .. } => *store_id,
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
    use crate::storage::history::HistoryStore;
    use crate::storage::log_manager::LogManager;
    use crate::storage::metadata_store::MetadataStore;
    use crate::storage::recovery::recover_from_wal;
    use crate::storage::reserved_store::{
        StoreId, FIRST_DYNAMIC_STORE_ID, HS_STORE_NAME, METADATA_STORE_ID,
    };
    use crate::storage::table::{get_version, open_or_create_btree};
    use crate::storage::wal::{
        LogFile, Lsn, RecoveryError, WalFileReader, WalReader, WalRecord, WalRecordHeader,
    };
    use crate::txn::{GlobalTxnState, TxnLogOp, TXN_NONE};

    const TEST_STORE_ID: StoreId = FIRST_DYNAMIC_STORE_ID;

    struct TestWalReader {
        records: VecDeque<(WalRecordHeader, WalRecord)>,
        current_lsn: Lsn,
    }

    #[derive(Serialize)]
    struct EncodedMetadataRecord {
        source: Option<String>,
        store_id: Option<StoreId>,
        row_format: Option<&'static str>,
        key_columns: Vec<String>,
        value_columns: Vec<String>,
        columns: Vec<String>,
    }

    impl TestWalReader {
        fn new(records: Vec<(WalRecordHeader, WalRecord)>) -> Self {
            Self {
                records: records.into(),
                current_lsn: Lsn::new(0, 0),
            }
        }
    }

    impl WalReader for TestWalReader {
        fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError> {
            let record = self.records.pop_front();
            if let Some((header, _)) = &record {
                self.current_lsn = header.lsn;
            }
            Ok(record)
        }

        fn current_lsn(&self) -> Lsn {
            self.current_lsn
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

    fn encode_file_metadata_value(store_id: StoreId) -> Vec<u8> {
        bson::to_vec(&EncodedMetadataRecord {
            source: None,
            store_id: Some(store_id),
            row_format: None,
            key_columns: Vec::new(),
            value_columns: Vec::new(),
            columns: Vec::new(),
        })
        .unwrap()
    }

    fn encode_table_metadata_value(source: &str) -> Vec<u8> {
        bson::to_vec(&EncodedMetadataRecord {
            source: Some(source.to_string()),
            store_id: None,
            row_format: Some("wt_row_v1"),
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
        let mut history_store =
            HistoryStore::open_or_create(base_path.join(HS_STORE_NAME)).unwrap();
        let reader = WalFileReader::open(base_path.join("global.wal")).unwrap();
        recover_from_wal(
            base_path,
            metadata_store.as_ref(),
            store_handles.as_ref(),
            &mut history_store,
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
                commit_lsn: Lsn::new(0, 1),
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
    fn test_next_committed_transaction_returns_none_at_eof() {
        let mut reader = TestWalReader::new(Vec::new());

        assert_eq!(
            super::next_committed_transaction(&mut reader).unwrap(),
            None
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
                    key: b"file:items.main.wt".to_vec(),
                    value: encode_file_metadata_value(FIRST_DYNAMIC_STORE_ID),
                },
                TxnLogOp::Put {
                    store_id: METADATA_STORE_ID,
                    key: b"table:items".to_vec(),
                    value: encode_table_metadata_value("file:items.main.wt"),
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
                .map(|entry| entry.source_uri().unwrap().to_string()),
            Some("file:items.main.wt".to_string())
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
