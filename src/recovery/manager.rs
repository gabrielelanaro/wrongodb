use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::errors::StorageError;
use crate::raft::node::RaftNodeCore;
use crate::recovery::txn_table::RecoveryTxnTable;
use crate::storage::table::Table;
use crate::storage::wal::{GlobalWal, RecoveryError, WalReader, WalRecord, WalSink};
use crate::txn::transaction_manager::TransactionManager;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Debug)]
pub(crate) struct RecoveryManager {
    wal_enabled: bool,
    raft_node: Option<Arc<Mutex<RaftNodeCore>>>,
    txn_manager: Arc<TransactionManager>,
}

impl RecoveryManager {
    pub fn initialize(
        base_path: &Path,
        wal_enabled: bool,
        txn_manager: Arc<TransactionManager>,
    ) -> Result<Self, WrongoDBError> {
        let mut manager = Self {
            wal_enabled,
            raft_node: None,
            txn_manager,
        };

        if manager.wal_enabled {
            warn_legacy_per_table_wal_files(base_path);
            manager.recover(base_path)?;
            manager.raft_node = Some(Arc::new(Mutex::new(RaftNodeCore::open(base_path)?)));
        }

        Ok(manager)
    }

    pub fn log_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(raft_node) = self.raft_node.as_ref() {
            raft_node.lock().log_put(store_name, key, value, txn_id)?;
        }
        Ok(())
    }

    pub fn log_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(raft_node) = self.raft_node.as_ref() {
            raft_node.lock().log_delete(store_name, key, txn_id)?;
        }
        Ok(())
    }

    pub fn log_txn_commit_sync(
        &self,
        txn_id: TxnId,
        commit_ts: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(raft_node) = self.raft_node.as_ref() {
            let mut raft_node = raft_node.lock();
            raft_node.log_txn_commit(txn_id, commit_ts)?;
            raft_node.sync()?;
        }
        Ok(())
    }

    pub fn log_txn_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(raft_node) = self.raft_node.as_ref() {
            raft_node.lock().log_txn_abort(txn_id)?;
        }
        Ok(())
    }

    pub fn checkpoint_and_truncate_if_safe(&self, active_txns: bool) -> Result<(), WrongoDBError> {
        if !self.wal_enabled || active_txns {
            return Ok(());
        }

        if let Some(raft_node) = self.raft_node.as_ref() {
            let mut raft_node = raft_node.lock();
            let checkpoint_lsn = raft_node.log_checkpoint()?;
            raft_node.set_checkpoint_lsn(checkpoint_lsn)?;
            raft_node.sync()?;
            raft_node.truncate_to_checkpoint()?;
        }

        Ok(())
    }

    pub fn recover(&self, base_path: &Path) -> Result<(), WrongoDBError> {
        let wal_path = GlobalWal::path_for_db(base_path);
        if !wal_path.exists() {
            return Ok(());
        }

        let mut first_pass_reader = match WalReader::open(&wal_path) {
            Ok(reader) => reader,
            Err(err) => {
                eprintln!("Skipping global WAL recovery (failed to open WAL): {err}");
                return Ok(());
            }
        };

        let mut txn_table = RecoveryTxnTable::new();
        while let Some(record) = next_recovery_record(&mut first_pass_reader, "pass 1")? {
            txn_table.process_record(&record);
        }
        txn_table.finalize_pending();

        let mut second_pass_reader = match WalReader::open(&wal_path) {
            Ok(reader) => reader,
            Err(err) => {
                eprintln!("Skipping global WAL recovery (failed to reopen WAL): {err}");
                return Ok(());
            }
        };

        let mut replay_tables: HashMap<String, Table> = HashMap::new();

        while let Some(record) = next_recovery_record(&mut second_pass_reader, "pass 2")? {
            if !txn_table.should_apply(&record) {
                continue;
            }

            match record {
                WalRecord::Put {
                    store_name,
                    key,
                    value,
                    ..
                } => {
                    ensure_replay_table(
                        &mut replay_tables,
                        base_path,
                        &store_name,
                        self.txn_manager.clone(),
                    )?
                    .put_recovery(&key, &value)?;
                }
                WalRecord::Delete {
                    store_name, key, ..
                } => {
                    ensure_replay_table(
                        &mut replay_tables,
                        base_path,
                        &store_name,
                        self.txn_manager.clone(),
                    )?
                    .delete_recovery(&key)?;
                }
                WalRecord::Checkpoint
                | WalRecord::TxnCommit { .. }
                | WalRecord::TxnAbort { .. } => {}
            }
        }

        for table in replay_tables.values_mut() {
            table.checkpoint()?;
        }

        Ok(())
    }
}

fn warn_legacy_per_table_wal_files(base_path: &Path) {
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
            eprintln!("Stopping global WAL replay during {pass} at corrupted tail: {err}");
            Ok(None)
        }
        Err(err) => Err(StorageError(format!("failed reading WAL during {pass}: {err}")).into()),
    }
}

fn ensure_replay_table<'a>(
    replay_tables: &'a mut HashMap<String, Table>,
    base_path: &Path,
    store_name: &str,
    txn_manager: Arc<TransactionManager>,
) -> Result<&'a mut Table, WrongoDBError> {
    if !replay_tables.contains_key(store_name) {
        let store_path = base_path.join(store_name);
        let table = Table::open_or_create_index(&store_path, txn_manager, None)?;
        replay_tables.insert(store_name.to_string(), table);
    }

    replay_tables
        .get_mut(store_name)
        .ok_or_else(|| StorageError(format!("replay table missing for store {store_name}")).into())
}

impl WalSink for RecoveryManager {
    fn log_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        RecoveryManager::log_put(self, store_name, key, value, txn_id)
    }

    fn log_delete(&self, store_name: &str, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        RecoveryManager::log_delete(self, store_name, key, txn_id)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::raft::hard_state::RaftHardStateStore;
    use crate::txn::GlobalTxnState;

    fn new_txn_manager() -> Arc<TransactionManager> {
        Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())))
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::initialize(dir.path(), true, new_txn_manager()).unwrap();

        manager.log_txn_commit_sync(7, 7).unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let mut reader = WalReader::open(wal_path).unwrap();
        let mut found_commit = false;
        while let Some((_header, record)) = reader.read_record().unwrap() {
            if matches!(
                record,
                WalRecord::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7
                }
            ) {
                found_commit = true;
            }
        }
        assert!(found_commit);
    }

    #[test]
    fn checkpoint_skip_and_truncate_follow_active_txn_flag() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::initialize(dir.path(), true, new_txn_manager()).unwrap();

        manager.log_txn_commit_sync(1, 1).unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        manager.checkpoint_and_truncate_if_safe(true).unwrap();
        let after_skip = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(after_skip, before);

        manager.checkpoint_and_truncate_if_safe(false).unwrap();
        let after_truncate = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after_truncate <= 512);
    }

    #[test]
    fn wal_enabled_startup_creates_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _manager = RecoveryManager::initialize(dir.path(), true, new_txn_manager()).unwrap();

        assert!(raft_state_path.exists());
    }

    #[test]
    fn wal_disabled_startup_does_not_create_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _manager = RecoveryManager::initialize(dir.path(), false, new_txn_manager()).unwrap();

        assert!(!raft_state_path.exists());
    }

    #[test]
    fn corrupt_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _manager = RecoveryManager::initialize(dir.path(), true, new_txn_manager()).unwrap();
        assert!(raft_state_path.exists());

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&raft_state_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        let err = RecoveryManager::initialize(dir.path(), true, new_txn_manager()).unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }
}
