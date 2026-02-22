use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::errors::StorageError;
use crate::engine::RaftMode;
use crate::raft::command::RaftCommand;
use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
use crate::raft::runtime::{RaftInboundMessage, RaftOutboundMessage, RaftRuntime};
use crate::recovery::txn_table::RecoveryTxnTable;
use crate::storage::table::Table;
use crate::storage::wal::{GlobalWal, RecoveryError, WalReader, WalRecord, WalSink};
use crate::txn::transaction_manager::TransactionManager;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftStatus {
    pub(crate) is_writable_primary: bool,
    pub(crate) leader_hint: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RecoveryManager {
    wal_enabled: bool,
    raft_runtime: Option<Arc<Mutex<RaftRuntime>>>,
    txn_manager: Arc<TransactionManager>,
}

impl RecoveryManager {
    pub fn initialize(
        base_path: &Path,
        wal_enabled: bool,
        txn_manager: Arc<TransactionManager>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        let mut manager = Self {
            wal_enabled,
            raft_runtime: None,
            txn_manager,
        };

        if manager.wal_enabled {
            warn_legacy_per_table_wal_files(base_path);
            manager.recover(base_path)?;
            let raft_cfg = raft_node_config_from_mode(raft_mode);
            let raft_node = RaftNodeCore::open_with_config(base_path, raft_cfg)?;
            manager.raft_runtime = Some(Arc::new(Mutex::new(RaftRuntime::new(raft_node))));
        }

        Ok(manager)
    }

    pub(crate) fn raft_status(&self) -> Option<RaftStatus> {
        self.raft_runtime.as_ref().map(|runtime| {
            let leadership = runtime.lock().leadership();
            RaftStatus {
                is_writable_primary: leadership.is_writable_primary(),
                leader_hint: leadership.leader_id,
            }
        })
    }

    #[allow(dead_code)]
    pub(crate) fn raft_tick(&self) -> Result<Vec<RaftOutboundMessage>, WrongoDBError> {
        let Some(runtime) = self.raft_runtime.as_ref() else {
            return Ok(Vec::new());
        };

        let mut runtime = runtime.lock();
        runtime.tick()?;
        Ok(runtime.drain_outbound())
    }

    #[allow(dead_code)]
    pub(crate) fn raft_handle_inbound(
        &self,
        msg: RaftInboundMessage,
    ) -> Result<Vec<RaftOutboundMessage>, WrongoDBError> {
        let Some(runtime) = self.raft_runtime.as_ref() else {
            return Ok(Vec::new());
        };

        let mut runtime = runtime.lock();
        runtime.handle_inbound(msg)?;
        Ok(runtime.drain_outbound())
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
        if let Some(runtime) = self.raft_runtime.as_ref() {
            let mut runtime = runtime.lock();
            runtime.ensure_writable_leader()?;
            runtime.propose(&RaftCommand::Put {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            })?;
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
        if let Some(runtime) = self.raft_runtime.as_ref() {
            let mut runtime = runtime.lock();
            runtime.ensure_writable_leader()?;
            runtime.propose(&RaftCommand::Delete {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                txn_id,
            })?;
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
        if let Some(runtime) = self.raft_runtime.as_ref() {
            let mut runtime = runtime.lock();
            runtime.ensure_writable_leader()?;
            runtime.propose(&RaftCommand::TxnCommit { txn_id, commit_ts })?;
            runtime.node_mut().sync()?;
        }
        Ok(())
    }

    pub fn log_txn_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(runtime) = self.raft_runtime.as_ref() {
            let mut runtime = runtime.lock();
            runtime.ensure_writable_leader()?;
            runtime.propose(&RaftCommand::TxnAbort { txn_id })?;
        }
        Ok(())
    }

    pub fn checkpoint_and_truncate_if_safe(&self, active_txns: bool) -> Result<(), WrongoDBError> {
        if !self.wal_enabled || active_txns {
            return Ok(());
        }

        if let Some(runtime) = self.raft_runtime.as_ref() {
            let mut runtime = runtime.lock();
            runtime.ensure_writable_leader()?;
            runtime.propose(&RaftCommand::Checkpoint)?;
            runtime.node_mut().sync()?;
            runtime.node_mut().truncate_to_checkpoint()?;
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

fn raft_node_config_from_mode(mode: RaftMode) -> RaftNodeConfig {
    match mode {
        RaftMode::Standalone => RaftNodeConfig::default(),
        RaftMode::Cluster {
            local_node_id,
            peer_ids,
        } => RaftNodeConfig {
            local_node_id,
            peer_ids,
            ..RaftNodeConfig::default()
        },
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
    use crate::raft::runtime::{RaftInboundMessage, RaftOutboundMessage};
    use crate::txn::GlobalTxnState;
    use crate::txn::TXN_NONE;

    fn new_txn_manager() -> Arc<TransactionManager> {
        Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())))
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let manager =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap();

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
        let manager =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap();

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

        let _manager =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap();

        assert!(raft_state_path.exists());
    }

    #[test]
    fn wal_disabled_startup_does_not_create_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _manager =
            RecoveryManager::initialize(dir.path(), false, new_txn_manager(), RaftMode::Standalone)
                .unwrap();

        assert!(!raft_state_path.exists());
    }

    #[test]
    fn corrupt_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _manager =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap();
        assert!(raft_state_path.exists());

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&raft_state_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        let err =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }

    #[test]
    fn standalone_mode_reports_writable_primary() {
        let dir = tempdir().unwrap();
        let manager =
            RecoveryManager::initialize(dir.path(), true, new_txn_manager(), RaftMode::Standalone)
                .unwrap();

        let status = manager.raft_status().unwrap();
        assert!(status.is_writable_primary);
        assert_eq!(status.leader_hint.as_deref(), Some("local"));
    }

    #[test]
    fn clustered_mode_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::initialize(
            dir.path(),
            true,
            new_txn_manager(),
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                peer_ids: vec!["n2".to_string()],
            },
        )
        .unwrap();

        let err = manager
            .log_put("users.main.wt", b"k1", b"v1", TXN_NONE)
            .unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn runtime_tick_and_inbound_surface_outbound_messages() {
        let dir = tempdir().unwrap();
        let manager = RecoveryManager::initialize(
            dir.path(),
            true,
            new_txn_manager(),
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                peer_ids: vec!["n2".to_string()],
            },
        )
        .unwrap();

        let mut outbound = Vec::new();
        for _ in 0..32 {
            outbound = manager.raft_tick().unwrap();
            if !outbound.is_empty() {
                break;
            }
        }
        assert_eq!(outbound.len(), 1);
        match &outbound[0] {
            RaftOutboundMessage::RequestVote { to, .. } => assert_eq!(to, "n2"),
            other => panic!("unexpected outbound message: {other:?}"),
        }

        let follow_up = manager
            .raft_handle_inbound(RaftInboundMessage::RequestVoteResponse {
                from: "n2".to_string(),
                resp: crate::raft::protocol::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                },
            })
            .unwrap();
        assert_eq!(follow_up.len(), 1);
        match &follow_up[0] {
            RaftOutboundMessage::AppendEntries { to, .. } => assert_eq!(to, "n2"),
            other => panic!("unexpected outbound message: {other:?}"),
        }
    }
}
