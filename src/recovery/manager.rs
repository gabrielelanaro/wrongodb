use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::api::connection::{RaftMode, RaftPeerConfig};
use crate::core::errors::StorageError;
use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
use crate::raft::service::{CommittedCommandExecutor, RaftServiceConfig, RaftServiceHandle};
use crate::storage::store_registry::StoreRegistry;
use crate::storage::wal::{
    GlobalWal, RecoveryError, WalReader, WalRecord, WalRecordHeader, WalSink,
};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

#[cfg(test)]
use crate::txn::transaction_manager::TransactionManager;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftStatus {
    pub(crate) is_writable_primary: bool,
    pub(crate) leader_hint: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RecoveryManager {
    wal_enabled: bool,
    raft_service: Option<RaftServiceHandle>,
    executor: Arc<LiveCommittedCommandExecutor>,
}

#[derive(Debug)]
pub(crate) struct LiveCommittedCommandExecutor {
    store_registry: Arc<StoreRegistry>,
    touched_stores: Mutex<HashMap<TxnId, HashSet<String>>>,
}

impl LiveCommittedCommandExecutor {
    pub(crate) fn new(store_registry: Arc<StoreRegistry>) -> Self {
        Self {
            store_registry,
            touched_stores: Mutex::new(HashMap::new()),
        }
    }
}

impl CommittedCommandExecutor for LiveCommittedCommandExecutor {
    fn execute(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        match cmd.command {
            RaftCommand::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                let table = self.store_registry.resolve_or_open_store(&store_name)?;
                table
                    .write()
                    .local_apply_put_with_txn(&key, &value, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            RaftCommand::Delete {
                store_name,
                key,
                txn_id,
            } => {
                let table = self.store_registry.resolve_or_open_store(&store_name)?;
                let _ = table.write().local_apply_delete_with_txn(&key, txn_id)?;
                if txn_id != TXN_NONE {
                    let mut touched = self.touched_stores.lock();
                    touched.entry(txn_id).or_default().insert(store_name);
                }
            }
            RaftCommand::TxnCommit { txn_id, .. } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.store_registry.resolve_or_open_store(&store_name)?;
                    table.write().local_mark_updates_committed(txn_id)?;
                }
            }
            RaftCommand::TxnAbort { txn_id } => {
                if txn_id == TXN_NONE {
                    return Ok(());
                }
                let touched = self
                    .touched_stores
                    .lock()
                    .remove(&txn_id)
                    .unwrap_or_default();
                for store_name in touched {
                    let table = self.store_registry.resolve_or_open_store(&store_name)?;
                    table.write().local_mark_updates_aborted(txn_id)?;
                }
            }
            RaftCommand::Checkpoint => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
enum RecoveryAction {
    ApplyChange(CommittedCommand),
    StageTransactionChange {
        txn_id: TxnId,
        change: BufferedRecoveryChange,
    },
    ApplyBufferedTransaction {
        txn_id: TxnId,
        commit: CommittedCommand,
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
    command: RaftCommand,
}

impl BufferedRecoveryChange {
    fn into_committed_command(self) -> CommittedCommand {
        CommittedCommand {
            index: self.index,
            term: self.term,
            command: self.command,
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
    pub fn initialize(
        base_path: &Path,
        wal_enabled: bool,
        store_registry: Arc<StoreRegistry>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        let executor = Arc::new(LiveCommittedCommandExecutor::new(store_registry));
        let mut manager = Self {
            wal_enabled,
            raft_service: None,
            executor,
        };

        if manager.wal_enabled {
            warn_legacy_per_table_wal_files(base_path);
            manager.recover(base_path)?;
            let (raft_cfg, mut service_cfg) = raft_configs_from_mode(raft_mode)?;
            service_cfg.executor = manager.executor.clone();
            let raft_node = RaftNodeCore::open_with_config(base_path, raft_cfg)?;
            manager.raft_service = Some(RaftServiceHandle::start(raft_node, service_cfg)?);
        }

        Ok(manager)
    }

    pub(crate) fn raft_status(&self) -> Option<RaftStatus> {
        self.raft_service.as_ref().and_then(|service| {
            service.leadership().ok().map(|leadership| RaftStatus {
                is_writable_primary: leadership.is_writable_primary(),
                leader_hint: leadership.leader_id,
            })
        })
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    #[allow(dead_code)]
    pub(crate) fn raft_tick(&self) -> Result<(), WrongoDBError> {
        let Some(service) = self.raft_service.as_ref() else {
            return Ok(());
        };

        service.force_tick()
    }

    #[allow(dead_code)]
    pub(crate) fn raft_handle_inbound(
        &self,
        msg: crate::raft::runtime::RaftInboundMessage,
    ) -> Result<(), WrongoDBError> {
        let Some(service) = self.raft_service.as_ref() else {
            return Ok(());
        };

        service.inject_inbound(msg)
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
        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::Put {
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
        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::Delete {
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
        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::TxnCommit { txn_id, commit_ts })?;
            service.sync()?;
        }
        Ok(())
    }

    pub fn log_txn_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::TxnAbort { txn_id })?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn checkpoint_and_truncate_if_safe(&self, active_txns: bool) -> Result<(), WrongoDBError> {
        if !self.wal_enabled || active_txns {
            return Ok(());
        }

        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::Checkpoint)?;
            service.sync()?;
            service.truncate_to_checkpoint()?;
        }

        Ok(())
    }

    pub fn recover(&self, base_path: &Path) -> Result<(), WrongoDBError> {
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

    fn apply_change(&self, command: CommittedCommand) -> Result<(), WrongoDBError> {
        self.executor.execute(command)
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
        commit: CommittedCommand,
    ) -> Result<(), WrongoDBError> {
        for change in staged_changes.release(txn_id) {
            self.apply_change(change.into_committed_command())?;
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
        for table in self.executor.store_registry.all_handles() {
            table.write().checkpoint()?;
        }
        Ok(())
    }
}

fn raft_configs_from_mode(
    mode: RaftMode,
) -> Result<(RaftNodeConfig, RaftServiceConfig), WrongoDBError> {
    match mode {
        RaftMode::Standalone => {
            let node_cfg = RaftNodeConfig::default();
            let service_cfg = RaftServiceConfig::with_defaults(
                node_cfg.local_node_id.clone(),
                None,
                HashMap::new(),
            );
            Ok((node_cfg, service_cfg))
        }
        RaftMode::Cluster {
            local_node_id,
            local_raft_addr,
            peers,
        } => {
            validate_cluster_mode(&local_node_id, &local_raft_addr, &peers)?;

            let peer_ids = peers.iter().map(|peer| peer.node_id.clone()).collect();
            let peer_addrs = peers
                .into_iter()
                .map(|peer| (peer.node_id, peer.raft_addr))
                .collect::<HashMap<_, _>>();
            let node_cfg = RaftNodeConfig {
                local_node_id: local_node_id.clone(),
                peer_ids,
                ..RaftNodeConfig::default()
            };
            let service_cfg =
                RaftServiceConfig::with_defaults(local_node_id, Some(local_raft_addr), peer_addrs);
            Ok((node_cfg, service_cfg))
        }
    }
}

fn validate_cluster_mode(
    local_node_id: &str,
    local_raft_addr: &str,
    peers: &[RaftPeerConfig],
) -> Result<(), WrongoDBError> {
    if local_node_id.is_empty() {
        return Err(StorageError("raft local_node_id cannot be empty".into()).into());
    }

    if local_raft_addr.is_empty() {
        return Err(StorageError("raft local_raft_addr cannot be empty".into()).into());
    }
    parse_socket_addr(local_raft_addr, "local_raft_addr")?;

    let mut peer_ids = std::collections::HashSet::new();
    let mut peer_addrs = std::collections::HashSet::new();
    for peer in peers {
        if peer.node_id.is_empty() {
            return Err(StorageError("raft peer node_id cannot be empty".into()).into());
        }
        if peer.node_id == local_node_id {
            return Err(StorageError("raft peers cannot include local_node_id".into()).into());
        }
        if !peer_ids.insert(peer.node_id.clone()) {
            return Err(
                StorageError(format!("duplicate raft peer node_id: {}", peer.node_id)).into(),
            );
        }

        if peer.raft_addr.is_empty() {
            return Err(StorageError("raft peer raft_addr cannot be empty".into()).into());
        }
        parse_socket_addr(&peer.raft_addr, "peer.raft_addr")?;
        if !peer_addrs.insert(peer.raft_addr.clone()) {
            return Err(
                StorageError(format!("duplicate raft peer raft_addr: {}", peer.raft_addr)).into(),
            );
        }
    }

    Ok(())
}

fn parse_socket_addr(addr: &str, label: &str) -> Result<SocketAddr, WrongoDBError> {
    addr.parse()
        .map_err(|e| StorageError(format!("invalid raft {label} '{addr}': {e}")).into())
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
        } if txn_id == TXN_NONE => RecoveryAction::ApplyChange(CommittedCommand {
            index: header.raft_index,
            term: header.raft_term,
            command: RaftCommand::Put {
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
                command: RaftCommand::Put {
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
        } if txn_id == TXN_NONE => RecoveryAction::ApplyChange(CommittedCommand {
            index: header.raft_index,
            term: header.raft_term,
            command: RaftCommand::Delete {
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
                command: RaftCommand::Delete {
                    store_name,
                    key,
                    txn_id,
                },
            },
        },
        WalRecord::TxnCommit { txn_id, commit_ts } => RecoveryAction::ApplyBufferedTransaction {
            txn_id,
            commit: CommittedCommand {
                index: header.raft_index,
                term: header.raft_term,
                command: RaftCommand::TxnCommit { txn_id, commit_ts },
            },
        },
        WalRecord::TxnAbort { txn_id } => RecoveryAction::DiscardBufferedTransaction { txn_id },
        WalRecord::Checkpoint => RecoveryAction::Ignore,
    }
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
    use std::net::TcpListener;
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::raft::hard_state::RaftHardStateStore;
    use crate::raft::runtime::RaftInboundMessage;
    use crate::txn::GlobalTxnState;
    use crate::txn::TXN_NONE;

    fn new_txn_manager() -> Arc<TransactionManager> {
        Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())))
    }

    fn new_store_registry(
        dir: &tempfile::TempDir,
        txn_manager: Arc<TransactionManager>,
    ) -> Arc<StoreRegistry> {
        Arc::new(StoreRegistry::new(dir.path().to_path_buf(), txn_manager))
    }

    fn new_manager(
        dir: &tempfile::TempDir,
        wal_enabled: bool,
        raft_mode: RaftMode,
    ) -> RecoveryManager {
        let txn_manager = new_txn_manager();
        let registry = new_store_registry(dir, txn_manager.clone());
        RecoveryManager::initialize(dir.path(), wal_enabled, registry, raft_mode).unwrap()
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    fn buffered_put_change(txn_id: TxnId, key: &[u8], value: &[u8]) -> BufferedRecoveryChange {
        BufferedRecoveryChange {
            index: txn_id,
            term: 1,
            command: RaftCommand::Put {
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

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let manager = new_manager(&dir, true, RaftMode::Standalone);

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
        let manager = new_manager(&dir, true, RaftMode::Standalone);

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

        let _manager = new_manager(&dir, true, RaftMode::Standalone);

        assert!(raft_state_path.exists());
    }

    #[test]
    fn wal_disabled_startup_does_not_create_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _manager = new_manager(&dir, false, RaftMode::Standalone);

        assert!(!raft_state_path.exists());
    }

    #[test]
    fn corrupt_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _manager = new_manager(&dir, true, RaftMode::Standalone);
        assert!(raft_state_path.exists());

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&raft_state_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        let txn_manager = new_txn_manager();
        let registry = new_store_registry(&dir, txn_manager.clone());
        let err = RecoveryManager::initialize(dir.path(), true, registry, RaftMode::Standalone)
            .unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }

    #[test]
    fn standalone_mode_reports_writable_primary() {
        let dir = tempdir().unwrap();
        let manager = new_manager(&dir, true, RaftMode::Standalone);

        let status = manager.raft_status().unwrap();
        assert!(status.is_writable_primary);
        assert_eq!(status.leader_hint.as_deref(), Some("local"));
    }

    #[test]
    fn clustered_mode_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let manager = new_manager(
            &dir,
            true,
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                local_raft_addr: free_local_addr(),
                peers: vec![RaftPeerConfig {
                    node_id: "n2".to_string(),
                    raft_addr: "127.0.0.1:65001".to_string(),
                }],
            },
        );

        let err = manager
            .log_put("users.main.wt", b"k1", b"v1", TXN_NONE)
            .unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn runtime_tick_and_inbound_helpers_remain_callable() {
        let dir = tempdir().unwrap();
        let manager = new_manager(
            &dir,
            true,
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                local_raft_addr: free_local_addr(),
                peers: vec![RaftPeerConfig {
                    node_id: "n2".to_string(),
                    raft_addr: "127.0.0.1:65002".to_string(),
                }],
            },
        );

        manager.raft_tick().unwrap();

        manager
            .raft_handle_inbound(RaftInboundMessage::RequestVoteResponse {
                from: "n2".to_string(),
                resp: crate::raft::protocol::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                },
            })
            .unwrap();
    }
}
