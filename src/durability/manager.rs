use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Weak};

use crate::api::connection::{RaftMode, RaftPeerConfig};
use crate::core::errors::StorageError;
use crate::durability::StoreCommandApplier;
use crate::raft::command::RaftCommand;
use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
use crate::raft::service::{RaftServiceConfig, RaftServiceHandle};
use crate::recovery::manager::warn_legacy_per_table_wal_files;
use crate::storage::wal::WalSink;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[cfg(test)]
use crate::storage::store_registry::StoreRegistry;
#[cfg(test)]
use crate::txn::transaction_manager::TransactionManager;
#[cfg(test)]
use crate::txn::GlobalTxnState;
#[cfg(test)]
use crate::txn::TXN_NONE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftStatus {
    pub(crate) is_writable_primary: bool,
    pub(crate) leader_hint: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DurabilityManager {
    wal_enabled: bool,
    raft_service: Option<RaftServiceHandle>,
}

#[derive(Debug)]
struct DurabilityWalSink {
    manager: Weak<DurabilityManager>,
}

impl WalSink for DurabilityWalSink {
    fn log_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("durability manager is not available".into()))?;
        manager.log_put(store_name, key, value, txn_id)
    }

    fn log_delete(&self, store_name: &str, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| StorageError("durability manager is not available".into()))?;
        manager.log_delete(store_name, key, txn_id)
    }
}

impl DurabilityManager {
    pub(crate) fn initialize(
        base_path: &Path,
        wal_enabled: bool,
        applier: Arc<StoreCommandApplier>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        let mut manager = Self {
            wal_enabled,
            raft_service: None,
        };

        if manager.wal_enabled {
            warn_legacy_per_table_wal_files(base_path);
            let (raft_cfg, mut service_cfg) = raft_configs_from_mode(raft_mode)?;
            service_cfg.executor = applier;
            let raft_node = RaftNodeCore::open_with_config(base_path, raft_cfg)?;
            manager.raft_service = Some(RaftServiceHandle::start(raft_node, service_cfg)?);
        }

        Ok(manager)
    }

    pub(crate) fn wal_sink(self: &Arc<Self>) -> Arc<dyn WalSink> {
        Arc::new(DurabilityWalSink {
            manager: Arc::downgrade(self),
        })
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

    pub(crate) fn log_put(
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

    pub(crate) fn log_delete(
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

    pub(crate) fn log_txn_commit_sync(
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

    pub(crate) fn log_txn_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(service) = self.raft_service.as_ref() {
            service.propose(RaftCommand::TxnAbort { txn_id })?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn checkpoint_and_truncate_if_safe(
        &self,
        active_txns: bool,
    ) -> Result<(), WrongoDBError> {
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
    use crate::storage::wal::{GlobalWal, WalReader, WalRecord};

    fn new_txn_manager() -> Arc<TransactionManager> {
        Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())))
    }

    fn new_store_registry(
        dir: &tempfile::TempDir,
        txn_manager: Arc<TransactionManager>,
    ) -> Arc<StoreRegistry> {
        Arc::new(StoreRegistry::new(dir.path().to_path_buf(), txn_manager))
    }

    fn new_durability_manager(
        dir: &tempfile::TempDir,
        wal_enabled: bool,
        raft_mode: RaftMode,
    ) -> DurabilityManager {
        let txn_manager = new_txn_manager();
        let registry = new_store_registry(dir, txn_manager);
        let applier = Arc::new(StoreCommandApplier::new(registry));
        DurabilityManager::initialize(dir.path(), wal_enabled, applier, raft_mode).unwrap()
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let manager = new_durability_manager(&dir, true, RaftMode::Standalone);

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
        let manager = new_durability_manager(&dir, true, RaftMode::Standalone);

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

        let _manager = new_durability_manager(&dir, true, RaftMode::Standalone);

        assert!(raft_state_path.exists());
    }

    #[test]
    fn wal_disabled_startup_does_not_create_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _manager = new_durability_manager(&dir, false, RaftMode::Standalone);

        assert!(!raft_state_path.exists());
    }

    #[test]
    fn corrupt_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _manager = new_durability_manager(&dir, true, RaftMode::Standalone);
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
        let registry = new_store_registry(&dir, txn_manager);
        let applier = Arc::new(StoreCommandApplier::new(registry));
        let err = DurabilityManager::initialize(dir.path(), true, applier, RaftMode::Standalone)
            .unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }

    #[test]
    fn standalone_mode_reports_writable_primary() {
        let dir = tempdir().unwrap();
        let manager = new_durability_manager(&dir, true, RaftMode::Standalone);

        let status = manager.raft_status().unwrap();
        assert!(status.is_writable_primary);
        assert_eq!(status.leader_hint.as_deref(), Some("local"));
    }

    #[test]
    fn clustered_mode_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let manager = new_durability_manager(
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
        let manager = new_durability_manager(
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
