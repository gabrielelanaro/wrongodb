use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Weak};

use crate::api::connection::{RaftMode, RaftPeerConfig};
use crate::core::errors::StorageError;
use crate::durability::{DurableOp, StoreCommandApplier};
use crate::hooks::MutationHooks;
use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
use crate::raft::service::{RaftServiceConfig, RaftServiceHandle};
use crate::recovery::manager::warn_legacy_per_table_wal_files;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[cfg(test)]
use crate::storage::table_cache::TableCache;
#[cfg(test)]
use crate::txn::GlobalTxnState;
#[cfg(test)]
use crate::txn::TransactionManager;
#[cfg(test)]
use crate::txn::TXN_NONE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftStatus {
    pub(crate) is_writable_primary: bool,
    pub(crate) leader_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurabilityGuarantee {
    Buffered,
    Sync,
}

#[derive(Debug)]
pub(crate) enum DurabilityBackend {
    Disabled,
    Raft(RaftDurabilityBackend),
}

#[derive(Debug)]
pub(crate) struct RaftDurabilityBackend {
    raft_service: RaftServiceHandle,
}

#[derive(Debug)]
struct DurabilityMutationHooks {
    backend: Weak<DurabilityBackend>,
}

impl MutationHooks for DurabilityMutationHooks {
    fn before_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| StorageError("durability backend is not available".into()))?;
        backend.record(
            DurableOp::Put {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn before_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| StorageError("durability backend is not available".into()))?;
        backend.record(
            DurableOp::Delete {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn before_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| StorageError("durability backend is not available".into()))?;
        backend.record(
            DurableOp::TxnCommit { txn_id, commit_ts },
            DurabilityGuarantee::Sync,
        )
    }

    fn before_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| StorageError("durability backend is not available".into()))?;
        backend.record(
            DurableOp::TxnAbort { txn_id },
            DurabilityGuarantee::Buffered,
        )
    }

    fn should_apply_locally(&self) -> bool {
        self.backend
            .upgrade()
            .map(|backend| !backend.is_enabled())
            .unwrap_or(false)
    }
}

impl DurabilityBackend {
    pub(crate) fn open(
        base_path: &Path,
        wal_enabled: bool,
        applier: Arc<StoreCommandApplier>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        if !wal_enabled {
            return Ok(Self::Disabled);
        }

        warn_legacy_per_table_wal_files(base_path);
        Ok(Self::Raft(RaftDurabilityBackend::open(
            base_path, applier, raft_mode,
        )?))
    }

    pub(crate) fn record(
        &self,
        op: DurableOp,
        guarantee: DurabilityGuarantee,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Raft(backend) => backend.record(op, guarantee),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub(crate) fn status(&self) -> Option<RaftStatus> {
        match self {
            Self::Disabled => None,
            Self::Raft(backend) => Some(backend.status()),
        }
    }

    pub(crate) fn mutation_hooks(self: &Arc<Self>) -> Arc<dyn MutationHooks> {
        Arc::new(DurabilityMutationHooks {
            backend: Arc::downgrade(self),
        })
    }

    pub(crate) fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Raft(backend) => backend.truncate_to_checkpoint(),
        }
    }

    #[cfg(test)]
    pub(crate) fn raft_tick(&self) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Raft(backend) => backend.raft_tick(),
        }
    }

    #[cfg(test)]
    pub(crate) fn raft_handle_inbound(
        &self,
        msg: crate::raft::runtime::RaftInboundMessage,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Raft(backend) => backend.raft_handle_inbound(msg),
        }
    }
}

impl RaftDurabilityBackend {
    fn open(
        base_path: &Path,
        applier: Arc<StoreCommandApplier>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        let (raft_cfg, mut service_cfg) = raft_configs_from_mode(raft_mode)?;
        service_cfg.executor = applier;
        let raft_node = RaftNodeCore::open_with_config(base_path, raft_cfg)?;
        Ok(Self {
            raft_service: RaftServiceHandle::start(raft_node, service_cfg)?,
        })
    }

    fn record(&self, op: DurableOp, guarantee: DurabilityGuarantee) -> Result<(), WrongoDBError> {
        self.raft_service.propose(op.into())?;
        if guarantee == DurabilityGuarantee::Sync {
            self.raft_service.sync()?;
        }
        Ok(())
    }

    fn status(&self) -> RaftStatus {
        let leadership = self
            .raft_service
            .leadership()
            .expect("raft service should return leadership while backend is alive");
        RaftStatus {
            is_writable_primary: leadership.is_writable_primary(),
            leader_hint: leadership.leader_id,
        }
    }

    fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        self.raft_service.truncate_to_checkpoint()
    }

    #[cfg(test)]
    fn raft_tick(&self) -> Result<(), WrongoDBError> {
        self.raft_service.force_tick()
    }

    #[cfg(test)]
    fn raft_handle_inbound(
        &self,
        msg: crate::raft::runtime::RaftInboundMessage,
    ) -> Result<(), WrongoDBError> {
        self.raft_service.inject_inbound(msg)
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

    fn new_table_cache(
        dir: &tempfile::TempDir,
        txn_manager: Arc<TransactionManager>,
    ) -> Arc<TableCache> {
        Arc::new(TableCache::new(dir.path().to_path_buf(), txn_manager))
    }

    fn new_durability_backend(
        dir: &tempfile::TempDir,
        wal_enabled: bool,
        raft_mode: RaftMode,
    ) -> DurabilityBackend {
        let txn_manager = new_txn_manager();
        let table_cache = new_table_cache(dir, txn_manager);
        let applier = Arc::new(StoreCommandApplier::new(table_cache));
        DurabilityBackend::open(dir.path(), wal_enabled, applier, raft_mode).unwrap()
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    #[test]
    fn disabled_backend_is_noop_and_reports_disabled() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(&dir, false, RaftMode::Standalone);

        assert!(!backend.is_enabled());
        assert_eq!(backend.status(), None);
        backend
            .record(
                DurableOp::Put {
                    store_name: "users.main.wt".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: TXN_NONE,
                },
                DurabilityGuarantee::Buffered,
            )
            .unwrap();
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(&dir, true, RaftMode::Standalone);

        backend
            .record(
                DurableOp::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                },
                DurabilityGuarantee::Sync,
            )
            .unwrap();

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
    fn checkpoint_truncate_works_when_invoked_by_caller() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(&dir, true, RaftMode::Standalone);

        backend
            .record(
                DurableOp::TxnCommit {
                    txn_id: 1,
                    commit_ts: 1,
                },
                DurabilityGuarantee::Sync,
            )
            .unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        backend
            .record(DurableOp::Checkpoint, DurabilityGuarantee::Sync)
            .unwrap();
        backend.truncate_to_checkpoint().unwrap();

        let after_truncate = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after_truncate <= 512);
    }

    #[test]
    fn wal_enabled_startup_creates_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _backend = new_durability_backend(&dir, true, RaftMode::Standalone);

        assert!(raft_state_path.exists());
    }

    #[test]
    fn wal_disabled_startup_does_not_create_raft_hard_state_file() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!raft_state_path.exists());

        let _backend = new_durability_backend(&dir, false, RaftMode::Standalone);

        assert!(!raft_state_path.exists());
    }

    #[test]
    fn corrupt_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _backend = new_durability_backend(&dir, true, RaftMode::Standalone);
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
        let table_cache = new_table_cache(&dir, txn_manager);
        let applier = Arc::new(StoreCommandApplier::new(table_cache));
        let err =
            DurabilityBackend::open(dir.path(), true, applier, RaftMode::Standalone).unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }

    #[test]
    fn standalone_mode_reports_writable_primary() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(&dir, true, RaftMode::Standalone);

        let status = backend.status().unwrap();
        assert!(status.is_writable_primary);
        assert_eq!(status.leader_hint.as_deref(), Some("local"));
    }

    #[test]
    fn clustered_mode_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(
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

        let err = backend
            .record(
                DurableOp::Put {
                    store_name: "users.main.wt".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: TXN_NONE,
                },
                DurabilityGuarantee::Buffered,
            )
            .unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn runtime_tick_and_inbound_helpers_remain_callable() {
        let dir = tempdir().unwrap();
        let backend = new_durability_backend(
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

        backend.raft_tick().unwrap();

        backend
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
