use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::core::errors::StorageError;
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp, StoreCommandApplier};
use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
use crate::raft::service::{RaftServiceConfig, RaftServiceHandle};
use crate::replication::{RaftMode, RaftPeerConfig};
use crate::WrongoDBError;

#[cfg(test)]
use parking_lot::Mutex;

/// Wire-protocol-facing primary/leader state derived from replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HelloState {
    pub(crate) is_writable_primary: bool,
    pub(crate) leader_hint: Option<String>,
}

/// Write coordination policy for higher layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WritePathMode {
    LocalApply,
    DeferredReplication,
}

/// Shared replication coordinator above storage durability.
///
/// This type owns RAFT runtime concerns and answers protocol-facing topology
/// questions. In standalone mode it degrades to simple local-write coordination
/// and delegates durability markers to the local durability backend.
#[derive(Debug)]
pub(crate) enum ReplicationCoordinator {
    Standalone,
    Raft(RaftReplicationCoordinator),
    #[cfg(test)]
    Test(TestReplicationCoordinator),
}

#[derive(Debug)]
pub(crate) struct RaftReplicationCoordinator {
    raft_service: RaftServiceHandle,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestReplicationCoordinator {
    write_path_mode: WritePathMode,
    recorded_ops: Arc<Mutex<Vec<(DurableOp, DurabilityGuarantee)>>>,
    hello_state: HelloState,
}

impl ReplicationCoordinator {
    pub(crate) fn open(
        base_path: &Path,
        enabled: bool,
        applier: Arc<StoreCommandApplier>,
        raft_mode: RaftMode,
    ) -> Result<Self, WrongoDBError> {
        if !enabled {
            return Ok(Self::Standalone);
        }

        match raft_mode {
            RaftMode::Standalone => Ok(Self::Standalone),
            clustered_mode => Ok(Self::Raft(RaftReplicationCoordinator::open(
                base_path,
                applier,
                clustered_mode,
            )?)),
        }
    }

    #[cfg(test)]
    pub(crate) fn standalone() -> Self {
        Self::Standalone
    }

    pub(crate) fn hello_state(&self) -> HelloState {
        match self {
            Self::Standalone => HelloState {
                is_writable_primary: true,
                leader_hint: None,
            },
            Self::Raft(backend) => backend.hello_state(),
            #[cfg(test)]
            Self::Test(backend) => backend.hello_state.clone(),
        }
    }

    pub(crate) fn write_path_mode(&self) -> WritePathMode {
        match self {
            Self::Standalone => WritePathMode::LocalApply,
            Self::Raft(_) => WritePathMode::DeferredReplication,
            #[cfg(test)]
            Self::Test(backend) => backend.write_path_mode,
        }
    }

    pub(crate) fn record(
        &self,
        durability_backend: &DurabilityBackend,
        op: DurableOp,
        guarantee: DurabilityGuarantee,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Standalone => durability_backend.record(op, guarantee),
            Self::Raft(backend) => backend.record(op, guarantee),
            #[cfg(test)]
            Self::Test(backend) => {
                backend.recorded_ops.lock().push((op, guarantee));
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn raft_tick(&self) -> Result<(), WrongoDBError> {
        match self {
            Self::Standalone => Ok(()),
            Self::Raft(backend) => backend.raft_tick(),
            #[cfg(test)]
            Self::Test(_) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn raft_handle_inbound(
        &self,
        msg: crate::raft::runtime::RaftInboundMessage,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Standalone => Ok(()),
            Self::Raft(backend) => backend.raft_handle_inbound(msg),
            #[cfg(test)]
            Self::Test(_) => {
                let _ = msg;
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_backend(
        write_path_mode: WritePathMode,
    ) -> (Self, Arc<Mutex<Vec<(DurableOp, DurabilityGuarantee)>>>) {
        let recorded_ops = Arc::new(Mutex::new(Vec::new()));
        (
            Self::Test(TestReplicationCoordinator {
                write_path_mode,
                recorded_ops: recorded_ops.clone(),
                hello_state: HelloState {
                    is_writable_primary: write_path_mode == WritePathMode::LocalApply,
                    leader_hint: None,
                },
            }),
            recorded_ops,
        )
    }
}

impl RaftReplicationCoordinator {
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

    fn hello_state(&self) -> HelloState {
        let leadership = self
            .raft_service
            .leadership()
            .expect("raft service should return leadership while backend is alive");
        HelloState {
            is_writable_primary: leadership.is_writable_primary(),
            leader_hint: leadership.leader_id,
        }
    }

    fn record(&self, op: DurableOp, guarantee: DurabilityGuarantee) -> Result<(), WrongoDBError> {
        self.raft_service.propose(op.into())?;
        if guarantee == DurabilityGuarantee::Sync {
            self.raft_service.sync()?;
        }
        Ok(())
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
    use crate::durability::StoreCommandApplier;
    use crate::raft::hard_state::RaftHardStateStore;
    use crate::raft::runtime::RaftInboundMessage;
    use crate::storage::table_cache::TableCache;
    use crate::txn::{GlobalTxnState, TransactionManager, TXN_NONE};

    fn new_txn_manager() -> Arc<TransactionManager> {
        Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())))
    }

    fn new_table_cache(
        dir: &tempfile::TempDir,
        txn_manager: Arc<TransactionManager>,
    ) -> Arc<TableCache> {
        Arc::new(TableCache::new(dir.path().to_path_buf(), txn_manager))
    }

    fn new_replication_coordinator(
        dir: &tempfile::TempDir,
        enabled: bool,
        raft_mode: RaftMode,
    ) -> ReplicationCoordinator {
        let txn_manager = new_txn_manager();
        let table_cache = new_table_cache(dir, txn_manager.clone());
        let applier = Arc::new(StoreCommandApplier::new(table_cache, txn_manager));
        ReplicationCoordinator::open(dir.path(), enabled, applier, raft_mode).unwrap()
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    #[test]
    fn disabled_coordination_reports_writable_primary() {
        let dir = tempdir().unwrap();
        let coordinator = new_replication_coordinator(&dir, false, RaftMode::Standalone);

        assert_eq!(
            coordinator.hello_state(),
            HelloState {
                is_writable_primary: true,
                leader_hint: None,
            }
        );
        assert_eq!(coordinator.write_path_mode(), WritePathMode::LocalApply);
    }

    #[test]
    fn corrupt_cluster_raft_hard_state_fails_startup() {
        let dir = tempdir().unwrap();
        let raft_state_path = RaftHardStateStore::path_for_db(dir.path());

        let _coordinator = new_replication_coordinator(
            &dir,
            true,
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                local_raft_addr: free_local_addr(),
                peers: vec![RaftPeerConfig {
                    node_id: "n2".to_string(),
                    raft_addr: "127.0.0.1:65003".to_string(),
                }],
            },
        );
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
        let table_cache = new_table_cache(&dir, txn_manager.clone());
        let applier = Arc::new(StoreCommandApplier::new(table_cache, txn_manager));
        let err = ReplicationCoordinator::open(
            dir.path(),
            true,
            applier,
            RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                local_raft_addr: free_local_addr(),
                peers: vec![RaftPeerConfig {
                    node_id: "n2".to_string(),
                    raft_addr: "127.0.0.1:65004".to_string(),
                }],
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("raft hard state"));
    }

    #[test]
    fn clustered_mode_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let coordinator = new_replication_coordinator(
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
        let durability = DurabilityBackend::Disabled;

        let err = coordinator
            .record(
                &durability,
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
        let coordinator = new_replication_coordinator(
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

        coordinator.raft_tick().unwrap();

        coordinator
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
