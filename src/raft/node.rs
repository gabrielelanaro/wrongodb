use std::collections::VecDeque;
use std::path::Path;

use crate::core::errors::StorageError;
use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::raft::hard_state::{RaftHardState, RaftHardStateStore};
use crate::raft::log_store::RaftLogStore;
use crate::raft::protocol::{
    handle_append_entries, handle_request_vote, AppendEntriesRequest, AppendEntriesResponse,
    ProtocolLogEntry, RaftProtocolState, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::role_engine::{RaftEffect, RaftRole, RaftRoleConfig, RaftRoleEngine};
use crate::storage::wal::GlobalWal;
use crate::WrongoDBError;

const LOCAL_NODE_ID: &str = "local";
const DEFAULT_ELECTION_TIMEOUT_MIN_TICKS: u64 = 10;
const DEFAULT_ELECTION_TIMEOUT_MAX_TICKS: u64 = 20;
const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 3;
const DEFAULT_TIMEOUT_SEED: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RaftProgress {
    pub commit_index: u64,
    pub last_applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftLeadershipState {
    pub role: RaftRole,
    pub current_term: u64,
    pub local_node_id: String,
    pub leader_id: Option<String>,
}

impl RaftLeadershipState {
    pub(crate) fn is_writable_primary(&self) -> bool {
        self.role == RaftRole::Leader
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftNodeConfig {
    pub(crate) local_node_id: String,
    pub(crate) peer_ids: Vec<String>,
    pub(crate) election_timeout_min_ticks: u64,
    pub(crate) election_timeout_max_ticks: u64,
    pub(crate) heartbeat_interval_ticks: u64,
    pub(crate) timeout_seed: u64,
}

impl Default for RaftNodeConfig {
    fn default() -> Self {
        Self {
            local_node_id: LOCAL_NODE_ID.to_string(),
            peer_ids: Vec::new(),
            election_timeout_min_ticks: DEFAULT_ELECTION_TIMEOUT_MIN_TICKS,
            election_timeout_max_ticks: DEFAULT_ELECTION_TIMEOUT_MAX_TICKS,
            heartbeat_interval_ticks: DEFAULT_HEARTBEAT_INTERVAL_TICKS,
            timeout_seed: DEFAULT_TIMEOUT_SEED,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RaftNodeCore {
    local_node_id: String,
    hard_state_store: RaftHardStateStore,
    hard_state: RaftHardState,
    progress: RaftProgress,
    protocol_log_store: RaftLogStore,
    protocol_state: RaftProtocolState,
    role_engine: RaftRoleEngine,
    last_applied_protocol_index: u64,
    wal: GlobalWal,
    applied_commands: VecDeque<CommittedCommand>,
}

impl RaftNodeCore {
    #[allow(dead_code)]
    pub(crate) fn open<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        Self::open_with_config(db_dir, RaftNodeConfig::default())
    }

    pub(crate) fn open_with_config<P: AsRef<Path>>(
        db_dir: P,
        cfg: RaftNodeConfig,
    ) -> Result<Self, WrongoDBError> {
        Self::open_with_config_and_applied_index(db_dir, cfg, 0)
    }

    pub(crate) fn open_with_config_and_applied_index<P: AsRef<Path>>(
        db_dir: P,
        cfg: RaftNodeConfig,
        applied_through_index: u64,
    ) -> Result<Self, WrongoDBError> {
        let hard_state_store = RaftHardStateStore::open_or_create(db_dir.as_ref())?;
        let hard_state = hard_state_store.state().clone();
        let protocol_log_store = RaftLogStore::open_or_create(db_dir.as_ref())?;
        let (protocol_last_log_index, protocol_last_log_term) =
            protocol_log_store.last_log_index_term();
        debug_assert_eq!(
            protocol_log_store
                .term_at(protocol_last_log_index)
                .unwrap_or(0),
            protocol_last_log_term
        );
        let protocol_state = RaftProtocolState {
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for.clone(),
            log: protocol_log_store.entries().to_vec(),
            commit_index: 0,
        };
        let role_engine = RaftRoleEngine::new(
            RaftRoleConfig {
                local_node_id: cfg.local_node_id.clone(),
                peer_ids: cfg.peer_ids.clone(),
                election_timeout_min_ticks: cfg.election_timeout_min_ticks,
                election_timeout_max_ticks: cfg.election_timeout_max_ticks,
                heartbeat_interval_ticks: cfg.heartbeat_interval_ticks,
                timeout_seed: cfg.timeout_seed,
            },
            &protocol_state,
        )?;

        let wal = GlobalWal::open_or_create(db_dir)?;
        if applied_through_index > protocol_last_log_index {
            return Err(StorageError(format!(
                "applied_through_index {applied_through_index} exceeds protocol log index {protocol_last_log_index}"
            ))
            .into());
        }
        let commit_index = applied_through_index;
        let last_applied_protocol_index = applied_through_index;
        let mut protocol_state = protocol_state;
        protocol_state.commit_index = commit_index;
        let progress = RaftProgress {
            commit_index,
            last_applied: last_applied_protocol_index,
        };
        let standalone_mode = cfg.peer_ids.is_empty();

        let mut node = Self {
            local_node_id: cfg.local_node_id,
            hard_state_store,
            hard_state,
            progress,
            protocol_log_store,
            protocol_state,
            role_engine,
            last_applied_protocol_index,
            wal,
            applied_commands: VecDeque::new(),
        };

        // Standalone mode bootstrap: zero-peer nodes self-elect on startup so writes are
        // immediately writable without an external tick loop.
        if standalone_mode {
            node.role_engine
                .bootstrap_single_node_leader(&node.protocol_state);
        }

        Ok(node)
    }

    #[allow(dead_code)]
    pub(crate) fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    #[allow(dead_code)]
    pub(crate) fn voted_for(&self) -> Option<&str> {
        self.hard_state.voted_for.as_deref()
    }

    #[allow(dead_code)]
    pub(crate) fn progress(&self) -> RaftProgress {
        self.progress
    }

    #[allow(dead_code)]
    pub(crate) fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    #[allow(dead_code)]
    pub(crate) fn set_current_term(&mut self, new_term: u64) -> Result<(), WrongoDBError> {
        self.hard_state_store.set_current_term(new_term)?;
        self.hard_state = self.hard_state_store.state().clone();
        self.protocol_state.current_term = self.hard_state.current_term;
        self.protocol_state.voted_for = self.hard_state.voted_for.clone();
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_voted_for(&mut self, candidate: Option<&str>) -> Result<(), WrongoDBError> {
        if matches!(candidate, Some("")) {
            return Err(StorageError("raft voted_for cannot be empty".into()).into());
        }
        self.hard_state_store.set_voted_for(candidate)?;
        self.hard_state = self.hard_state_store.state().clone();
        self.protocol_state.current_term = self.hard_state.current_term;
        self.protocol_state.voted_for = self.hard_state.voted_for.clone();
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn protocol_state(&self) -> &RaftProtocolState {
        &self.protocol_state
    }

    pub(crate) fn role(&self) -> RaftRole {
        self.role_engine.role()
    }

    pub(crate) fn leadership(&self) -> RaftLeadershipState {
        let role = self.role();
        let leader_id = self
            .role_engine
            .leader_id()
            .map(|id| id.to_string())
            .or_else(|| {
                if role == RaftRole::Leader {
                    Some(self.local_node_id.clone())
                } else {
                    None
                }
            });
        RaftLeadershipState {
            role,
            current_term: self.protocol_state.current_term,
            local_node_id: self.local_node_id.clone(),
            leader_id,
        }
    }

    pub(crate) fn ensure_writable_leader(&self) -> Result<(), WrongoDBError> {
        let leadership = self.leadership();
        if leadership.is_writable_primary() {
            return Ok(());
        }

        Err(WrongoDBError::NotLeader {
            leader_hint: leadership.leader_id,
        })
    }

    pub(crate) fn propose_command(
        &mut self,
        command: &RaftCommand,
    ) -> Result<(u64, Vec<RaftEffect>), WrongoDBError> {
        self.ensure_writable_leader()?;

        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();

        let entry = ProtocolLogEntry {
            term: self.protocol_state.current_term,
            payload: command.encode(),
        };
        self.protocol_state.log.push(entry);
        let proposal_index = self.protocol_state.last_log_index();

        let effects = self
            .role_engine
            .on_local_log_appended(&mut self.protocol_state);

        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        self.refresh_progress_from_protocol_state();
        Ok((proposal_index, effects))
    }

    pub(crate) fn apply_committed_entries(&mut self) -> Result<u64, WrongoDBError> {
        while self.last_applied_protocol_index < self.protocol_state.commit_index {
            let apply_index = self.last_applied_protocol_index + 1;
            let entry_pos = usize::try_from(apply_index.saturating_sub(1))
                .map_err(|_| StorageError("raft apply index conversion overflow".into()))?;
            let entry = self
                .protocol_state
                .log
                .get(entry_pos)
                .ok_or_else(|| {
                    StorageError(format!(
                        "raft apply index {} out of bounds (log_len={})",
                        apply_index,
                        self.protocol_state.log.len()
                    ))
                })?
                .clone();

            let command = RaftCommand::decode(&entry.payload)?;
            self.apply_command_to_wal(&command)?;
            self.applied_commands.push_back(CommittedCommand {
                index: apply_index,
                term: entry.term,
                command,
            });
            self.last_applied_protocol_index = apply_index;
        }

        self.refresh_progress_from_protocol_state();
        Ok(self.last_applied_protocol_index)
    }

    pub(crate) fn applied_index(&self) -> u64 {
        self.last_applied_protocol_index
    }

    pub(crate) fn is_index_committed_and_applied(&self, index: u64) -> bool {
        self.protocol_state.commit_index >= index && self.last_applied_protocol_index >= index
    }

    pub(crate) fn is_index_committed(&self, index: u64) -> bool {
        self.protocol_state.commit_index >= index
    }

    pub(crate) fn drain_committed_commands(&mut self) -> Vec<CommittedCommand> {
        self.applied_commands.drain(..).collect()
    }

    pub(crate) fn tick(&mut self) -> Result<Vec<RaftEffect>, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();

        let effects = self.role_engine.tick(&mut self.protocol_state);
        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        self.refresh_progress_from_protocol_state();
        Ok(effects)
    }

    pub(crate) fn handle_request_vote_rpc(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let req_for_hook = req.clone();

        let response = handle_request_vote(&mut self.protocol_state, req);
        self.role_engine.on_inbound_request_vote(
            previous_term,
            &req_for_hook,
            &response,
            &self.protocol_state,
        );
        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.refresh_progress_from_protocol_state();
        Ok(response)
    }

    pub(crate) fn handle_append_entries_rpc(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();
        let req_for_hook = req.clone();

        let response = handle_append_entries(&mut self.protocol_state, req);
        self.role_engine.on_inbound_append_entries(
            previous_term,
            &req_for_hook,
            &response,
            &self.protocol_state,
        );
        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        self.refresh_progress_from_protocol_state();
        Ok(response)
    }

    pub(crate) fn handle_request_vote_response_rpc(
        &mut self,
        from: &str,
        resp: RequestVoteResponse,
    ) -> Result<Vec<RaftEffect>, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();

        let effects =
            self.role_engine
                .on_request_vote_response(from, resp, &mut self.protocol_state);

        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        self.refresh_progress_from_protocol_state();
        Ok(effects)
    }

    pub(crate) fn handle_append_entries_response_rpc(
        &mut self,
        from: &str,
        resp: AppendEntriesResponse,
    ) -> Result<Vec<RaftEffect>, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();

        let effects =
            self.role_engine
                .on_append_entries_response(from, resp, &mut self.protocol_state);

        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        self.refresh_progress_from_protocol_state();
        Ok(effects)
    }

    pub(crate) fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.wal.truncate_to_checkpoint()?;
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.wal.sync()?;
        self.protocol_log_store.sync()?;
        Ok(())
    }

    fn apply_command_to_wal(&mut self, command: &RaftCommand) -> Result<(), WrongoDBError> {
        match command {
            RaftCommand::Put {
                uri,
                key,
                value,
                txn_id,
            } => {
                let _ = self.wal.log_put(uri, key, value, *txn_id)?;
            }
            RaftCommand::Delete { uri, key, txn_id } => {
                let _ = self.wal.log_delete(uri, key, *txn_id)?;
            }
            RaftCommand::TxnCommit { txn_id, commit_ts } => {
                let _ = self.wal.log_txn_commit(*txn_id, *commit_ts)?;
            }
            RaftCommand::TxnAbort { txn_id } => {
                let _ = self.wal.log_txn_abort(*txn_id)?;
            }
            RaftCommand::Checkpoint => {
                let lsn = self.wal.log_checkpoint()?;
                self.wal.set_checkpoint_lsn(lsn)?;
            }
        }
        Ok(())
    }

    fn refresh_progress_from_protocol_state(&mut self) {
        self.progress.commit_index = self.protocol_state.commit_index;
        self.progress.last_applied = self.last_applied_protocol_index;
    }

    fn persist_hard_state_if_changed(
        &mut self,
        previous_term: u64,
        previous_vote: Option<String>,
    ) -> Result<(), WrongoDBError> {
        if self.protocol_state.current_term != previous_term {
            self.hard_state_store
                .set_current_term(self.protocol_state.current_term)?;
        }

        if self.protocol_state.voted_for != previous_vote {
            self.hard_state_store
                .set_voted_for(self.protocol_state.voted_for.as_deref())?;
        }

        self.hard_state = self.hard_state_store.state().clone();
        self.protocol_state.current_term = self.hard_state.current_term;
        self.protocol_state.voted_for = self.hard_state.voted_for.clone();
        Ok(())
    }

    fn persist_protocol_log_if_changed(
        &mut self,
        previous_log: Vec<crate::raft::protocol::ProtocolLogEntry>,
    ) -> Result<(), WrongoDBError> {
        if self.protocol_state.log == previous_log {
            return Ok(());
        }

        let mut divergence = 0usize;
        while divergence < previous_log.len()
            && divergence < self.protocol_state.log.len()
            && previous_log[divergence] == self.protocol_state.log[divergence]
        {
            divergence += 1;
        }

        if divergence < previous_log.len() {
            self.protocol_log_store
                .truncate_from((divergence + 1) as u64)?;
        }

        if divergence < self.protocol_state.log.len() {
            self.protocol_log_store
                .append_entries(&self.protocol_state.log[divergence..])?;
        }

        self.protocol_log_store.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use tempfile::tempdir;

    use super::*;
    use crate::raft::command::RaftCommand;
    use crate::raft::protocol::{AppendEntriesRequest, ProtocolLogEntry, RequestVoteRequest};
    use crate::raft::role_engine::{RaftEffect, RaftRole};
    use crate::storage::wal::{WalFileReader, WalReader, WalRecord};

    fn entry(term: u64, payload: &[u8]) -> ProtocolLogEntry {
        ProtocolLogEntry {
            term,
            payload: payload.to_vec(),
        }
    }

    fn node_cfg(local: &str, peers: &[&str], min: u64, max: u64, heartbeat: u64) -> RaftNodeConfig {
        RaftNodeConfig {
            local_node_id: local.to_string(),
            peer_ids: peers.iter().map(|peer| (*peer).to_string()).collect(),
            election_timeout_min_ticks: min,
            election_timeout_max_ticks: max,
            heartbeat_interval_ticks: heartbeat,
            timeout_seed: 11,
        }
    }

    fn elect_with_single_peer(node: &mut RaftNodeCore, peer_id: &str) {
        let mut effects = Vec::new();
        for _ in 0..8 {
            effects = node.tick().unwrap();
            if !effects.is_empty() {
                break;
            }
        }
        assert!(!effects.is_empty(), "expected election effects");
        for effect in effects {
            match effect {
                RaftEffect::SendRequestVote { to, req } => {
                    assert_eq!(to, peer_id);
                    let _ = node
                        .handle_request_vote_response_rpc(
                            &to,
                            crate::raft::protocol::RequestVoteResponse {
                                term: req.term,
                                vote_granted: true,
                            },
                        )
                        .unwrap();
                }
                other => panic!("unexpected election effect: {other:?}"),
            }
        }
        assert_eq!(node.role(), RaftRole::Leader);
    }

    #[test]
    fn standalone_open_bootstraps_to_writable_leader() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open(dir.path()).unwrap();
        let leadership = node.leadership();

        assert_eq!(leadership.role, RaftRole::Leader);
        assert_eq!(leadership.current_term, 0);
        assert_eq!(leadership.leader_id.as_deref(), Some("local"));
        node.ensure_writable_leader().unwrap();
    }

    #[test]
    fn clustered_open_starts_non_leader_and_rejects_writes() {
        let dir = tempdir().unwrap();
        let node =
            RaftNodeCore::open_with_config(dir.path(), node_cfg("n1", &["n2", "n3"], 10, 10, 3))
                .unwrap();
        let leadership = node.leadership();

        assert_eq!(leadership.role, RaftRole::Follower);
        let err = node.ensure_writable_leader().unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn leader_proposal_appends_log_and_emits_replication_effects() {
        let dir = tempdir().unwrap();
        let mut node =
            RaftNodeCore::open_with_config(dir.path(), node_cfg("n1", &["n2"], 1, 1, 1)).unwrap();
        elect_with_single_peer(&mut node, "n2");

        let (proposal_index, effects) = node
            .propose_command(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();

        assert_eq!(proposal_index, 1);
        assert_eq!(node.protocol_state().last_log_index(), 1);
        assert_eq!(node.protocol_state().commit_index, 0);
        assert_eq!(node.applied_index(), 0);
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            RaftEffect::SendAppendEntries { to, req } => {
                assert_eq!(to, "n2");
                assert_eq!(req.entries.len(), 1);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn apply_committed_entries_is_idempotent_and_ordered() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        node.propose_command(&RaftCommand::Put {
            uri: "table:users".to_string(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            txn_id: 1,
        })
        .unwrap();
        node.propose_command(&RaftCommand::Delete {
            uri: "table:users".to_string(),
            key: b"k1".to_vec(),
            txn_id: 1,
        })
        .unwrap();
        assert_eq!(node.protocol_state().commit_index, 2);
        assert_eq!(node.applied_index(), 0);

        node.apply_committed_entries().unwrap();
        assert_eq!(node.applied_index(), 2);
        node.apply_committed_entries().unwrap();
        assert_eq!(node.applied_index(), 2);
    }

    #[test]
    fn restart_keeps_uncommitted_protocol_tail_unapplied() {
        let dir = tempdir().unwrap();
        {
            let mut node =
                RaftNodeCore::open_with_config(dir.path(), node_cfg("n1", &["n2"], 1, 1, 1))
                    .unwrap();
            elect_with_single_peer(&mut node, "n2");
            let (proposal_index, _effects) = node
                .propose_command(&RaftCommand::Put {
                    uri: "table:users".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: 1,
                })
                .unwrap();
            assert_eq!(proposal_index, 1);
            assert_eq!(node.protocol_state().commit_index, 0);
            assert_eq!(node.applied_index(), 0);
            node.sync().unwrap();
        }

        let reopened =
            RaftNodeCore::open_with_config(dir.path(), node_cfg("n1", &["n2"], 1, 1, 1)).unwrap();
        assert_eq!(reopened.protocol_state().last_log_index(), 1);
        assert_eq!(reopened.protocol_state().commit_index, 0);
        assert_eq!(reopened.applied_index(), 0);

        let mut reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn startup_fails_when_applied_index_exceeds_protocol_log_index() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();
        node.propose_command(&RaftCommand::Put {
            uri: "table:users".to_string(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            txn_id: 1,
        })
        .unwrap();
        node.sync().unwrap();

        let err = RaftNodeCore::open_with_config_and_applied_index(
            dir.path(),
            RaftNodeConfig::default(),
            2,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("applied_through_index 2 exceeds protocol log index 1"));
    }

    struct InMemoryCluster {
        nodes: HashMap<String, RaftNodeCore>,
    }

    impl InMemoryCluster {
        fn new(nodes: HashMap<String, RaftNodeCore>) -> Self {
            Self { nodes }
        }

        fn node(&self, id: &str) -> &RaftNodeCore {
            self.nodes.get(id).unwrap()
        }

        fn node_mut(&mut self, id: &str) -> &mut RaftNodeCore {
            self.nodes.get_mut(id).unwrap()
        }

        fn tick(&mut self, id: &str) -> Vec<RaftEffect> {
            self.node_mut(id).tick().unwrap()
        }

        fn role(&self, id: &str) -> RaftRole {
            self.node(id).role()
        }

        fn queue_effects(
            queue: &mut VecDeque<(String, RaftEffect)>,
            from: &str,
            effects: Vec<RaftEffect>,
        ) {
            for effect in effects {
                queue.push_back((from.to_string(), effect));
            }
        }

        fn deliver_one(
            &mut self,
            from: String,
            effect: RaftEffect,
            queue: &mut VecDeque<(String, RaftEffect)>,
        ) {
            match effect {
                RaftEffect::SendRequestVote { to, req } => {
                    let resp = self.node_mut(&to).handle_request_vote_rpc(req).unwrap();
                    let follow_ups = self
                        .node_mut(&from)
                        .handle_request_vote_response_rpc(&to, resp)
                        .unwrap();
                    Self::queue_effects(queue, &from, follow_ups);
                }
                RaftEffect::SendAppendEntries { to, req } => {
                    let resp = self.node_mut(&to).handle_append_entries_rpc(req).unwrap();
                    let follow_ups = self
                        .node_mut(&from)
                        .handle_append_entries_response_rpc(&to, resp)
                        .unwrap();
                    Self::queue_effects(queue, &from, follow_ups);
                }
            }
        }

        fn drain_queue(&mut self, queue: &mut VecDeque<(String, RaftEffect)>, max_steps: usize) {
            for _ in 0..max_steps {
                let Some((from, effect)) = queue.pop_front() else {
                    break;
                };
                self.deliver_one(from, effect, queue);
            }
            assert!(queue.is_empty(), "effect queue did not drain");
        }
    }

    #[test]
    fn append_writes_a_storage_wal_record() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();
        node.set_current_term(5).unwrap();
        let (_index, _effects) = node
            .propose_command(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();
        node.apply_committed_entries().unwrap();
        node.sync().unwrap();

        let mut reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        let (header, _record) = reader.read_record().unwrap().unwrap();
        assert_eq!(header.lsn, crate::storage::wal::Lsn::new(0, 512));
        assert_eq!(header.prev_lsn, crate::storage::wal::Lsn::new(0, 0));
    }

    #[test]
    fn propose_advances_commit_but_requires_apply_for_last_applied_progress() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 0,
                last_applied: 0
            }
        );

        let (_idx, _effects) = node
            .propose_command(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();
        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 1,
                last_applied: 0
            }
        );

        node.apply_committed_entries().unwrap();
        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 1,
                last_applied: 1
            }
        );
    }

    #[test]
    fn set_current_term_is_monotonic_and_clears_vote() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        node.set_voted_for(Some("node-a")).unwrap();
        assert_eq!(node.voted_for(), Some("node-a"));

        node.set_current_term(3).unwrap();
        assert_eq!(node.current_term(), 3);
        assert_eq!(node.voted_for(), None);

        let err = node.set_current_term(2).unwrap_err();
        assert!(err.to_string().contains("raft term cannot decrease"));
    }

    #[test]
    fn voted_for_persists_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut node = RaftNodeCore::open(dir.path()).unwrap();
            node.set_current_term(8).unwrap();
            node.set_voted_for(Some("node-b")).unwrap();
        }

        let reopened = RaftNodeCore::open(dir.path()).unwrap();
        assert_eq!(reopened.local_node_id(), "local");
        assert_eq!(reopened.current_term(), 8);
        assert_eq!(reopened.voted_for(), Some("node-b"));

        let mut reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn log_methods_append_expected_record_types() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        let commands = vec![
            RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            },
            RaftCommand::Delete {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                txn_id: 1,
            },
            RaftCommand::TxnAbort { txn_id: 1 },
        ];
        for command in &commands {
            node.propose_command(command).unwrap();
        }
        node.apply_committed_entries().unwrap();
        node.sync().unwrap();

        let mut reader = WalFileReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        let mut types = Vec::new();
        while let Some((_header, record)) = reader.read_record().unwrap() {
            let t = match record {
                WalRecord::Put { .. } => "put",
                WalRecord::Delete { .. } => "delete",
                WalRecord::TxnAbort { .. } => "abort",
                WalRecord::TxnCommit { .. } => "commit",
                WalRecord::Checkpoint => "checkpoint",
            };
            types.push(t);
        }

        assert_eq!(types, vec!["put", "delete", "abort"]);
    }

    #[test]
    fn request_vote_higher_term_reject_persists_term_and_clears_vote_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut node = RaftNodeCore::open(dir.path()).unwrap();
            node.handle_append_entries_rpc(AppendEntriesRequest {
                term: 5,
                leader_id: "leader-a".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry(5, b"local-tail")],
                leader_commit: 0,
            })
            .unwrap();
            node.set_current_term(5).unwrap();
            node.set_voted_for(Some("node-a")).unwrap();

            let response = node
                .handle_request_vote_rpc(RequestVoteRequest {
                    term: 6,
                    candidate_id: "node-b".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .unwrap();

            assert!(!response.vote_granted);
            assert_eq!(response.term, 6);
            assert_eq!(node.current_term(), 6);
            assert_eq!(node.voted_for(), None);
        }

        let reopened = RaftNodeCore::open(dir.path()).unwrap();
        assert_eq!(reopened.current_term(), 6);
        assert_eq!(reopened.voted_for(), None);
    }

    #[test]
    fn append_entries_conflict_repair_is_durable_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut node = RaftNodeCore::open(dir.path()).unwrap();

            let first = node
                .handle_append_entries_rpc(AppendEntriesRequest {
                    term: 1,
                    leader_id: "leader-a".to_string(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![entry(1, b"a"), entry(1, b"b"), entry(1, b"c")],
                    leader_commit: 0,
                })
                .unwrap();
            assert!(first.success);

            let second = node
                .handle_append_entries_rpc(AppendEntriesRequest {
                    term: 2,
                    leader_id: "leader-b".to_string(),
                    prev_log_index: 1,
                    prev_log_term: 1,
                    entries: vec![entry(2, b"x"), entry(2, b"y")],
                    leader_commit: 2,
                })
                .unwrap();
            assert!(second.success);
            assert_eq!(
                node.protocol_state().log,
                vec![entry(1, b"a"), entry(2, b"x"), entry(2, b"y")]
            );
        }

        let reopened = RaftNodeCore::open(dir.path()).unwrap();
        assert_eq!(
            reopened.protocol_state().log,
            vec![entry(1, b"a"), entry(2, b"x"), entry(2, b"y")]
        );
    }

    #[test]
    fn append_entries_commit_index_never_decreases_through_node_adapter() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        node.handle_append_entries_rpc(AppendEntriesRequest {
            term: 3,
            leader_id: "leader-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry(3, b"a"), entry(3, b"b"), entry(3, b"c")],
            leader_commit: 1,
        })
        .unwrap();
        assert_eq!(node.protocol_state().commit_index, 1);

        node.handle_append_entries_rpc(AppendEntriesRequest {
            term: 3,
            leader_id: "leader-a".to_string(),
            prev_log_index: 3,
            prev_log_term: 3,
            entries: vec![],
            leader_commit: 3,
        })
        .unwrap();
        assert_eq!(node.protocol_state().commit_index, 3);

        node.handle_append_entries_rpc(AppendEntriesRequest {
            term: 3,
            leader_id: "leader-a".to_string(),
            prev_log_index: 3,
            prev_log_term: 3,
            entries: vec![],
            leader_commit: 1,
        })
        .unwrap();
        assert_eq!(node.protocol_state().commit_index, 3);
    }

    #[test]
    fn three_node_tick_driven_election_selects_one_leader() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_c = tempdir().unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".to_string(),
            RaftNodeCore::open_with_config(dir_a.path(), node_cfg("n1", &["n2", "n3"], 2, 2, 2))
                .unwrap(),
        );
        nodes.insert(
            "n2".to_string(),
            RaftNodeCore::open_with_config(dir_b.path(), node_cfg("n2", &["n1", "n3"], 5, 5, 2))
                .unwrap(),
        );
        nodes.insert(
            "n3".to_string(),
            RaftNodeCore::open_with_config(dir_c.path(), node_cfg("n3", &["n1", "n2"], 5, 5, 2))
                .unwrap(),
        );
        let mut cluster = InMemoryCluster::new(nodes);
        let mut queue = VecDeque::new();

        for _ in 0..2 {
            for id in ["n1", "n2", "n3"] {
                let effects = cluster.tick(id);
                InMemoryCluster::queue_effects(&mut queue, id, effects);
            }
            cluster.drain_queue(&mut queue, 64);
        }

        assert_eq!(cluster.role("n1"), RaftRole::Leader);
        assert_eq!(cluster.role("n2"), RaftRole::Follower);
        assert_eq!(cluster.role("n3"), RaftRole::Follower);
    }

    #[test]
    fn no_majority_round_then_re_election_succeeds() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_c = tempdir().unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".to_string(),
            RaftNodeCore::open_with_config(dir_a.path(), node_cfg("n1", &["n2", "n3"], 2, 4, 2))
                .unwrap(),
        );
        nodes.insert(
            "n2".to_string(),
            RaftNodeCore::open_with_config(dir_b.path(), node_cfg("n2", &["n1", "n3"], 6, 6, 2))
                .unwrap(),
        );
        nodes.insert(
            "n3".to_string(),
            RaftNodeCore::open_with_config(dir_c.path(), node_cfg("n3", &["n1", "n2"], 6, 6, 2))
                .unwrap(),
        );
        let mut cluster = InMemoryCluster::new(nodes);

        // First election on n1: drop all outgoing RequestVote effects.
        let dropped = loop {
            let effects = cluster.tick("n1");
            if !effects.is_empty() {
                break effects;
            }
        };
        assert!(!dropped.is_empty());
        assert_eq!(cluster.role("n1"), RaftRole::Candidate);

        // Advance ticks enough for a retry election and deliver effects this time.
        let mut queue = VecDeque::new();
        let mut elected = false;
        for _ in 0..20 {
            let effects = cluster.tick("n1");
            InMemoryCluster::queue_effects(&mut queue, "n1", effects);
            cluster.drain_queue(&mut queue, 128);
            if cluster.role("n1") == RaftRole::Leader {
                elected = true;
                break;
            }
        }

        assert!(elected, "n1 should eventually re-elect itself");
        assert_eq!(cluster.role("n1"), RaftRole::Leader);
        assert!(cluster.node("n1").current_term() >= 2);
    }

    #[test]
    fn leader_replicates_entries_and_advances_commit_via_effects() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_c = tempdir().unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".to_string(),
            RaftNodeCore::open_with_config(dir_a.path(), node_cfg("n1", &["n2", "n3"], 2, 2, 1))
                .unwrap(),
        );
        nodes.insert(
            "n2".to_string(),
            RaftNodeCore::open_with_config(dir_b.path(), node_cfg("n2", &["n1", "n3"], 10, 10, 1))
                .unwrap(),
        );
        nodes.insert(
            "n3".to_string(),
            RaftNodeCore::open_with_config(dir_c.path(), node_cfg("n3", &["n1", "n2"], 10, 10, 1))
                .unwrap(),
        );
        let mut cluster = InMemoryCluster::new(nodes);
        let mut queue = VecDeque::new();

        // Elect n1.
        for _ in 0..2 {
            for id in ["n1", "n2", "n3"] {
                let effects = cluster.tick(id);
                InMemoryCluster::queue_effects(&mut queue, id, effects);
            }
            cluster.drain_queue(&mut queue, 128);
        }
        assert_eq!(cluster.role("n1"), RaftRole::Leader);

        // Inject one leader-term entry and replicate it via effects.
        {
            let leader = cluster.node_mut("n1");
            let leader_term = leader.protocol_state.current_term;
            leader
                .protocol_state
                .log
                .push(entry(leader_term, b"leader-current-term"));
        }

        let heartbeat_effects = cluster.tick("n1");
        InMemoryCluster::queue_effects(&mut queue, "n1", heartbeat_effects);
        cluster.drain_queue(&mut queue, 256);

        let leader = cluster.node("n1");
        assert!(leader.protocol_state.commit_index >= 1);
    }
}
