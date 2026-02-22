use std::path::Path;

use crate::core::errors::StorageError;
use crate::raft::hard_state::{RaftHardState, RaftHardStateStore};
use crate::raft::log_store::RaftLogStore;
use crate::raft::protocol::{
    handle_append_entries, handle_request_vote, AppendEntriesRequest, AppendEntriesResponse,
    RaftProtocolState, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::role_engine::{RaftEffect, RaftRole, RaftRoleConfig, RaftRoleEngine};
use crate::storage::wal::{GlobalWal, Lsn};
use crate::txn::{Timestamp, TxnId};
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
    #[allow(dead_code)]
    local_node_id: String,
    #[allow(dead_code)]
    hard_state_store: RaftHardStateStore,
    hard_state: RaftHardState,
    progress: RaftProgress,
    protocol_log_store: RaftLogStore,
    protocol_state: RaftProtocolState,
    role_engine: RaftRoleEngine,
    wal: GlobalWal,
}

impl RaftNodeCore {
    pub(crate) fn open<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        Self::open_with_config(db_dir, RaftNodeConfig::default())
    }

    pub(crate) fn open_with_config<P: AsRef<Path>>(
        db_dir: P,
        cfg: RaftNodeConfig,
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
            commit_index: protocol_last_log_index,
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
        let _last_raft_term = wal.last_raft_term();
        let last_raft_index = wal.last_raft_index();
        let progress = RaftProgress {
            commit_index: last_raft_index,
            last_applied: last_raft_index,
        };

        Ok(Self {
            local_node_id: cfg.local_node_id,
            hard_state_store,
            hard_state,
            progress,
            protocol_log_store,
            protocol_state,
            role_engine,
            wal,
        })
    }

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

    #[allow(dead_code)]
    pub(crate) fn role(&self) -> RaftRole {
        self.role_engine.role()
    }

    #[allow(dead_code)]
    pub(crate) fn tick(&mut self) -> Result<Vec<RaftEffect>, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();
        let previous_log = self.protocol_state.log.clone();

        let effects = self.role_engine.tick(&mut self.protocol_state);
        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        Ok(effects)
    }

    #[allow(dead_code)]
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
        Ok(response)
    }

    #[allow(dead_code)]
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
        Ok(response)
    }

    #[allow(dead_code)]
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
        Ok(effects)
    }

    #[allow(dead_code)]
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
        Ok(effects)
    }

    pub(crate) fn log_put(
        &mut self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        let lsn = self
            .wal
            .log_put(store_name, key, value, txn_id, self.current_term())?;
        self.refresh_progress_from_wal_tail();
        Ok(lsn)
    }

    pub(crate) fn log_delete(
        &mut self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        let lsn = self
            .wal
            .log_delete(store_name, key, txn_id, self.current_term())?;
        self.refresh_progress_from_wal_tail();
        Ok(lsn)
    }

    pub(crate) fn log_txn_commit(
        &mut self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<Lsn, WrongoDBError> {
        let lsn = self
            .wal
            .log_txn_commit(txn_id, commit_ts, self.current_term())?;
        self.refresh_progress_from_wal_tail();
        Ok(lsn)
    }

    pub(crate) fn log_txn_abort(&mut self, txn_id: TxnId) -> Result<Lsn, WrongoDBError> {
        let lsn = self.wal.log_txn_abort(txn_id, self.current_term())?;
        self.refresh_progress_from_wal_tail();
        Ok(lsn)
    }

    pub(crate) fn log_checkpoint(&mut self) -> Result<Lsn, WrongoDBError> {
        let lsn = self.wal.log_checkpoint(self.current_term())?;
        self.refresh_progress_from_wal_tail();
        Ok(lsn)
    }

    pub(crate) fn set_checkpoint_lsn(&mut self, lsn: Lsn) -> Result<(), WrongoDBError> {
        self.wal.set_checkpoint_lsn(lsn)
    }

    pub(crate) fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.wal.truncate_to_checkpoint()?;
        self.refresh_progress_from_wal_tail();
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.wal.sync()?;
        self.protocol_log_store.sync()?;
        Ok(())
    }

    fn refresh_progress_from_wal_tail(&mut self) {
        let last_raft_index = self.wal.last_raft_index();
        self.progress.commit_index = last_raft_index;
        self.progress.last_applied = last_raft_index;
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
    use crate::raft::protocol::{AppendEntriesRequest, ProtocolLogEntry, RequestVoteRequest};
    use crate::raft::role_engine::{RaftEffect, RaftRole};
    use crate::storage::wal::{WalReader, WalRecord};

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
    fn append_uses_current_term_in_wal_header() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();
        node.set_current_term(5).unwrap();
        node.log_put("users.main.wt", b"k1", b"v1", 1).unwrap();
        node.sync().unwrap();

        let mut reader = WalReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        let (header, _record) = reader.read_record().unwrap().unwrap();
        assert_eq!(header.raft_term, 5);
        assert_eq!(header.raft_index, 1);
    }

    #[test]
    fn append_advances_commit_and_apply_progress_monotonically() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 0,
                last_applied: 0
            }
        );

        node.log_put("users.main.wt", b"k1", b"v1", 1).unwrap();
        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 1,
                last_applied: 1
            }
        );

        node.log_txn_commit(1, 1).unwrap();
        assert_eq!(
            node.progress(),
            RaftProgress {
                commit_index: 2,
                last_applied: 2
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

        let mut reader = WalReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn log_methods_append_expected_record_types() {
        let dir = tempdir().unwrap();
        let mut node = RaftNodeCore::open(dir.path()).unwrap();

        node.log_put("users.main.wt", b"k1", b"v1", 1).unwrap();
        node.log_delete("users.main.wt", b"k1", 1).unwrap();
        node.log_txn_abort(1).unwrap();
        node.sync().unwrap();

        let mut reader = WalReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
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
