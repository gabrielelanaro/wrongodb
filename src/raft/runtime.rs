#![allow(dead_code)]

use std::collections::VecDeque;

use crate::core::errors::StorageError;
use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::raft::node::{RaftLeadershipState, RaftNodeCore};
use crate::raft::protocol::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::role_engine::RaftEffect;
use crate::WrongoDBError;

const DEFAULT_PROPOSAL_WAIT_STEPS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaftInboundMessage {
    RequestVote {
        from: String,
        req: RequestVoteRequest,
    },
    AppendEntries {
        from: String,
        req: AppendEntriesRequest,
    },
    RequestVoteResponse {
        from: String,
        resp: RequestVoteResponse,
    },
    AppendEntriesResponse {
        from: String,
        resp: AppendEntriesResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaftOutboundMessage {
    RequestVote {
        to: String,
        req: RequestVoteRequest,
    },
    AppendEntries {
        to: String,
        req: AppendEntriesRequest,
    },
    RequestVoteResponse {
        to: String,
        resp: RequestVoteResponse,
    },
    AppendEntriesResponse {
        to: String,
        resp: AppendEntriesResponse,
    },
}

#[derive(Debug)]
pub(crate) struct RaftRuntime {
    node: RaftNodeCore,
    outbound: VecDeque<RaftOutboundMessage>,
}

impl RaftRuntime {
    pub(crate) fn new(node: RaftNodeCore) -> Self {
        Self {
            node,
            outbound: VecDeque::new(),
        }
    }

    pub(crate) fn leadership(&self) -> RaftLeadershipState {
        self.node.leadership()
    }

    pub(crate) fn ensure_writable_leader(&self) -> Result<(), WrongoDBError> {
        self.node.ensure_writable_leader()
    }

    pub(crate) fn propose(&mut self, command: &RaftCommand) -> Result<u64, WrongoDBError> {
        let proposal_index = self.propose_without_wait(command)?;
        self.wait_for_commit(proposal_index, DEFAULT_PROPOSAL_WAIT_STEPS)?;
        Ok(proposal_index)
    }

    pub(crate) fn propose_without_wait(
        &mut self,
        command: &RaftCommand,
    ) -> Result<u64, WrongoDBError> {
        let (proposal_index, effects) = self.node.propose_command(command)?;
        self.enqueue_effects(effects);
        Ok(proposal_index)
    }

    pub(crate) fn tick(&mut self) -> Result<(), WrongoDBError> {
        let effects = self.node.tick()?;
        self.enqueue_effects(effects);
        Ok(())
    }

    pub(crate) fn handle_inbound(&mut self, msg: RaftInboundMessage) -> Result<(), WrongoDBError> {
        match msg {
            RaftInboundMessage::RequestVote { from, req } => {
                let resp = self.node.handle_request_vote_rpc(req)?;
                self.outbound
                    .push_back(RaftOutboundMessage::RequestVoteResponse { to: from, resp });
            }
            RaftInboundMessage::AppendEntries { from, req } => {
                let resp = self.node.handle_append_entries_rpc(req)?;
                self.outbound
                    .push_back(RaftOutboundMessage::AppendEntriesResponse { to: from, resp });
            }
            RaftInboundMessage::RequestVoteResponse { from, resp } => {
                let effects = self.node.handle_request_vote_response_rpc(&from, resp)?;
                self.enqueue_effects(effects);
            }
            RaftInboundMessage::AppendEntriesResponse { from, resp } => {
                let effects = self.node.handle_append_entries_response_rpc(&from, resp)?;
                self.enqueue_effects(effects);
            }
        }

        self.node.apply_committed_entries()?;
        Ok(())
    }

    pub(crate) fn wait_for_commit(
        &mut self,
        proposal_index: u64,
        max_steps: usize,
    ) -> Result<(), WrongoDBError> {
        self.wait_for_commit_with_driver(proposal_index, max_steps, |_| Ok(None))
    }

    pub(crate) fn wait_for_commit_with_driver<F>(
        &mut self,
        proposal_index: u64,
        max_steps: usize,
        mut driver: F,
    ) -> Result<(), WrongoDBError>
    where
        F: FnMut(RaftOutboundMessage) -> Result<Option<RaftInboundMessage>, WrongoDBError>,
    {
        for _ in 0..max_steps {
            self.node.apply_committed_entries()?;
            if self.node.is_index_committed_and_applied(proposal_index) {
                return Ok(());
            }

            let outbound = self.drain_outbound();
            if outbound.is_empty() {
                self.tick()?;
                continue;
            }

            for msg in outbound {
                if let Some(inbound) = driver(msg)? {
                    self.handle_inbound(inbound)?;
                }
            }
        }

        Err(StorageError(format!(
            "raft proposal at index {proposal_index} timed out before quorum commit"
        ))
        .into())
    }

    pub(crate) fn step_until_quiescent<F>(
        &mut self,
        max_steps: usize,
        mut driver: F,
    ) -> Result<(), WrongoDBError>
    where
        F: FnMut(RaftOutboundMessage) -> Result<Option<RaftInboundMessage>, WrongoDBError>,
    {
        for _ in 0..max_steps {
            let outbound = self.drain_outbound();
            if outbound.is_empty() {
                return Ok(());
            }

            for msg in outbound {
                if let Some(inbound) = driver(msg)? {
                    self.handle_inbound(inbound)?;
                }
            }
            self.node.apply_committed_entries()?;
        }

        Err(StorageError("raft runtime did not quiesce within max_steps".into()).into())
    }

    pub(crate) fn drain_outbound(&mut self) -> Vec<RaftOutboundMessage> {
        self.outbound.drain(..).collect()
    }

    pub(crate) fn apply_committed_entries(&mut self) -> Result<u64, WrongoDBError> {
        self.node.apply_committed_entries()
    }

    pub(crate) fn drain_committed_commands(&mut self) -> Vec<CommittedCommand> {
        self.node.drain_committed_commands()
    }

    pub(crate) fn sync_node(&mut self) -> Result<(), WrongoDBError> {
        self.node.sync()
    }

    pub(crate) fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.node.truncate_to_checkpoint()
    }

    pub(crate) fn is_index_committed_and_applied(&self, index: u64) -> bool {
        self.node.is_index_committed_and_applied(index)
    }

    pub(crate) fn is_index_committed(&self, index: u64) -> bool {
        self.node.is_index_committed(index)
    }

    pub(crate) fn applied_index(&self) -> u64 {
        self.node.applied_index()
    }

    pub(crate) fn node_mut(&mut self) -> &mut RaftNodeCore {
        &mut self.node
    }

    fn enqueue_effects(&mut self, effects: Vec<RaftEffect>) {
        for effect in effects {
            match effect {
                RaftEffect::SendRequestVote { to, req } => {
                    self.outbound
                        .push_back(RaftOutboundMessage::RequestVote { to, req });
                }
                RaftEffect::SendAppendEntries { to, req } => {
                    self.outbound
                        .push_back(RaftOutboundMessage::AppendEntries { to, req });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::raft::command::RaftCommand;
    use crate::raft::node::{RaftNodeConfig, RaftNodeCore};
    use crate::raft::role_engine::RaftRole;

    fn cfg(local: &str, peers: &[&str], min: u64, max: u64, heartbeat: u64) -> RaftNodeConfig {
        RaftNodeConfig {
            local_node_id: local.to_string(),
            peer_ids: peers.iter().map(|peer| (*peer).to_string()).collect(),
            election_timeout_min_ticks: min,
            election_timeout_max_ticks: max,
            heartbeat_interval_ticks: heartbeat,
            timeout_seed: 17,
        }
    }

    #[test]
    fn tick_emits_outbound_request_vote_when_election_starts() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open_with_config(dir.path(), cfg("n1", &["n2"], 1, 1, 1)).unwrap();
        let mut runtime = RaftRuntime::new(node);

        runtime.tick().unwrap();
        let outbound = runtime.drain_outbound();
        assert_eq!(outbound.len(), 1);
        match &outbound[0] {
            RaftOutboundMessage::RequestVote { to, req } => {
                assert_eq!(to, "n2");
                assert_eq!(req.candidate_id, "n1");
            }
            other => panic!("unexpected outbound message: {other:?}"),
        }
    }

    #[test]
    fn inbound_request_generates_response_envelope() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open_with_config(dir.path(), cfg("n1", &["n2"], 5, 5, 1)).unwrap();
        let mut runtime = RaftRuntime::new(node);

        runtime
            .handle_inbound(RaftInboundMessage::RequestVote {
                from: "n2".to_string(),
                req: RequestVoteRequest {
                    term: 1,
                    candidate_id: "n2".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            })
            .unwrap();

        let outbound = runtime.drain_outbound();
        assert_eq!(outbound.len(), 1);
        match &outbound[0] {
            RaftOutboundMessage::RequestVoteResponse { to, resp } => {
                assert_eq!(to, "n2");
                assert!(resp.vote_granted);
            }
            other => panic!("unexpected outbound message: {other:?}"),
        }
    }

    #[test]
    fn inbound_vote_response_drives_role_transition_and_heartbeats() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open_with_config(dir.path(), cfg("n1", &["n2"], 1, 1, 1)).unwrap();
        let mut runtime = RaftRuntime::new(node);

        runtime.tick().unwrap();
        let _ = runtime.drain_outbound();

        runtime
            .handle_inbound(RaftInboundMessage::RequestVoteResponse {
                from: "n2".to_string(),
                resp: RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                },
            })
            .unwrap();

        let leadership = runtime.leadership();
        assert_eq!(leadership.role, RaftRole::Leader);

        let outbound = runtime.drain_outbound();
        assert_eq!(outbound.len(), 1);
        match &outbound[0] {
            RaftOutboundMessage::AppendEntries { to, req } => {
                assert_eq!(to, "n2");
                assert!(req.entries.is_empty());
            }
            other => panic!("unexpected outbound message: {other:?}"),
        }
    }

    #[test]
    fn propose_commits_immediately_in_single_node_mode() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open(dir.path()).unwrap();
        let mut runtime = RaftRuntime::new(node);

        let proposal_index = runtime
            .propose(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();

        assert_eq!(proposal_index, 1);
        assert!(runtime.node.applied_index() >= 1);
    }

    #[test]
    fn propose_times_out_without_transport_progress_in_cluster_mode() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open_with_config(dir.path(), cfg("n1", &["n2"], 1, 1, 1)).unwrap();
        let mut runtime = RaftRuntime::new(node);

        runtime.tick().unwrap();
        let vote_request = runtime.drain_outbound().remove(0);
        let vote_resp = match vote_request {
            RaftOutboundMessage::RequestVote { to, req } => {
                assert_eq!(to, "n2");
                RaftInboundMessage::RequestVoteResponse {
                    from: "n2".to_string(),
                    resp: RequestVoteResponse {
                        term: req.term,
                        vote_granted: true,
                    },
                }
            }
            other => panic!("unexpected outbound message: {other:?}"),
        };
        runtime.handle_inbound(vote_resp).unwrap();
        let _ = runtime.drain_outbound();
        assert_eq!(runtime.leadership().role, RaftRole::Leader);

        let err = runtime
            .propose(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn wait_for_commit_with_driver_commits_after_majority_ack() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let mut leader = RaftRuntime::new(
            RaftNodeCore::open_with_config(dir_a.path(), cfg("n1", &["n2"], 1, 1, 1)).unwrap(),
        );
        let mut follower = RaftRuntime::new(
            RaftNodeCore::open_with_config(dir_b.path(), cfg("n2", &["n1"], 5, 5, 1)).unwrap(),
        );

        leader.tick().unwrap();
        let vote_req = leader.drain_outbound().remove(0);
        let req = match vote_req {
            RaftOutboundMessage::RequestVote { to, req } => {
                assert_eq!(to, "n2");
                req
            }
            other => panic!("unexpected message: {other:?}"),
        };
        follower
            .handle_inbound(RaftInboundMessage::RequestVote {
                from: "n1".to_string(),
                req,
            })
            .unwrap();
        let vote_resp = follower.drain_outbound().remove(0);
        if let RaftOutboundMessage::RequestVoteResponse { to, resp } = vote_resp {
            assert_eq!(to, "n1");
            leader
                .handle_inbound(RaftInboundMessage::RequestVoteResponse {
                    from: "n2".to_string(),
                    resp,
                })
                .unwrap();
        } else {
            panic!("expected vote response");
        }
        assert_eq!(leader.leadership().role, RaftRole::Leader);
        let _ = leader.drain_outbound();

        let (proposal_index, effects) = leader
            .node
            .propose_command(&RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();
        leader.enqueue_effects(effects);
        leader
            .wait_for_commit_with_driver(proposal_index, 64, |outbound| match outbound {
                RaftOutboundMessage::AppendEntries { to, req } => {
                    assert_eq!(to, "n2");
                    follower.handle_inbound(RaftInboundMessage::AppendEntries {
                        from: "n1".to_string(),
                        req,
                    })?;
                    let response = follower
                        .drain_outbound()
                        .into_iter()
                        .next()
                        .ok_or_else(|| StorageError("missing follower append response".into()))?;
                    match response {
                        RaftOutboundMessage::AppendEntriesResponse { to, resp } => {
                            assert_eq!(to, "n1");
                            Ok(Some(RaftInboundMessage::AppendEntriesResponse {
                                from: "n2".to_string(),
                                resp,
                            }))
                        }
                        other => Err(StorageError(format!(
                            "unexpected follower outbound message: {other:?}"
                        ))
                        .into()),
                    }
                }
                other => Err(StorageError(format!(
                    "unexpected leader outbound message: {other:?}"
                ))
                .into()),
            })
            .unwrap();

        assert!(leader.node.is_index_committed_and_applied(proposal_index));
    }
}
