#![allow(dead_code)]

use std::collections::VecDeque;

use crate::raft::node::{RaftLeadershipState, RaftNodeCore};
use crate::raft::protocol::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::role_engine::RaftEffect;
use crate::WrongoDBError;

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

        Ok(())
    }

    pub(crate) fn drain_outbound(&mut self) -> Vec<RaftOutboundMessage> {
        self.outbound.drain(..).collect()
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
}
