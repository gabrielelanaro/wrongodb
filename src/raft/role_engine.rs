use std::collections::{HashMap, HashSet};

use crate::core::errors::StorageError;
use crate::raft::protocol::{
    AppendEntriesRequest, AppendEntriesResponse, RaftProtocolState, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::WrongoDBError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftRoleConfig {
    pub(crate) local_node_id: String,
    pub(crate) peer_ids: Vec<String>,
    pub(crate) election_timeout_min_ticks: u64,
    pub(crate) election_timeout_max_ticks: u64,
    pub(crate) heartbeat_interval_ticks: u64,
    pub(crate) timeout_seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaftEffect {
    SendRequestVote {
        to: String,
        req: RequestVoteRequest,
    },
    SendAppendEntries {
        to: String,
        req: AppendEntriesRequest,
    },
}

#[derive(Debug)]
pub(crate) struct RaftRoleEngine {
    config: RaftRoleConfig,
    role: RaftRole,
    election_elapsed_ticks: u64,
    election_timeout_ticks: u64,
    heartbeat_elapsed_ticks: u64,
    rng_state: u64,
    candidate_votes: HashSet<String>,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
}

impl RaftRoleEngine {
    pub(crate) fn new(
        mut config: RaftRoleConfig,
        _protocol_state: &RaftProtocolState,
    ) -> Result<Self, WrongoDBError> {
        validate_config(&config)?;

        let mut seen = HashSet::new();
        config.peer_ids.retain(|peer_id| {
            if peer_id == &config.local_node_id {
                return false;
            }
            seen.insert(peer_id.clone())
        });

        let mut engine = Self {
            config,
            role: RaftRole::Follower,
            election_elapsed_ticks: 0,
            election_timeout_ticks: 1,
            heartbeat_elapsed_ticks: 0,
            rng_state: 0,
            candidate_votes: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        };
        engine.rng_state = engine.config.timeout_seed;
        engine.reset_election_timer();
        Ok(engine)
    }

    pub(crate) fn role(&self) -> RaftRole {
        self.role
    }

    pub(crate) fn tick(&mut self, protocol_state: &mut RaftProtocolState) -> Vec<RaftEffect> {
        match self.role {
            RaftRole::Leader => {
                self.heartbeat_elapsed_ticks = self.heartbeat_elapsed_ticks.saturating_add(1);
                if self.heartbeat_elapsed_ticks >= self.config.heartbeat_interval_ticks {
                    self.heartbeat_elapsed_ticks = 0;
                    self.build_heartbeat_effects(protocol_state)
                } else {
                    Vec::new()
                }
            }
            RaftRole::Follower | RaftRole::Candidate => {
                self.election_elapsed_ticks = self.election_elapsed_ticks.saturating_add(1);
                if self.election_elapsed_ticks >= self.election_timeout_ticks {
                    self.start_election(protocol_state)
                } else {
                    Vec::new()
                }
            }
        }
    }

    pub(crate) fn on_inbound_request_vote(
        &mut self,
        prev_term: u64,
        req: &RequestVoteRequest,
        resp: &RequestVoteResponse,
        protocol_state: &RaftProtocolState,
    ) {
        if req.term > prev_term {
            self.become_follower(protocol_state);
        }
        if resp.vote_granted {
            self.reset_election_timer();
        }
    }

    pub(crate) fn on_inbound_append_entries(
        &mut self,
        prev_term: u64,
        req: &AppendEntriesRequest,
        resp: &AppendEntriesResponse,
        protocol_state: &RaftProtocolState,
    ) {
        if req.term > prev_term || (resp.success && req.term >= protocol_state.current_term) {
            self.become_follower(protocol_state);
        }
    }

    pub(crate) fn on_request_vote_response(
        &mut self,
        from: &str,
        resp: RequestVoteResponse,
        protocol_state: &mut RaftProtocolState,
    ) -> Vec<RaftEffect> {
        if !self.is_known_peer(from) {
            return Vec::new();
        }

        if resp.term > protocol_state.current_term {
            protocol_state.current_term = resp.term;
            protocol_state.voted_for = None;
            self.become_follower(protocol_state);
            return Vec::new();
        }

        if self.role != RaftRole::Candidate {
            return Vec::new();
        }

        if resp.term < protocol_state.current_term || !resp.vote_granted {
            return Vec::new();
        }

        if !self.candidate_votes.insert(from.to_string()) {
            return Vec::new();
        }

        if self.has_majority_votes() {
            self.become_leader(protocol_state);
            return self.build_heartbeat_effects(protocol_state);
        }

        Vec::new()
    }

    pub(crate) fn on_append_entries_response(
        &mut self,
        from: &str,
        resp: AppendEntriesResponse,
        protocol_state: &mut RaftProtocolState,
    ) -> Vec<RaftEffect> {
        if !self.is_known_peer(from) {
            return Vec::new();
        }

        if resp.term > protocol_state.current_term {
            protocol_state.current_term = resp.term;
            protocol_state.voted_for = None;
            self.become_follower(protocol_state);
            return Vec::new();
        }

        if self.role != RaftRole::Leader || resp.term < protocol_state.current_term {
            return Vec::new();
        }

        if resp.success {
            let last_log_index = protocol_state.last_log_index();
            let bounded_match = resp.match_index.min(last_log_index);
            self.match_index.insert(from.to_string(), bounded_match);
            self.next_index
                .insert(from.to_string(), bounded_match.saturating_add(1).max(1));

            self.advance_leader_commit_index(protocol_state);

            let next = self.next_index.get(from).copied().unwrap_or(1);
            if next <= last_log_index {
                return vec![self.build_append_effect_for_peer(
                    from,
                    protocol_state,
                    AppendMode::ReplicateFromNext,
                )];
            }

            return Vec::new();
        }

        let current_next = self
            .next_index
            .get(from)
            .copied()
            .unwrap_or(protocol_state.last_log_index().saturating_add(1).max(1));
        let decremented = current_next.saturating_sub(1).max(1);
        self.next_index.insert(from.to_string(), decremented);

        vec![self.build_append_effect_for_peer(from, protocol_state, AppendMode::ReplicateFromNext)]
    }

    fn start_election(&mut self, protocol_state: &mut RaftProtocolState) -> Vec<RaftEffect> {
        self.role = RaftRole::Candidate;
        protocol_state.current_term = protocol_state.current_term.saturating_add(1);
        protocol_state.voted_for = Some(self.config.local_node_id.clone());

        self.candidate_votes.clear();
        self.candidate_votes
            .insert(self.config.local_node_id.clone());
        self.next_index.clear();
        self.match_index.clear();
        self.heartbeat_elapsed_ticks = 0;
        self.reset_election_timer();

        if self.has_majority_votes() {
            self.become_leader(protocol_state);
            return self.build_heartbeat_effects(protocol_state);
        }

        self.config
            .peer_ids
            .iter()
            .map(|peer_id| RaftEffect::SendRequestVote {
                to: peer_id.clone(),
                req: RequestVoteRequest {
                    term: protocol_state.current_term,
                    candidate_id: self.config.local_node_id.clone(),
                    last_log_index: protocol_state.last_log_index(),
                    last_log_term: protocol_state.last_log_term(),
                },
            })
            .collect()
    }

    fn become_follower(&mut self, _protocol_state: &RaftProtocolState) {
        self.role = RaftRole::Follower;
        self.candidate_votes.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.heartbeat_elapsed_ticks = 0;
        self.reset_election_timer();
    }

    fn become_leader(&mut self, protocol_state: &RaftProtocolState) {
        self.role = RaftRole::Leader;
        self.candidate_votes.clear();
        self.heartbeat_elapsed_ticks = 0;
        self.next_index.clear();
        self.match_index.clear();

        let next = protocol_state.last_log_index().saturating_add(1).max(1);
        for peer_id in &self.config.peer_ids {
            self.next_index.insert(peer_id.clone(), next);
            self.match_index.insert(peer_id.clone(), 0);
        }
    }

    fn build_heartbeat_effects(&self, protocol_state: &RaftProtocolState) -> Vec<RaftEffect> {
        self.config
            .peer_ids
            .iter()
            .map(|peer| {
                self.build_append_effect_for_peer(peer, protocol_state, AppendMode::Heartbeat)
            })
            .collect()
    }

    fn build_append_effect_for_peer(
        &self,
        peer_id: &str,
        protocol_state: &RaftProtocolState,
        mode: AppendMode,
    ) -> RaftEffect {
        let next_index = self.next_index.get(peer_id).copied().unwrap_or(1).max(1);
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = protocol_state.term_at(prev_log_index).unwrap_or(0);

        let entries = match mode {
            AppendMode::Heartbeat => Vec::new(),
            AppendMode::ReplicateFromNext => {
                let start = usize::try_from(next_index.saturating_sub(1)).unwrap_or(usize::MAX);
                if start < protocol_state.log.len() {
                    protocol_state.log[start..].to_vec()
                } else {
                    Vec::new()
                }
            }
        };

        RaftEffect::SendAppendEntries {
            to: peer_id.to_string(),
            req: AppendEntriesRequest {
                term: protocol_state.current_term,
                leader_id: self.config.local_node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: protocol_state.commit_index,
            },
        }
    }

    fn advance_leader_commit_index(&self, protocol_state: &mut RaftProtocolState) {
        let last_index = protocol_state.last_log_index();
        if last_index == 0 || protocol_state.commit_index >= last_index {
            return;
        }

        let majority = self.majority_threshold();
        for n in ((protocol_state.commit_index + 1)..=last_index).rev() {
            if protocol_state.term_at(n) != Some(protocol_state.current_term) {
                continue;
            }

            let mut replicated = 1usize;
            for peer_id in &self.config.peer_ids {
                if self.match_index.get(peer_id).copied().unwrap_or(0) >= n {
                    replicated += 1;
                }
            }

            if replicated >= majority {
                protocol_state.commit_index = n;
                return;
            }
        }
    }

    fn is_known_peer(&self, peer_id: &str) -> bool {
        self.config.peer_ids.iter().any(|peer| peer == peer_id)
    }

    fn has_majority_votes(&self) -> bool {
        self.candidate_votes.len() >= self.majority_threshold()
    }

    fn majority_threshold(&self) -> usize {
        (self.config.peer_ids.len() + 1) / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed_ticks = 0;
        self.election_timeout_ticks = self.next_election_timeout_ticks();
    }

    fn next_election_timeout_ticks(&mut self) -> u64 {
        let min_ticks = self.config.election_timeout_min_ticks;
        let max_ticks = self.config.election_timeout_max_ticks;
        if min_ticks == max_ticks {
            return min_ticks;
        }

        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let span = max_ticks - min_ticks + 1;
        min_ticks + (self.rng_state % span)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendMode {
    Heartbeat,
    ReplicateFromNext,
}

fn validate_config(config: &RaftRoleConfig) -> Result<(), WrongoDBError> {
    if config.local_node_id.is_empty() {
        return Err(StorageError("raft local_node_id cannot be empty".into()).into());
    }

    if config.election_timeout_min_ticks == 0 {
        return Err(StorageError("raft election_timeout_min_ticks must be >= 1".into()).into());
    }

    if config.election_timeout_max_ticks < config.election_timeout_min_ticks {
        return Err(StorageError(
            "raft election_timeout_max_ticks must be >= election_timeout_min_ticks".into(),
        )
        .into());
    }

    if config.heartbeat_interval_ticks == 0 {
        return Err(StorageError("raft heartbeat_interval_ticks must be >= 1".into()).into());
    }

    for peer_id in &config.peer_ids {
        if peer_id.is_empty() {
            return Err(StorageError("raft peer id cannot be empty".into()).into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::protocol::ProtocolLogEntry;

    fn entry(term: u64, payload: &[u8]) -> ProtocolLogEntry {
        ProtocolLogEntry {
            term,
            payload: payload.to_vec(),
        }
    }

    fn config(local: &str, peers: &[&str], min: u64, max: u64, heartbeat: u64) -> RaftRoleConfig {
        RaftRoleConfig {
            local_node_id: local.to_string(),
            peer_ids: peers.iter().map(|p| (*p).to_string()).collect(),
            election_timeout_min_ticks: min,
            election_timeout_max_ticks: max,
            heartbeat_interval_ticks: heartbeat,
            timeout_seed: 7,
        }
    }

    #[test]
    fn follower_timeout_starts_election_and_emits_request_vote() {
        let cfg = config("n1", &["n2", "n3"], 2, 2, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();

        assert!(engine.tick(&mut state).is_empty());
        let effects = engine.tick(&mut state);

        assert_eq!(engine.role(), RaftRole::Candidate);
        assert_eq!(state.current_term, 1);
        assert_eq!(state.voted_for.as_deref(), Some("n1"));
        assert_eq!(effects.len(), 2);
    }

    #[test]
    fn candidate_majority_vote_transitions_to_leader_and_emits_heartbeats() {
        let cfg = config("n1", &["n2", "n3"], 1, 1, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();

        let _ = engine.tick(&mut state);
        let effects = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );

        assert_eq!(engine.role(), RaftRole::Leader);
        assert_eq!(effects.len(), 2);
        for effect in effects {
            match effect {
                RaftEffect::SendAppendEntries { req, .. } => assert!(req.entries.is_empty()),
                other => panic!("unexpected effect: {other:?}"),
            }
        }
    }

    #[test]
    fn candidate_without_majority_retries_after_timeout_with_higher_term() {
        let cfg = config("n1", &["n2", "n3", "n4"], 2, 2, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();

        let _ = engine.tick(&mut state);
        let _ = engine.tick(&mut state);
        let effects = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );
        assert!(effects.is_empty());
        assert_eq!(engine.role(), RaftRole::Candidate);
        assert_eq!(state.current_term, 1);

        let _ = engine.tick(&mut state);
        let retry_effects = engine.tick(&mut state);
        assert_eq!(state.current_term, 2);
        assert_eq!(engine.role(), RaftRole::Candidate);
        assert_eq!(retry_effects.len(), 3);
    }

    #[test]
    fn higher_term_vote_response_steps_down_to_follower() {
        let cfg = config("n1", &["n2"], 1, 1, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);
        assert_eq!(engine.role(), RaftRole::Candidate);

        let effects = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 5,
                vote_granted: false,
            },
            &mut state,
        );
        assert!(effects.is_empty());
        assert_eq!(engine.role(), RaftRole::Follower);
        assert_eq!(state.current_term, 5);
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn leader_emits_heartbeat_at_configured_interval() {
        let cfg = config("n1", &["n2"], 1, 1, 2);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);
        let _ = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );
        assert_eq!(engine.role(), RaftRole::Leader);

        assert!(engine.tick(&mut state).is_empty());
        let effects = engine.tick(&mut state);
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            RaftEffect::SendAppendEntries { req, .. } => assert!(req.entries.is_empty()),
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn append_failure_decrements_next_index_and_emits_retry() {
        let cfg = config("n1", &["n2"], 1, 1, 1);
        let mut state = RaftProtocolState {
            current_term: 0,
            voted_for: None,
            log: vec![entry(1, b"a"), entry(1, b"b"), entry(1, b"c")],
            commit_index: 0,
        };
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);
        let _ = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );

        let effects = engine.on_append_entries_response(
            "n2",
            AppendEntriesResponse {
                term: 1,
                success: false,
                match_index: 0,
            },
            &mut state,
        );

        assert_eq!(effects.len(), 1);
        match &effects[0] {
            RaftEffect::SendAppendEntries { req, .. } => {
                assert_eq!(req.prev_log_index, 2);
                assert_eq!(req.prev_log_term, 1);
                assert_eq!(req.entries.len(), 1);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn append_success_updates_indexes_and_emits_follow_up_when_behind() {
        let cfg = config("n1", &["n2"], 1, 1, 1);
        let mut state = RaftProtocolState {
            current_term: 0,
            voted_for: None,
            log: vec![
                entry(1, b"a"),
                entry(1, b"b"),
                entry(1, b"c"),
                entry(1, b"d"),
            ],
            commit_index: 0,
        };
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);
        let _ = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );

        let effects = engine.on_append_entries_response(
            "n2",
            AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 2,
            },
            &mut state,
        );

        assert_eq!(effects.len(), 1);
        match &effects[0] {
            RaftEffect::SendAppendEntries { req, .. } => {
                assert_eq!(req.prev_log_index, 2);
                assert_eq!(req.entries.len(), 2);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn commit_advances_by_majority_with_current_term_gate() {
        let cfg = config("n1", &["n2", "n3"], 1, 1, 1);
        let mut state = RaftProtocolState {
            current_term: 0,
            voted_for: None,
            log: vec![
                entry(1, b"a"),
                entry(2, b"b"),
                entry(3, b"c"),
                entry(2, b"d"),
            ],
            commit_index: 0,
        };
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);
        let _ = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );
        state.current_term = 3;

        let _ = engine.on_append_entries_response(
            "n2",
            AppendEntriesResponse {
                term: 3,
                success: true,
                match_index: 4,
            },
            &mut state,
        );
        let _ = engine.on_append_entries_response(
            "n3",
            AppendEntriesResponse {
                term: 3,
                success: true,
                match_index: 4,
            },
            &mut state,
        );

        assert_eq!(state.commit_index, 3);
    }

    #[test]
    fn unknown_peers_and_duplicate_votes_are_ignored() {
        let cfg = config("n1", &["n2", "n3"], 1, 1, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let _ = engine.tick(&mut state);

        let unknown = engine.on_request_vote_response(
            "n9",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );
        assert!(unknown.is_empty());
        assert_eq!(engine.role(), RaftRole::Candidate);

        let _ = engine.on_request_vote_response(
            "n2",
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
            &mut state,
        );
        assert_eq!(engine.role(), RaftRole::Leader);
    }

    #[test]
    fn single_node_becomes_leader_after_timeout() {
        let cfg = config("n1", &[], 1, 1, 1);
        let mut state = RaftProtocolState::default();
        let mut engine = RaftRoleEngine::new(cfg, &state).unwrap();
        let effects = engine.tick(&mut state);

        assert!(effects.is_empty());
        assert_eq!(engine.role(), RaftRole::Leader);
        assert_eq!(state.current_term, 1);
        assert_eq!(state.voted_for.as_deref(), Some("n1"));
    }
}
