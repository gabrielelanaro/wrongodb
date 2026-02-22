#![allow(dead_code)]

pub(crate) type NodeId = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProtocolLogEntry {
    pub(crate) term: u64,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct RaftProtocolState {
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<ProtocolLogEntry>,
    pub(crate) commit_index: u64,
}

impl RaftProtocolState {
    pub(crate) fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    pub(crate) fn last_log_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    // Raft log indices are 1-based. Index 0 is a sentinel that represents
    // "before the first entry", so its term is defined as 0.
    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        let zero_based = usize::try_from(index - 1).ok()?;
        self.log.get(zero_based).map(|entry| entry.term)
    }

    // Truncates using 1-based semantics: truncate_from(3) keeps entries [1, 2]
    // and removes [3..].
    pub(crate) fn truncate_from(&mut self, index: u64) {
        if index == 0 {
            self.log.clear();
            return;
        }

        let Some(zero_based) = usize::try_from(index - 1).ok() else {
            return;
        };

        if zero_based < self.log.len() {
            self.log.truncate(zero_based);
        }
    }

    pub(crate) fn append_entries(&mut self, entries: &[ProtocolLogEntry]) {
        self.log.extend_from_slice(entries);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestVoteRequest {
    pub(crate) term: u64,
    pub(crate) candidate_id: NodeId,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestVoteResponse {
    pub(crate) term: u64,
    pub(crate) vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendEntriesRequest {
    pub(crate) term: u64,
    pub(crate) leader_id: NodeId,
    pub(crate) prev_log_index: u64,
    pub(crate) prev_log_term: u64,
    pub(crate) entries: Vec<ProtocolLogEntry>,
    pub(crate) leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendEntriesResponse {
    pub(crate) term: u64,
    pub(crate) success: bool,
    pub(crate) match_index: u64,
}

pub(crate) fn handle_request_vote(
    state: &mut RaftProtocolState,
    req: RequestVoteRequest,
) -> RequestVoteResponse {
    if req.term < state.current_term {
        return RequestVoteResponse {
            term: state.current_term,
            vote_granted: false,
        };
    }

    if req.term > state.current_term {
        state.current_term = req.term;
        state.voted_for = None;
    }

    let can_vote_for_candidate =
        state.voted_for.is_none() || state.voted_for.as_deref() == Some(req.candidate_id.as_str());

    if !can_vote_for_candidate {
        return RequestVoteResponse {
            term: state.current_term,
            vote_granted: false,
        };
    }

    if !candidate_log_is_up_to_date(state, req.last_log_term, req.last_log_index) {
        return RequestVoteResponse {
            term: state.current_term,
            vote_granted: false,
        };
    }

    state.voted_for = Some(req.candidate_id);
    RequestVoteResponse {
        term: state.current_term,
        vote_granted: true,
    }
}

pub(crate) fn handle_append_entries(
    state: &mut RaftProtocolState,
    req: AppendEntriesRequest,
) -> AppendEntriesResponse {
    if req.term < state.current_term {
        return AppendEntriesResponse {
            term: state.current_term,
            success: false,
            match_index: 0,
        };
    }

    if req.term > state.current_term {
        state.current_term = req.term;
        state.voted_for = None;
    }

    let _ = &req.leader_id;

    if req.prev_log_index > state.last_log_index() {
        return AppendEntriesResponse {
            term: state.current_term,
            success: false,
            match_index: 0,
        };
    }

    if req.prev_log_index > 0 && state.term_at(req.prev_log_index) != Some(req.prev_log_term) {
        return AppendEntriesResponse {
            term: state.current_term,
            success: false,
            match_index: 0,
        };
    }

    let mut first_unmatched_entry = None;
    for (offset, incoming) in req.entries.iter().enumerate() {
        let index = req.prev_log_index + 1 + offset as u64;
        match state.term_at(index) {
            Some(existing_term) if existing_term == incoming.term => {}
            Some(_) | None => {
                first_unmatched_entry = Some(offset);
                break;
            }
        }
    }

    if let Some(offset) = first_unmatched_entry {
        let first_unmatched_index = req.prev_log_index + 1 + offset as u64;
        state.truncate_from(first_unmatched_index);
        state.append_entries(&req.entries[offset..]);
    }

    let new_commit_index = req.leader_commit.min(state.last_log_index());
    if new_commit_index > state.commit_index {
        state.commit_index = new_commit_index;
    }

    let match_index = if req.entries.is_empty() {
        req.prev_log_index
    } else {
        req.prev_log_index + req.entries.len() as u64
    };

    AppendEntriesResponse {
        term: state.current_term,
        success: true,
        match_index,
    }
}

fn candidate_log_is_up_to_date(
    state: &RaftProtocolState,
    candidate_last_log_term: u64,
    candidate_last_log_index: u64,
) -> bool {
    let local_last_log_term = state.last_log_term();
    let local_last_log_index = state.last_log_index();

    if candidate_last_log_term != local_last_log_term {
        return candidate_last_log_term > local_last_log_term;
    }

    candidate_last_log_index >= local_last_log_index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, payload: &[u8]) -> ProtocolLogEntry {
        ProtocolLogEntry {
            term,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn test_request_vote_rejects_stale_term() {
        let mut state = RaftProtocolState {
            current_term: 3,
            voted_for: None,
            log: vec![],
            commit_index: 0,
        };

        let response = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 2,
                candidate_id: "node-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        assert_eq!(
            response,
            RequestVoteResponse {
                term: 3,
                vote_granted: false
            }
        );
        assert_eq!(state.current_term, 3);
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn test_request_vote_higher_term_clears_prior_vote_even_if_log_is_behind() {
        let mut state = RaftProtocolState {
            current_term: 3,
            voted_for: Some("node-z".to_string()),
            log: vec![entry(5, b"local")],
            commit_index: 0,
        };

        let response = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 4,
                candidate_id: "node-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            },
        );

        assert_eq!(
            response,
            RequestVoteResponse {
                term: 4,
                vote_granted: false
            }
        );
        assert_eq!(state.current_term, 4);
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn test_request_vote_grants_when_unvoted_and_log_is_up_to_date() {
        let mut state = RaftProtocolState {
            current_term: 5,
            voted_for: None,
            log: vec![entry(2, b"a"), entry(5, b"b")],
            commit_index: 0,
        };

        let response = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 5,
                candidate_id: "node-a".to_string(),
                last_log_index: 2,
                last_log_term: 5,
            },
        );

        assert!(response.vote_granted);
        assert_eq!(response.term, 5);
        assert_eq!(state.voted_for, Some("node-a".to_string()));
    }

    #[test]
    fn test_request_vote_grants_repeat_vote_for_same_candidate() {
        let mut state = RaftProtocolState {
            current_term: 5,
            voted_for: Some("node-a".to_string()),
            log: vec![entry(2, b"a"), entry(5, b"b")],
            commit_index: 0,
        };

        let response = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 5,
                candidate_id: "node-a".to_string(),
                last_log_index: 2,
                last_log_term: 5,
            },
        );

        assert!(response.vote_granted);
        assert_eq!(state.voted_for, Some("node-a".to_string()));
    }

    #[test]
    fn test_request_vote_rejects_different_candidate_after_vote_cast() {
        let mut state = RaftProtocolState {
            current_term: 5,
            voted_for: Some("node-a".to_string()),
            log: vec![entry(2, b"a"), entry(5, b"b")],
            commit_index: 0,
        };

        let response = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 5,
                candidate_id: "node-b".to_string(),
                last_log_index: 2,
                last_log_term: 5,
            },
        );

        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
        assert_eq!(state.voted_for, Some("node-a".to_string()));
    }

    #[test]
    fn test_request_vote_log_up_to_date_comparisons() {
        let cases = vec![
            (4, 1, true, "higher term wins"),
            (2, 100, false, "lower term loses"),
            (3, 5, true, "equal term and higher index wins"),
            (3, 2, false, "equal term and lower index loses"),
        ];

        for (candidate_last_term, candidate_last_index, expected_grant, label) in cases {
            let mut state = RaftProtocolState {
                current_term: 7,
                voted_for: None,
                log: vec![entry(1, b"a"), entry(3, b"b"), entry(3, b"c")],
                commit_index: 0,
            };

            let response = handle_request_vote(
                &mut state,
                RequestVoteRequest {
                    term: 7,
                    candidate_id: "candidate".to_string(),
                    last_log_index: candidate_last_index,
                    last_log_term: candidate_last_term,
                },
            );

            assert_eq!(response.vote_granted, expected_grant, "{label}");
        }
    }

    #[test]
    fn test_append_entries_rejects_stale_leader_term() {
        let mut state = RaftProtocolState {
            current_term: 4,
            voted_for: None,
            log: vec![entry(2, b"a")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 3,
                leader_id: "leader".to_string(),
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert_eq!(
            response,
            AppendEntriesResponse {
                term: 4,
                success: false,
                match_index: 0
            }
        );
        assert_eq!(state.current_term, 4);
    }

    #[test]
    fn test_append_entries_higher_term_clears_vote_even_on_reject() {
        let mut state = RaftProtocolState {
            current_term: 3,
            voted_for: Some("old-candidate".to_string()),
            log: vec![],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 4,
                leader_id: "leader".to_string(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert!(!response.success);
        assert_eq!(response.term, 4);
        assert_eq!(state.current_term, 4);
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn test_append_entries_heartbeat_succeeds_when_prev_matches() {
        let mut state = RaftProtocolState {
            current_term: 4,
            voted_for: None,
            log: vec![entry(2, b"a"), entry(4, b"b")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 4,
                leader_id: "leader".to_string(),
                prev_log_index: 2,
                prev_log_term: 4,
                entries: vec![],
                leader_commit: 1,
            },
        );

        assert_eq!(
            response,
            AppendEntriesResponse {
                term: 4,
                success: true,
                match_index: 2
            }
        );
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.commit_index, 1);
    }

    #[test]
    fn test_append_entries_rejects_when_prev_log_index_is_missing() {
        let mut state = RaftProtocolState {
            current_term: 2,
            voted_for: None,
            log: vec![entry(2, b"a")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert!(!response.success);
        assert_eq!(response.match_index, 0);
        assert_eq!(state.log.len(), 1);
    }

    #[test]
    fn test_append_entries_rejects_when_prev_log_term_mismatches() {
        let mut state = RaftProtocolState {
            current_term: 2,
            voted_for: None,
            log: vec![entry(1, b"a"), entry(2, b"b")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert!(!response.success);
        assert_eq!(response.match_index, 0);
        assert_eq!(state.log.len(), 2);
    }

    #[test]
    fn test_append_entries_appends_when_follower_log_is_shorter() {
        let mut state = RaftProtocolState {
            current_term: 3,
            voted_for: None,
            log: vec![entry(1, b"a"), entry(2, b"b")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 3,
                leader_id: "leader".to_string(),
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![entry(3, b"c"), entry(3, b"d")],
                leader_commit: 0,
            },
        );

        assert!(response.success);
        assert_eq!(response.match_index, 4);
        assert_eq!(state.log.len(), 4);
        assert_eq!(state.term_at(3), Some(3));
        assert_eq!(state.term_at(4), Some(3));
    }

    #[test]
    fn test_append_entries_truncates_conflicting_suffix_and_appends() {
        let mut state = RaftProtocolState {
            current_term: 4,
            voted_for: None,
            log: vec![
                entry(1, b"a"),
                entry(2, b"b"),
                entry(4, b"x"),
                entry(4, b"y"),
            ],
            commit_index: 2,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 4,
                leader_id: "leader".to_string(),
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![entry(3, b"c"), entry(3, b"d")],
                leader_commit: 2,
            },
        );

        assert!(response.success);
        assert_eq!(response.match_index, 4);
        assert_eq!(
            state.log,
            vec![
                entry(1, b"a"),
                entry(2, b"b"),
                entry(3, b"c"),
                entry(3, b"d")
            ]
        );
    }

    #[test]
    fn test_append_entries_keeps_identical_overlapping_entries() {
        let initial_log = vec![entry(1, b"a"), entry(2, b"b"), entry(2, b"c")];
        let mut state = RaftProtocolState {
            current_term: 2,
            voted_for: None,
            log: initial_log.clone(),
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![entry(2, b"b"), entry(2, b"c")],
                leader_commit: 0,
            },
        );

        assert!(response.success);
        assert_eq!(response.match_index, 3);
        assert_eq!(state.log, initial_log);
    }

    #[test]
    fn test_append_entries_commit_index_advance_uses_min_rule() {
        let mut state = RaftProtocolState {
            current_term: 2,
            voted_for: None,
            log: vec![entry(1, b"a"), entry(1, b"b"), entry(2, b"c")],
            commit_index: 0,
        };

        let response = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 3,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 10,
            },
        );

        assert!(response.success);
        assert_eq!(state.commit_index, 3);
    }

    #[test]
    fn test_append_entries_commit_index_never_decreases() {
        let mut state = RaftProtocolState {
            current_term: 2,
            voted_for: None,
            log: vec![entry(1, b"a"), entry(1, b"b"), entry(2, b"c")],
            commit_index: 2,
        };

        let first = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 3,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 3,
            },
        );
        assert!(first.success);
        assert_eq!(state.commit_index, 3);

        let second = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 2,
                leader_id: "leader".to_string(),
                prev_log_index: 3,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 1,
            },
        );
        assert!(second.success);
        assert_eq!(state.commit_index, 3);
    }

    #[test]
    fn test_handlers_never_decrease_current_term() {
        let mut state = RaftProtocolState {
            current_term: 5,
            voted_for: None,
            log: vec![entry(5, b"a")],
            commit_index: 0,
        };

        let _ = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 4,
                candidate_id: "node-a".to_string(),
                last_log_index: 1,
                last_log_term: 5,
            },
        );
        assert_eq!(state.current_term, 5);

        let _ = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 3,
                leader_id: "leader".to_string(),
                prev_log_index: 1,
                prev_log_term: 5,
                entries: vec![],
                leader_commit: 0,
            },
        );
        assert_eq!(state.current_term, 5);

        let _ = handle_request_vote(
            &mut state,
            RequestVoteRequest {
                term: 8,
                candidate_id: "node-b".to_string(),
                last_log_index: 1,
                last_log_term: 5,
            },
        );
        assert_eq!(state.current_term, 8);

        let _ = handle_append_entries(
            &mut state,
            AppendEntriesRequest {
                term: 7,
                leader_id: "leader".to_string(),
                prev_log_index: 1,
                prev_log_term: 5,
                entries: vec![],
                leader_commit: 0,
            },
        );
        assert_eq!(state.current_term, 8);
    }
}
