use std::path::Path;

use crate::core::errors::StorageError;
use crate::raft::hard_state::{RaftHardState, RaftHardStateStore};
use crate::raft::log_store::RaftLogStore;
use crate::raft::protocol::{
    handle_append_entries, handle_request_vote, AppendEntriesRequest, AppendEntriesResponse,
    RaftProtocolState, RequestVoteRequest, RequestVoteResponse,
};
use crate::storage::wal::{GlobalWal, Lsn};
use crate::txn::{Timestamp, TxnId};
use crate::WrongoDBError;

const LOCAL_NODE_ID: &str = "local";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RaftProgress {
    pub commit_index: u64,
    pub last_applied: u64,
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
    wal: GlobalWal,
}

impl RaftNodeCore {
    pub(crate) fn open<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
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

        let wal = GlobalWal::open_or_create(db_dir)?;
        let _last_raft_term = wal.last_raft_term();
        let last_raft_index = wal.last_raft_index();
        let progress = RaftProgress {
            commit_index: last_raft_index,
            last_applied: last_raft_index,
        };

        Ok(Self {
            local_node_id: LOCAL_NODE_ID.to_string(),
            hard_state_store,
            hard_state,
            progress,
            protocol_log_store,
            protocol_state,
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
    pub(crate) fn handle_request_vote_rpc(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, WrongoDBError> {
        let previous_term = self.protocol_state.current_term;
        let previous_vote = self.protocol_state.voted_for.clone();

        let response = handle_request_vote(&mut self.protocol_state, req);
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

        let response = handle_append_entries(&mut self.protocol_state, req);
        self.persist_hard_state_if_changed(previous_term, previous_vote)?;
        self.persist_protocol_log_if_changed(previous_log)?;
        Ok(response)
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
    use tempfile::tempdir;

    use super::*;
    use crate::raft::protocol::{AppendEntriesRequest, ProtocolLogEntry, RequestVoteRequest};
    use crate::storage::wal::{WalReader, WalRecord};

    fn entry(term: u64, payload: &[u8]) -> ProtocolLogEntry {
        ProtocolLogEntry {
            term,
            payload: payload.to_vec(),
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
}
