use std::path::Path;

use crate::core::errors::StorageError;
use crate::raft::hard_state::{RaftHardState, RaftHardStateStore};
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
    wal: GlobalWal,
}

impl RaftNodeCore {
    pub(crate) fn open<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let hard_state_store = RaftHardStateStore::open_or_create(db_dir.as_ref())?;
        let hard_state = hard_state_store.state().clone();

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
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_voted_for(&mut self, candidate: Option<&str>) -> Result<(), WrongoDBError> {
        if matches!(candidate, Some("")) {
            return Err(StorageError("raft voted_for cannot be empty".into()).into());
        }
        self.hard_state_store.set_voted_for(candidate)?;
        self.hard_state = self.hard_state_store.state().clone();
        Ok(())
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
        self.wal.sync()
    }

    fn refresh_progress_from_wal_tail(&mut self) {
        let last_raft_index = self.wal.last_raft_index();
        self.progress.commit_index = last_raft_index;
        self.progress.last_applied = last_raft_index;
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::wal::{WalReader, WalRecord};

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
}
