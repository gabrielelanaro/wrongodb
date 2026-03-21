use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::storage::wal::GlobalWal;
use crate::txn::{Timestamp, TxnId, TxnLogOp};
use crate::WrongoDBError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageSyncPolicy {
    #[cfg(test)]
    Buffered,
    Sync,
}

/// Local storage durability backend for WAL markers and checkpoint truncation.
#[derive(Debug)]
pub(crate) enum DurabilityBackend {
    Disabled,
    LocalWal(LocalWalDurabilityBackend),
}

#[derive(Debug)]
pub(crate) struct LocalWalDurabilityBackend {
    wal: Arc<Mutex<GlobalWal>>,
}

impl DurabilityBackend {
    pub(crate) fn open(base_path: &Path, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        if !wal_enabled {
            return Ok(Self::Disabled);
        }

        Ok(Self::LocalWal(LocalWalDurabilityBackend::open(base_path)?))
    }

    pub(crate) fn log_transaction_commit(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        ops: &[TxnLogOp],
        sync_policy: StorageSyncPolicy,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::LocalWal(backend) => {
                backend.log_transaction_commit(txn_id, commit_ts, ops, sync_policy)
            }
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        match self {
            Self::Disabled => false,
            Self::LocalWal(_) => true,
        }
    }

    pub(crate) fn record_checkpoint(
        &self,
        sync_policy: StorageSyncPolicy,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::LocalWal(backend) => backend.record_checkpoint(sync_policy),
        }
    }

    pub(crate) fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::LocalWal(backend) => backend.truncate_to_checkpoint(),
        }
    }
}

impl LocalWalDurabilityBackend {
    fn open(base_path: &Path) -> Result<Self, WrongoDBError> {
        Ok(Self {
            wal: Arc::new(Mutex::new(GlobalWal::open_or_create(base_path)?)),
        })
    }

    fn log_transaction_commit(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        ops: &[TxnLogOp],
        sync_policy: StorageSyncPolicy,
    ) -> Result<(), WrongoDBError> {
        let mut wal = self.wal.lock();
        wal.log_txn_commit(txn_id, commit_ts, ops)?;
        if sync_policy == StorageSyncPolicy::Sync {
            wal.sync()?;
        }
        Ok(())
    }

    fn record_checkpoint(&self, sync_policy: StorageSyncPolicy) -> Result<(), WrongoDBError> {
        let mut wal = self.wal.lock();
        wal.log_checkpoint()?;
        if sync_policy == StorageSyncPolicy::Sync {
            wal.sync()?;
        }
        Ok(())
    }

    fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        self.wal.lock().truncate_to_checkpoint()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::wal::{GlobalWal, WalFileReader, WalReader, WalRecord};

    #[test]
    fn disabled_backend_is_noop_and_reports_disabled() {
        let dir = tempdir().unwrap();
        let backend = DurabilityBackend::open(dir.path(), false).unwrap();

        assert!(!backend.is_enabled());
        backend
            .log_transaction_commit(
                7,
                7,
                &[TxnLogOp::Put {
                    uri: "table:users".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
                StorageSyncPolicy::Buffered,
            )
            .unwrap();
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let backend = DurabilityBackend::open(dir.path(), true).unwrap();

        backend
            .log_transaction_commit(
                7,
                7,
                &[TxnLogOp::Put {
                    uri: "table:users".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
                StorageSyncPolicy::Sync,
            )
            .unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let mut reader = WalFileReader::open(wal_path).unwrap();
        let mut found_commit = false;
        while let Some((_header, record)) = reader.read_record().unwrap() {
            if matches!(
                record,
                WalRecord::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                    ..
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
        let backend = DurabilityBackend::open(dir.path(), true).unwrap();

        backend
            .log_transaction_commit(
                1,
                1,
                &[TxnLogOp::Put {
                    uri: "table:users".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
                StorageSyncPolicy::Sync,
            )
            .unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        backend.record_checkpoint(StorageSyncPolicy::Sync).unwrap();
        backend.truncate_to_checkpoint().unwrap();

        let after_truncate = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after_truncate <= 512);
    }

    #[test]
    fn wal_enabled_startup_creates_global_wal_file() {
        let dir = tempdir().unwrap();
        let wal_path = GlobalWal::path_for_db(dir.path());
        assert!(!wal_path.exists());

        let _backend = DurabilityBackend::open(dir.path(), true).unwrap();

        assert!(wal_path.exists());
    }
}
