use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::durability::DurableOp;
use crate::storage::wal::GlobalWal;
use crate::txn::{NoopRecoveryUnit, RecoveryUnit, WalRecoveryUnit};
use crate::WrongoDBError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurabilityGuarantee {
    Buffered,
    Sync,
}

/// Local durability backend for standalone storage recovery and WAL markers.
///
/// RAFT replication is handled in [`crate::replication::ReplicationCoordinator`].
/// This backend intentionally stays focused on local durability only.
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

    pub(crate) fn record(
        &self,
        op: DurableOp,
        guarantee: DurabilityGuarantee,
    ) -> Result<(), WrongoDBError> {
        match self {
            Self::Disabled => Ok(()),
            Self::LocalWal(backend) => backend.record(op, guarantee),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        match self {
            Self::Disabled => false,
            Self::LocalWal(_) => true,
        }
    }

    pub(crate) fn new_recovery_unit(&self) -> Arc<dyn RecoveryUnit> {
        match self {
            Self::Disabled => Arc::new(NoopRecoveryUnit),
            Self::LocalWal(backend) => backend.new_recovery_unit(),
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

    fn record(&self, op: DurableOp, guarantee: DurabilityGuarantee) -> Result<(), WrongoDBError> {
        let mut wal = self.wal.lock();
        match op {
            DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                wal.log_put(&store_name, &key, &value, txn_id, 0)?;
            }
            DurableOp::Delete {
                store_name,
                key,
                txn_id,
            } => {
                wal.log_delete(&store_name, &key, txn_id, 0)?;
            }
            DurableOp::TxnCommit { txn_id, commit_ts } => {
                wal.log_txn_commit(txn_id, commit_ts, 0)?;
            }
            DurableOp::TxnAbort { txn_id } => {
                wal.log_txn_abort(txn_id, 0)?;
            }
            DurableOp::Checkpoint => {
                wal.log_checkpoint(0)?;
            }
        }
        if guarantee == DurabilityGuarantee::Sync {
            wal.sync()?;
        }
        Ok(())
    }

    fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        self.wal.lock().truncate_to_checkpoint()
    }

    fn new_recovery_unit(&self) -> Arc<dyn RecoveryUnit> {
        Arc::new(WalRecoveryUnit::new(self.wal.clone()))
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
            .record(
                DurableOp::Put {
                    store_name: "users.main.wt".to_string(),
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    txn_id: crate::txn::TXN_NONE,
                },
                DurabilityGuarantee::Buffered,
            )
            .unwrap();
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let backend = DurabilityBackend::open(dir.path(), true).unwrap();

        backend
            .record(
                DurableOp::TxnCommit {
                    txn_id: 7,
                    commit_ts: 7,
                },
                DurabilityGuarantee::Sync,
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
                    commit_ts: 7
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
            .record(
                DurableOp::TxnCommit {
                    txn_id: 1,
                    commit_ts: 1,
                },
                DurabilityGuarantee::Sync,
            )
            .unwrap();

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        backend
            .record(DurableOp::Checkpoint, DurabilityGuarantee::Sync)
            .unwrap();
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
