use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::storage::wal::{LogFile, WalFileReader};
use crate::txn::{Timestamp, TxnId, TxnLogOp};
use crate::WrongoDBError;

// ============================================================================
// Constants
// ============================================================================

const LOG_FILE_NAME: &str = "global.wal";

// ============================================================================
// Public Configuration Types
// ============================================================================

/// Log sync method exposed on the public connection config.
///
/// `Fsync` means "flush log data and required metadata to stable storage",
/// while `Dsync` means "flush log data but avoid extra metadata writes when
/// possible".
///
/// This increment keeps the current single-file logging implementation, so
/// `Fsync` and `Dsync` currently share the same `sync_all` behavior. The enum
/// exists now to align the public config with the WT-style direction without
/// committing the public API to a second breaking config change later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSyncMethod {
    /// Flush log data and the metadata needed to make the write durable.
    Fsync,
    /// Flush log data while avoiding extra metadata writes when possible.
    Dsync,
    /// Do not force a sync on each commit.
    None,
}

/// Per-transaction log sync policy used by [`LoggingConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionSyncConfig {
    /// Whether each committed transaction should explicitly sync the log.
    pub enabled: bool,
    /// Which sync primitive to use when transaction sync is enabled.
    pub method: LogSyncMethod,
}

impl Default for TransactionSyncConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: LogSyncMethod::Fsync,
        }
    }
}

/// Runtime logging configuration for a [`crate::Connection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoggingConfig {
    /// Whether newly opened connections append commit and checkpoint records.
    ///
    /// Startup recovery still runs if a log file is already present.
    pub enabled: bool,
    /// Per-transaction sync policy for commit records.
    pub transaction_sync: TransactionSyncConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            transaction_sync: TransactionSyncConfig::default(),
        }
    }
}

// ============================================================================
// Internal Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageSyncPolicy {
    Buffered,
    Sync,
}

impl From<TransactionSyncConfig> for StorageSyncPolicy {
    fn from(config: TransactionSyncConfig) -> Self {
        if config.enabled && config.method != LogSyncMethod::None {
            Self::Sync
        } else {
            Self::Buffered
        }
    }
}

/// Connection-owned runtime log manager for storage commits and checkpoints.
///
/// `LogManager` is intentionally concrete. It owns the runtime logging state
/// used by sessions, while startup recovery stays a separate concern.
#[derive(Debug)]
pub(crate) struct LogManager {
    enabled: bool,
    log_file: Option<Mutex<LogFile>>,
    sync_policy: StorageSyncPolicy,
}

impl LogManager {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn open(base_path: &Path, config: &LoggingConfig) -> Result<Self, WrongoDBError> {
        let log_file = if config.enabled {
            Some(Mutex::new(LogFile::open_or_create(log_file_path(
                base_path,
            ))?))
        } else {
            None
        };

        Ok(Self {
            enabled: config.enabled,
            log_file,
            sync_policy: StorageSyncPolicy::from(config.transaction_sync),
        })
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            log_file: None,
            sync_policy: StorageSyncPolicy::from(TransactionSyncConfig::default()),
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Whether this manager was opened with runtime logging enabled.
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Append one committed transaction record to the runtime log.
    ///
    /// This is the storage-facing durability hook used by [`Session`](crate::Session)
    /// right before MVCC state is made visible. When logging is disabled this is
    /// a no-op.
    pub(crate) fn log_transaction_commit(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        ops: &[TxnLogOp],
    ) -> Result<(), WrongoDBError> {
        let Some(log_file) = &self.log_file else {
            return Ok(());
        };

        let mut log_file = log_file.lock();
        log_file.log_txn_commit(txn_id, commit_ts, ops)?;
        if self.sync_policy == StorageSyncPolicy::Sync {
            log_file.sync()?;
        }
        Ok(())
    }

    /// Append one checkpoint record to the runtime log.
    ///
    /// The caller still owns checkpoint policy, including store flushing and the
    /// decision about whether truncation is currently allowed.
    pub(crate) fn log_checkpoint(&self) -> Result<(), WrongoDBError> {
        let Some(log_file) = &self.log_file else {
            return Ok(());
        };

        let mut log_file = log_file.lock();
        log_file.log_checkpoint()?;
        if self.sync_policy == StorageSyncPolicy::Sync {
            log_file.sync()?;
        }
        Ok(())
    }

    /// Truncate the runtime log after a completed checkpoint.
    ///
    /// When logging is disabled this is a no-op.
    pub(crate) fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        let Some(log_file) = &self.log_file else {
            return Ok(());
        };

        log_file.lock().truncate_to_checkpoint()
    }
}

// ============================================================================
// Recovery Helpers
// ============================================================================

pub(crate) fn open_recovery_reader_if_present(base_path: &Path) -> Option<WalFileReader> {
    let wal_path = log_file_path(base_path);
    if !wal_path.exists() {
        return None;
    }

    match WalFileReader::open(&wal_path) {
        Ok(reader) => Some(reader),
        Err(err) => {
            eprintln!("Skipping global WAL recovery (failed to open WAL): {err}");
            None
        }
    }
}

// ============================================================================
// Private Functions
// ============================================================================

fn log_file_path(base_path: &Path) -> PathBuf {
    base_path.join(LOG_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::reserved_store::FIRST_DYNAMIC_STORE_ID;
    use crate::storage::wal::{WalReader, WalRecord};

    const TEST_STORE_ID: u64 = FIRST_DYNAMIC_STORE_ID;

    fn log_path(base_path: &Path) -> PathBuf {
        base_path.join(LOG_FILE_NAME)
    }

    #[test]
    fn disabled_manager_is_noop_and_reports_disabled() {
        let manager = LogManager::disabled();

        assert!(!manager.is_enabled());
        manager
            .log_transaction_commit(
                7,
                7,
                &[TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            )
            .unwrap();
    }

    #[test]
    fn commit_marker_is_persisted_after_sync() {
        let dir = tempdir().unwrap();
        let manager = LogManager::open(dir.path(), &LoggingConfig::default()).unwrap();

        manager
            .log_transaction_commit(
                7,
                7,
                &[TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            )
            .unwrap();

        let mut reader = WalFileReader::open(log_path(dir.path())).unwrap();
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
        let manager = LogManager::open(dir.path(), &LoggingConfig::default()).unwrap();

        manager
            .log_transaction_commit(
                1,
                1,
                &[TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            )
            .unwrap();

        let wal_path = log_path(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        manager.log_checkpoint().unwrap();
        manager.truncate_to_checkpoint().unwrap();

        let after_truncate = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after_truncate <= 512);
    }

    #[test]
    fn logging_enabled_startup_creates_global_wal_file() {
        let dir = tempdir().unwrap();
        let wal_path = log_path(dir.path());
        assert!(!wal_path.exists());

        let _manager = LogManager::open(dir.path(), &LoggingConfig::default()).unwrap();

        assert!(wal_path.exists());
    }
}
