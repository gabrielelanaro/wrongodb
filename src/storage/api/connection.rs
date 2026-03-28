use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::api::session::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::history::HistoryStore;
use crate::storage::log_manager::{open_recovery_reader_if_present, LogManager};
pub use crate::storage::log_manager::{LogSyncMethod, LoggingConfig, TransactionSyncConfig};
use crate::storage::metadata_store::{reseed_next_store_id_from_metadata, MetadataStore};
use crate::storage::recovery::recover_from_wal;
use crate::storage::reserved_store::HS_STORE_NAME;
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;

/// Configuration used when opening a [`Connection`].
///
/// The public connection constructor stays intentionally small. This struct is
/// where callers choose storage logging policy without having to know about
/// the internal storage and recovery wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnectionConfig {
    /// Runtime logging configuration for storage commits and checkpoints.
    pub logging: LoggingConfig,
}

impl ConnectionConfig {
    /// Create a config from the default logging policy.
    ///
    /// The returned config preserves the current behavior: logging enabled and
    /// synchronous commit flushes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override whether runtime logging is enabled.
    ///
    /// Startup recovery still runs if a log file is present. This flag only
    /// controls whether newly opened connections append new log records.
    pub fn logging_enabled(mut self, enabled: bool) -> Self {
        self.logging.enabled = enabled;
        self
    }

    /// Override whether commits should sync the log by default.
    ///
    /// When disabled, commits still append log records if logging is enabled,
    /// but they do not force the log to stable storage on each commit.
    pub fn transaction_sync_enabled(mut self, enabled: bool) -> Self {
        self.logging.transaction_sync.enabled = enabled;
        self
    }

    /// Override the configured log sync method.
    ///
    /// `Fsync` and `Dsync` are currently implemented the same way in the local
    /// log manager, but both names are exposed so the public API already
    /// matches the intended WT-style configuration shape.
    pub fn transaction_sync_method(mut self, method: LogSyncMethod) -> Self {
        self.logging.transaction_sync.method = method;
        self
    }
}

/// Top-level database handle that owns shared engine state.
///
/// `Connection` exists so sessions can stay cheap and request-scoped. Long-lived
/// components such as storage metadata, open store handles, MVCC state, and
/// local durability machinery live here once, and every [`Session`](crate::Session)
/// borrows that shared infrastructure instead of rebuilding it.
///
/// In other words: `Connection` owns the engine, [`Session`](crate::Session)
/// owns request-local state.
pub struct Connection {
    base_path: PathBuf,
    metadata_store: Arc<MetadataStore>,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    history_store: Arc<RwLock<HistoryStore>>,
    global_txn: Arc<GlobalTxnState>,
    log_manager: Arc<LogManager>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl Connection {
    /// Open or create a database rooted at `path`.
    ///
    /// This is the only supported way to construct the database because it
    /// wires recovery, local durability, schema state, and shared caches
    /// together consistently before any session is opened.
    ///
    /// The constructor does this eagerly so callers never have to reason about
    /// partially initialized engine state.
    pub fn open<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let ConnectionConfig { logging } = config;
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        let global_txn = Arc::new(GlobalTxnState::new());
        let store_handles = Arc::new(HandleCache::new());
        let log_manager = Arc::new(LogManager::open(&base_path, &logging)?);
        let history_store = Arc::new(RwLock::new(HistoryStore::open_or_create(
            base_path.join(HS_STORE_NAME),
        )?));
        let metadata_store = Arc::new(MetadataStore::new(
            base_path.clone(),
            store_handles.clone(),
            global_txn.clone(),
            log_manager.clone(),
        )?);
        if let Some(recovery_reader) = open_recovery_reader_if_present(&base_path) {
            let mut history_store_guard = history_store.write();
            recover_from_wal(
                &base_path,
                metadata_store.as_ref(),
                store_handles.as_ref(),
                &mut history_store_guard,
                global_txn.as_ref(),
                recovery_reader,
            )?;
        }

        reseed_next_store_id_from_metadata(metadata_store.as_ref())?;

        Ok(Self {
            base_path,
            metadata_store,
            store_handles,
            history_store,
            global_txn,
            log_manager,
        })
    }

    /// Open a fresh request-scoped [`Session`](crate::Session).
    ///
    /// Sessions are intentionally lightweight. They hold mutable per-request
    /// state such as the active transaction and bound cursors, while
    /// `Connection` retains the shared engine state.
    ///
    /// This separation is the reason session creation is cheap and why multiple
    /// sessions can share one opened database safely.
    pub fn open_session(&self) -> Session {
        Session::new(
            self.base_path.clone(),
            self.store_handles.clone(),
            self.metadata_store.clone(),
            self.history_store.clone(),
            self.global_txn.clone(),
            self.log_manager.clone(),
        )
    }

    pub(crate) fn metadata_store(&self) -> Arc<MetadataStore> {
        self.metadata_store.clone()
    }

    pub(crate) fn base_path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::history::HistoryEntry;
    use crate::storage::mvcc::UpdateType;
    use crate::storage::table::{open_or_create_btree, reconcile_for_checkpoint};

    fn commit_put(
        conn: &Connection,
        handle: &Arc<RwLock<BTreeCursor>>,
        store_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> u64 {
        let mut txn = conn.global_txn.begin_snapshot_txn();
        let commit_ts = txn.id();
        handle.write().put(store_id, key, value, &mut txn).unwrap();
        conn.log_manager
            .log_transaction_commit(txn.id(), commit_ts, txn.log_ops())
            .unwrap();
        txn.commit(conn.global_txn.as_ref()).unwrap();
        commit_ts
    }

    #[test]
    fn recovery_rebuilds_history_store_after_reconcile_without_checkpoint() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("db");
        let (store_id, ts1, ts2);

        {
            let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
            let mut session = conn.open_session();
            session.create_table("table:test", Vec::new()).unwrap();

            let (dynamic_store_id, store_name) = conn
                .metadata_store
                .all_stores_with_id()
                .unwrap()
                .into_iter()
                .find(|(_, store_name)| store_name == "test.main.wt")
                .unwrap();
            let handle = conn
                .store_handles
                .get_or_try_insert_with(store_name.clone(), |store_name| {
                    let path = conn.base_path.join(store_name);
                    Ok(RwLock::new(open_or_create_btree(path)?))
                })
                .unwrap();

            let first_ts = commit_put(&conn, &handle, dynamic_store_id, b"k1", b"v1");
            let second_ts = commit_put(&conn, &handle, dynamic_store_id, b"k1", b"v2");

            let mut history_store = conn.history_store.write();
            reconcile_for_checkpoint(
                &mut handle.write(),
                conn.global_txn.as_ref(),
                dynamic_store_id,
                Some(&mut history_store),
            )
            .unwrap();

            store_id = dynamic_store_id;
            ts1 = first_ts;
            ts2 = second_ts;
        }

        let reopened = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        let rebuilt = reopened
            .history_store
            .write()
            .find_version(store_id, b"k1", ts1)
            .unwrap();

        assert_eq!(
            rebuilt,
            Some(HistoryEntry {
                store_id,
                key: b"k1".to_vec(),
                start_ts: ts1,
                stop_ts: ts2,
                update_type: UpdateType::Standard,
                data: b"v1".to_vec(),
            })
        );
    }
}
