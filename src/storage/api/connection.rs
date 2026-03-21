use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::api::session::Session;
use crate::storage::durability::DurabilityBackend;
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_catalog::MetadataCatalog;
use crate::storage::recovery::{recover_from_wal, RecoveryExecutor};
use crate::storage::table::Table;
use crate::storage::wal::{GlobalWal, WalFileReader};
use crate::txn::{GlobalTxnState, TransactionManager};
use crate::WrongoDBError;

/// Configuration used when opening a [`Connection`].
///
/// The public connection constructor stays intentionally small. This struct is
/// where callers choose storage durability mode without having to know about
/// the internal storage and recovery wiring.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Enables WAL recording for storage writes.
    ///
    /// WAL replay on startup is always attempted if `global.wal` exists. This
    /// flag only controls whether new local writes append to the WAL after the
    /// connection is opened.
    pub wal_enabled: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self { wal_enabled: true }
    }
}

impl ConnectionConfig {
    /// Create a config from explicit storage durability policy inputs.
    ///
    /// Callers provide the storage-affecting inputs at the construction site so
    /// the local durability mode stays visible instead of being implied by a
    /// hidden default.
    pub fn new(wal_enabled: bool) -> Self {
        Self { wal_enabled }
    }

    /// Override whether WAL recording is enabled.
    ///
    /// This stays on the config builder so durability mode is chosen before the
    /// engine is opened, not mutated later at runtime.
    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
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
    metadata_catalog: Arc<MetadataCatalog>,
    table_handles: Arc<HandleCache<String, RwLock<Table>>>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
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
        let ConnectionConfig { wal_enabled } = config;
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_handles = Arc::new(HandleCache::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));
        let recovery_executor = Arc::new(RecoveryExecutor::new(
            base_path.clone(),
            metadata_catalog.clone(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));

        if let Some(recovery_reader) = open_recovery_reader(&base_path) {
            recover_from_wal(recovery_executor, recovery_reader)?;
        }

        let durability_backend = Arc::new(DurabilityBackend::open(&base_path, wal_enabled)?);

        Ok(Self {
            base_path,
            metadata_catalog,
            table_handles,
            transaction_manager,
            durability_backend,
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
            self.table_handles.clone(),
            self.metadata_catalog.clone(),
            self.transaction_manager.clone(),
            self.durability_backend.clone(),
        )
    }

    pub(crate) fn metadata_catalog(&self) -> Arc<MetadataCatalog> {
        self.metadata_catalog.clone()
    }

    pub(crate) fn base_path(&self) -> &Path {
        &self.base_path
    }
}

fn open_recovery_reader(base_path: &Path) -> Option<WalFileReader> {
    let wal_path = GlobalWal::path_for_db(base_path);
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
