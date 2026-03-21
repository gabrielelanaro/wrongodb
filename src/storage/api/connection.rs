use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::durability::{DurabilityBackend, StoreCommandApplier};
use crate::recovery::recover_from_wal;
use crate::replication::RaftMode;
use crate::schema::SchemaCatalog;
use crate::storage::api::session::Session;
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_catalog::MetadataCatalog;
use crate::storage::table::Table;
use crate::storage::wal::{GlobalWal, WalFileReader};
use crate::txn::{GlobalTxnState, TransactionManager};
use crate::WrongoDBError;

/// Configuration used when opening a [`Connection`].
///
/// The public connection constructor stays intentionally small. This struct is
/// where callers choose the coarse durability mode without having to know
/// about the internal storage, recovery, or replication wiring.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Enables local durability and crash recovery.
    ///
    /// This is separate from `raft_mode` because standalone durability and
    /// clustered replication are distinct concerns.
    pub wal_enabled: bool,
    /// Selects standalone or clustered write replication behavior.
    pub raft_mode: RaftMode,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            raft_mode: RaftMode::Standalone,
        }
    }
}

impl ConnectionConfig {
    /// Create a config from explicit durability and replication policy inputs.
    ///
    /// Callers provide the policy-affecting inputs at the construction site so
    /// the database startup mode stays visible instead of being implied by a
    /// hidden default.
    pub fn new(wal_enabled: bool, raft_mode: RaftMode) -> Self {
        Self {
            wal_enabled,
            raft_mode,
        }
    }

    /// Override whether local durability is enabled.
    ///
    /// This stays on the config builder so durability mode is chosen before the
    /// engine is opened, not mutated later at runtime.
    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Override the replication mode.
    ///
    /// This exists because replication mode affects startup wiring, not just
    /// request-time behavior.
    pub fn raft_mode(mut self, mode: RaftMode) -> Self {
        self.raft_mode = mode;
        self
    }
}

/// Top-level database handle that owns shared engine state.
///
/// `Connection` exists so sessions can stay cheap and request-scoped. Long-lived
/// components such as schema metadata, open store handles, MVCC state, and
/// local durability machinery live here once, and every [`Session`](crate::Session)
/// borrows that shared infrastructure instead of rebuilding it.
///
/// In other words: `Connection` owns the engine, [`Session`](crate::Session)
/// owns request-local state.
pub struct Connection {
    base_path: PathBuf,
    metadata_catalog: Arc<MetadataCatalog>,
    schema_catalog: Arc<SchemaCatalog>,
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
        let ConnectionConfig {
            wal_enabled,
            raft_mode,
        } = config;
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
        let schema_catalog = Arc::new(SchemaCatalog::new(
            base_path.clone(),
            metadata_catalog.clone(),
        ));
        let applier = Arc::new(StoreCommandApplier::new(
            base_path.clone(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));

        if wal_enabled {
            if let Some(recovery_reader) = open_recovery_reader(&base_path) {
                recover_from_wal(applier.clone(), recovery_reader)?;
            }
        }

        let standalone_local_wal = wal_enabled && matches!(&raft_mode, RaftMode::Standalone);
        let durability_backend =
            Arc::new(DurabilityBackend::open(&base_path, standalone_local_wal)?);

        Ok(Self {
            base_path,
            metadata_catalog,
            schema_catalog,
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

    pub(crate) fn schema_catalog(&self) -> Arc<SchemaCatalog> {
        self.schema_catalog.clone()
    }

    pub(crate) fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub(crate) fn table_handles(&self) -> Arc<HandleCache<String, RwLock<Table>>> {
        self.table_handles.clone()
    }

    pub(crate) fn durability_backend(&self) -> Arc<DurabilityBackend> {
        self.durability_backend.clone()
    }

    pub(crate) fn transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
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
