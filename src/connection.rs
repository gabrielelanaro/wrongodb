use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::datahandle_cache::DataHandleCache;
use crate::session::Session;
use crate::storage::wal::{wal_path_from_base_path, CheckpointSignal, GlobalWal, WalReader, WalRecord};
use crate::txn::GlobalTxnState;
use crate::WrongoDBError;
use parking_lot::Mutex;

pub struct ConnectionConfig {
    pub wal_enabled: bool,
    pub checkpoint_wait_secs: Option<u64>,
    pub checkpoint_log_size_bytes: Option<u64>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            checkpoint_wait_secs: Some(60),
            checkpoint_log_size_bytes: Some(64 * 1024 * 1024),
        }
    }
}

impl ConnectionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn checkpoint_wait_secs(mut self, wait_secs: Option<u64>) -> Self {
        self.checkpoint_wait_secs = wait_secs;
        self
    }

    pub fn checkpoint_log_size_bytes(mut self, bytes: Option<u64>) -> Self {
        self.checkpoint_log_size_bytes = bytes;
        self
    }

    pub fn disable_auto_checkpoint(mut self) -> Self {
        self.checkpoint_wait_secs = None;
        self.checkpoint_log_size_bytes = None;
        self
    }
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    wal: Option<Arc<GlobalWal>>,
    checkpoint_wait_secs: Option<u64>,
    checkpoint_log_size_bytes: Option<u64>,
    checkpoint_mutex: Arc<Mutex<()>>,
    checkpoint_manager: Option<CheckpointManager>,
    global_txn: Arc<GlobalTxnState>,
}

struct CheckpointManager {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    signal: Option<CheckpointSignal>,
}

impl Drop for CheckpointManager {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(signal) = &self.signal {
            signal.notify();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) struct CheckpointContext {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal: Option<Arc<GlobalWal>>,
    checkpoint_mutex: Arc<Mutex<()>>,
    global_txn: Arc<GlobalTxnState>,
}

impl CheckpointContext {
    pub(crate) fn new(
        base_path: PathBuf,
        dhandle_cache: Arc<DataHandleCache>,
        wal: Option<Arc<GlobalWal>>,
        checkpoint_mutex: Arc<Mutex<()>>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            base_path,
            dhandle_cache,
            wal,
            checkpoint_mutex,
            global_txn,
        }
    }
}

pub(crate) fn checkpoint_all_with_context(ctx: &CheckpointContext) -> Result<(), WrongoDBError> {
    let _guard = ctx.checkpoint_mutex.lock();
    let snapshot = ctx.global_txn.checkpoint_snapshot();
    let expected_offset = ctx.wal.as_ref().map(|wal| wal.last_offset());
    let collection_names = list_collections(&ctx.base_path)?;

    for name in collection_names {
        let uri = format!("table:{}", name);
        let table = ctx.dhandle_cache.get_or_open_primary(
            &uri,
            &name,
            &ctx.base_path,
            ctx.wal.clone(),
            ctx.global_txn.clone(),
        )?;
        let mut guard = table.write();
        guard.checkpoint_with_snapshot(&snapshot)?;
    }

    if let Some(wal) = ctx.wal.as_ref() {
        if snapshot.active.is_empty() {
            if let Some(expected) = expected_offset {
                let _ = wal.try_advance_checkpoint(expected)?;
            }
        }
    }

    Ok(())
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("base_path", &self.base_path)
            .field("wal_enabled", &self.wal_enabled)
            .field("checkpoint_wait_secs", &self.checkpoint_wait_secs)
            .field("checkpoint_log_size_bytes", &self.checkpoint_log_size_bytes)
            .finish()
    }
}

impl Connection {
    pub fn open<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
    {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        let global_txn = Arc::new(GlobalTxnState::new());
        let wal = if config.wal_enabled {
            Some(Arc::new(GlobalWal::open_or_create(&base_path, 4096)?))
        } else {
            None
        };

        if let Some(wal) = wal.as_ref() {
            wal.set_log_size_threshold(config.checkpoint_log_size_bytes);
        }

        let checkpoint_mutex = Arc::new(Mutex::new(()));

        let mut conn = Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new()),
            wal_enabled: config.wal_enabled,
            wal,
            checkpoint_wait_secs: config.checkpoint_wait_secs,
            checkpoint_log_size_bytes: config.checkpoint_log_size_bytes,
            checkpoint_mutex,
            checkpoint_manager: None,
            global_txn,
        };

        if conn.wal_enabled {
            if let Err(err) = conn.recover_from_wal() {
                eprintln!("WAL recovery failed: {}. Database may be inconsistent.", err);
            }
        }

        conn.start_checkpoint_manager();

        Ok(conn)
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.base_path.clone(),
            self.wal.clone(),
            self.global_txn.clone(),
            self.checkpoint_mutex.clone(),
        )
    }

    pub fn checkpoint_all(&self) -> Result<(), WrongoDBError> {
        let ctx = self.checkpoint_context();
        checkpoint_all_with_context(&ctx)
    }

    fn checkpoint_context(&self) -> CheckpointContext {
        CheckpointContext {
            base_path: self.base_path.clone(),
            dhandle_cache: self.dhandle_cache.clone(),
            wal: self.wal.clone(),
            checkpoint_mutex: self.checkpoint_mutex.clone(),
            global_txn: self.global_txn.clone(),
        }
    }

    fn start_checkpoint_manager(&mut self) {
        let wait_secs = self.checkpoint_wait_secs;
        let log_size = self.checkpoint_log_size_bytes;
        if wait_secs.is_none() && log_size.is_none() {
            return;
        }

        let signal = self
            .wal
            .as_ref()
            .map(|wal| wal.checkpoint_signal())
            .unwrap_or_else(CheckpointSignal::new);
        let thread_signal = signal.clone();
        let ctx = self.checkpoint_context();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();

        let handle = thread::spawn(move || {
            let wait = wait_secs.map(Duration::from_secs).unwrap_or(Duration::from_secs(3600));
            loop {
                if stop_thread.load(Ordering::Acquire) {
                    break;
                }
                let signaled = thread_signal.wait_timeout(wait);
                if stop_thread.load(Ordering::Acquire) {
                    break;
                }
                if signaled || wait_secs.is_some() {
                    if let Err(err) = checkpoint_all_with_context(&ctx) {
                        eprintln!("auto-checkpoint failed: {}", err);
                    }
                }
            }
        });

        self.checkpoint_manager = Some(CheckpointManager {
            stop,
            handle: Some(handle),
            signal: Some(signal),
        });
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    fn recover_from_wal(&self) -> Result<(), WrongoDBError> {
        let wal_path = wal_path_from_base_path(&self.base_path);
        if !wal_path.exists() {
            return Ok(());
        }

        let mut wal_reader = WalReader::open(&wal_path)
            .map_err(|e| crate::core::errors::StorageError(format!("failed to open WAL: {e}")))?;

        let mut txn_table = crate::txn::recovery::RecoveryTxnTable::new();

        while let Some((_header, record)) = wal_reader
            .read_record()
            .map_err(|e| crate::core::errors::StorageError(format!("WAL read failed: {e}")))? {
            txn_table.process_record(&record);
        }
        txn_table.finalize_pending();

        let mut wal_reader = WalReader::open(&wal_path)
            .map_err(|e| crate::core::errors::StorageError(format!("failed to reopen WAL: {e}")))?;

        while let Some((_header, record)) = wal_reader
            .read_record()
            .map_err(|e| crate::core::errors::StorageError(format!("WAL read failed: {e}")))? {
            if !txn_table.should_apply(&record) {
                continue;
            }

            match record {
                WalRecord::Put { uri, key, value, .. } => {
                    if let Some(table) = self.open_table_for_uri(&uri)? {
                        let mut guard = table.write();
                        guard.apply_recovery_put(&key, &value)?;
                    }
                }
                WalRecord::Delete { uri, key, .. } => {
                    if let Some(table) = self.open_table_for_uri(&uri)? {
                        let mut guard = table.write();
                        guard.apply_recovery_delete(&key)?;
                    }
                }
                WalRecord::Checkpoint { .. } => {}
                WalRecord::TxnCommit { .. } => {}
                WalRecord::TxnAbort { .. } => {}
            }
        }

        Ok(())
    }

    fn open_table_for_uri(&self, uri: &str) -> Result<Option<Arc<parking_lot::RwLock<crate::storage::table::Table>>>, WrongoDBError> {
        if uri.starts_with("table:") {
            let collection = &uri[6..];
            let table = self.dhandle_cache.get_or_open_primary(
                uri,
                collection,
                &self.base_path,
                self.wal.clone(),
                self.global_txn.clone(),
            )?;
            return Ok(Some(table));
        }

        if uri.starts_with("index:") {
            let (collection, index_name) = parse_index_uri(uri)?;
            let table = self.dhandle_cache.get_or_open_primary(
                &format!("table:{}", collection),
                collection,
                &self.base_path,
                self.wal.clone(),
                self.global_txn.clone(),
            )?;
            let index_table = {
                let table_guard = table.read();
                let catalog = match table_guard.index_catalog() {
                    Some(cat) => cat,
                    None => return Ok(None),
                };
                match catalog.index_handle(index_name) {
                    Some(idx) => idx,
                    None => return Ok(None),
                }
            };
            return Ok(Some(index_table));
        }

        Ok(None)
    }
}

fn parse_index_uri(uri: &str) -> Result<(&str, &str), WrongoDBError> {
    if !uri.starts_with("index:") {
        return Err(crate::core::errors::StorageError(format!("invalid index URI: {}", uri)).into());
    }
    let rest = &uri[6..];
    let mut parts = rest.splitn(2, ':');
    let collection = parts.next().unwrap_or("");
    let index = parts.next().unwrap_or("");
    if collection.is_empty() || index.is_empty() {
        return Err(crate::core::errors::StorageError(format!("invalid index URI: {}", uri)).into());
    }
    Ok((collection, index))
}

fn list_collections(base_path: &Path) -> Result<Vec<String>, WrongoDBError> {
    let mut names = Vec::new();
    for entry in fs::read_dir(base_path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = match file_name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some(name) = file_name.strip_suffix(".main.wt") {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}
