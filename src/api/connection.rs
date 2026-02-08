use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::api::data_handle_cache::DataHandleCache;
use crate::api::session::Session;
use crate::core::errors::StorageError;
use crate::core::lock_stats::set_lock_stats_enabled;
use crate::storage::btree::BTree;
use crate::storage::wal::{GlobalWal, RecoveryError, WalReader, WalRecord};
use crate::txn::GlobalTxnState;
use crate::txn::RecoveryTxnTable;
use crate::txn::TXN_NONE;
use crate::WrongoDBError;

pub struct ConnectionConfig {
    pub wal_enabled: bool,
    /// WAL fsync policy:
    /// - 0 = sync on every commit (strict durability)
    /// - N > 0 = at most one sync every N milliseconds (group sync)
    pub wal_sync_interval_ms: u64,
    pub lock_stats_enabled: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            wal_sync_interval_ms: 100,
            lock_stats_enabled: false,
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

    pub fn wal_sync_interval_ms(mut self, interval_ms: u64) -> Self {
        self.wal_sync_interval_ms = interval_ms;
        self
    }

    pub fn wal_sync_immediate(mut self) -> Self {
        self.wal_sync_interval_ms = 0;
        self
    }

    pub fn lock_stats_enabled(mut self, enabled: bool) -> Self {
        self.lock_stats_enabled = enabled;
        self
    }
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    wal_sync_interval_ms: u64,
    lock_stats_enabled: bool,
    wal_last_sync_ms: Arc<AtomicU64>,
    global_wal: Option<Arc<Mutex<GlobalWal>>>,
    global_txn: Arc<GlobalTxnState>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("base_path", &self.base_path)
            .field("wal_enabled", &self.wal_enabled)
            .field("wal_sync_interval_ms", &self.wal_sync_interval_ms)
            .field("lock_stats_enabled", &self.lock_stats_enabled)
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
        set_lock_stats_enabled(config.lock_stats_enabled);
        let global_txn = Arc::new(GlobalTxnState::new());
        let global_wal = if config.wal_enabled {
            warn_legacy_per_table_wal_files(&base_path);
            recover_global_wal(&base_path, global_txn.clone())?;
            Some(Arc::new(Mutex::new(GlobalWal::open_or_create(&base_path)?)))
        } else {
            None
        };

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new(global_wal.clone())),
            wal_enabled: config.wal_enabled,
            wal_sync_interval_ms: config.wal_sync_interval_ms,
            lock_stats_enabled: config.lock_stats_enabled,
            wal_last_sync_ms: Arc::new(AtomicU64::new(now_millis())),
            global_wal,
            global_txn,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.base_path.clone(),
            self.wal_enabled,
            self.wal_sync_interval_ms,
            self.wal_last_sync_ms.clone(),
            self.global_wal.clone(),
            self.global_txn.clone(),
        )
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}

fn warn_legacy_per_table_wal_files(base_path: &Path) {
    let mut legacy_wals = Vec::new();

    if let Ok(entries) = fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name == "global.wal" {
                continue;
            }
            if name.ends_with(".wal") {
                legacy_wals.push(name.to_string());
            }
        }
    }

    if !legacy_wals.is_empty() {
        eprintln!(
            "Found {} legacy per-table WAL file(s); they are ignored after global WAL cutover: {}",
            legacy_wals.len(),
            legacy_wals.join(", ")
        );
    }
}

fn recover_global_wal(
    base_path: &Path,
    global_txn: Arc<GlobalTxnState>,
) -> Result<(), WrongoDBError> {
    let wal_path = GlobalWal::path_for_db(base_path);
    if !wal_path.exists() {
        return Ok(());
    }

    let mut first_pass_reader = match WalReader::open(&wal_path) {
        Ok(reader) => reader,
        Err(err) => {
            eprintln!("Skipping global WAL recovery (failed to open WAL): {err}");
            return Ok(());
        }
    };

    let mut txn_table = RecoveryTxnTable::new();
    while let Some(record) = next_recovery_record(&mut first_pass_reader, "pass 1")? {
        txn_table.process_record(&record);
    }
    txn_table.finalize_pending();

    let mut second_pass_reader = match WalReader::open(&wal_path) {
        Ok(reader) => reader,
        Err(err) => {
            eprintln!("Skipping global WAL recovery (failed to reopen WAL): {err}");
            return Ok(());
        }
    };

    let mut replay_trees: HashMap<String, BTree> = HashMap::new();

    while let Some(record) = next_recovery_record(&mut second_pass_reader, "pass 2")? {
        if !txn_table.should_apply(&record) {
            continue;
        }

        match record {
            WalRecord::Put {
                store_name,
                key,
                value,
                ..
            } => {
                ensure_replay_tree(
                    &mut replay_trees,
                    base_path,
                    &store_name,
                    global_txn.clone(),
                )?
                .put_with_txn(&key, &value, TXN_NONE, false)?;
            }
            WalRecord::Delete {
                store_name, key, ..
            } => {
                ensure_replay_tree(
                    &mut replay_trees,
                    base_path,
                    &store_name,
                    global_txn.clone(),
                )?
                .delete_with_txn(&key, TXN_NONE, false)?;
            }
            WalRecord::Checkpoint | WalRecord::TxnCommit { .. } | WalRecord::TxnAbort { .. } => {}
        }
    }

    for tree in replay_trees.values_mut() {
        tree.checkpoint()?;
    }

    Ok(())
}

fn next_recovery_record(
    reader: &mut WalReader,
    pass: &str,
) -> Result<Option<WalRecord>, WrongoDBError> {
    match reader.read_record() {
        Ok(Some((_header, record))) => Ok(Some(record)),
        Ok(None) => Ok(None),
        Err(
            err @ (RecoveryError::ChecksumMismatch { .. }
            | RecoveryError::BrokenLsnChain { .. }
            | RecoveryError::CorruptRecordHeader { .. }
            | RecoveryError::CorruptRecordPayload { .. }),
        ) => {
            eprintln!("Stopping global WAL replay during {pass} at corrupted tail: {err}");
            Ok(None)
        }
        Err(err) => Err(StorageError(format!("failed reading WAL during {pass}: {err}")).into()),
    }
}

fn ensure_replay_tree<'a>(
    replay_trees: &'a mut HashMap<String, BTree>,
    base_path: &Path,
    store_name: &str,
    global_txn: Arc<GlobalTxnState>,
) -> Result<&'a mut BTree, WrongoDBError> {
    if !replay_trees.contains_key(store_name) {
        let store_path = base_path.join(store_name);
        let tree = if store_path.exists() {
            BTree::open_with_global_wal(&store_path, false, global_txn, None)?
        } else {
            BTree::create_with_global_wal(&store_path, 4096, false, global_txn, None)?
        };
        replay_trees.insert(store_name.to_string(), tree);
    }

    replay_trees
        .get_mut(store_name)
        .ok_or_else(|| StorageError(format!("replay tree missing for store {store_name}")).into())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
