use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::api::data_handle_cache::DataHandleCache;
use crate::api::session::Session;
use crate::core::errors::StorageError;
use crate::storage::table::Table;
use crate::storage::wal::{GlobalWal, RecoveryError, WalReader, WalRecord};
use crate::txn::{GlobalTxnState, RecoveryTxnTable, TxnManager};
use crate::WrongoDBError;

pub struct ConnectionConfig {
    pub wal_enabled: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self { wal_enabled: true }
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
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: Arc<DataHandleCache>,
    wal_enabled: bool,
    txn_manager: Arc<TxnManager>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("base_path", &self.base_path)
            .field("wal_enabled", &self.wal_enabled)
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
        let global_wal = if config.wal_enabled {
            warn_legacy_per_table_wal_files(&base_path);
            let recovery_manager = Arc::new(TxnManager::new(false, global_txn.clone(), None));
            recover_global_wal(&base_path, recovery_manager)?;
            Some(Arc::new(Mutex::new(GlobalWal::open_or_create(&base_path)?)))
        } else {
            None
        };
        let txn_manager = Arc::new(TxnManager::new(config.wal_enabled, global_txn, global_wal));

        Ok(Self {
            base_path,
            dhandle_cache: Arc::new(DataHandleCache::new(txn_manager.clone())),
            wal_enabled: config.wal_enabled,
            txn_manager,
        })
    }

    pub fn open_session(&self) -> Session {
        Session::new(
            self.dhandle_cache.clone(),
            self.base_path.clone(),
            self.txn_manager.clone(),
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

fn recover_global_wal(base_path: &Path, txn_manager: Arc<TxnManager>) -> Result<(), WrongoDBError> {
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

    let mut replay_tables: HashMap<String, Table> = HashMap::new();

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
                ensure_replay_table(
                    &mut replay_tables,
                    base_path,
                    &store_name,
                    txn_manager.clone(),
                )?
                .put_recovery(&key, &value)?;
            }
            WalRecord::Delete {
                store_name, key, ..
            } => {
                ensure_replay_table(
                    &mut replay_tables,
                    base_path,
                    &store_name,
                    txn_manager.clone(),
                )?
                .delete_recovery(&key)?;
            }
            WalRecord::Checkpoint | WalRecord::TxnCommit { .. } | WalRecord::TxnAbort { .. } => {}
        }
    }

    for table in replay_tables.values_mut() {
        table.checkpoint()?;
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

fn ensure_replay_table<'a>(
    replay_tables: &'a mut HashMap<String, Table>,
    base_path: &Path,
    store_name: &str,
    txn_manager: Arc<TxnManager>,
) -> Result<&'a mut Table, WrongoDBError> {
    if !replay_tables.contains_key(store_name) {
        let store_path = base_path.join(store_name);
        let table = Table::open_or_create_index(&store_path, txn_manager)?;
        replay_tables.insert(store_name.to_string(), table);
    }

    replay_tables
        .get_mut(store_name)
        .ok_or_else(|| StorageError(format!("replay table missing for store {store_name}")).into())
}
