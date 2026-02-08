use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

use crate::api::cursor::{Cursor, CursorKind};
use crate::api::data_handle_cache::DataHandleCache;
use crate::core::errors::StorageError;
use crate::core::lock_stats::{begin_lock_hold, record_lock_wait, LockStatKind};
use crate::storage::table::Table;
use crate::storage::wal::GlobalWal;
use crate::txn::{GlobalTxnState, PendingWalOp, Transaction};
use crate::WrongoDBError;

struct SessionTxnContext {
    txn: Transaction,
}

pub struct Session {
    cache: Arc<DataHandleCache>,
    base_path: PathBuf,
    wal_enabled: bool,
    wal_sync_interval_ms: u64,
    wal_last_sync_ms: Arc<AtomicU64>,
    global_wal: Option<Arc<Mutex<GlobalWal>>>,
    global_txn: Arc<GlobalTxnState>,
    txn: Option<SessionTxnContext>,
}

impl Session {
    pub(crate) fn new(
        cache: Arc<DataHandleCache>,
        base_path: PathBuf,
        wal_enabled: bool,
        wal_sync_interval_ms: u64,
        wal_last_sync_ms: Arc<AtomicU64>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            cache,
            base_path,
            wal_enabled,
            wal_sync_interval_ms,
            wal_last_sync_ms,
            global_wal,
            global_txn,
            txn: None,
        }
    }

    /// Mark a table as touched in the current transaction.
    fn mark_table_touched(&mut self, uri: &str) {
        if let Some(ref mut ctx) = self.txn {
            ctx.txn.mark_table_touched(uri);
        }
    }

    fn get_primary_table(
        &mut self,
        collection: &str,
        mark_touched: bool,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        let uri = format!("table:{}", collection);
        let table = self.cache.get_or_open_primary(
            &uri,
            collection,
            &self.base_path,
            self.wal_enabled,
            self.global_txn.clone(),
        )?;
        if mark_touched {
            self.mark_table_touched(&uri);
        }
        Ok(table)
    }

    pub(crate) fn table_handle(
        &mut self,
        collection: &str,
        mark_touched: bool,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        self.get_primary_table(collection, mark_touched)
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            let _table = self.get_primary_table(collection, false)?;
            Ok(())
        } else {
            Err(WrongoDBError::Storage(StorageError(format!(
                "unsupported URI: {}",
                uri
            ))))
        }
    }

    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            let table = self.get_primary_table(collection, true)?;
            return Ok(Cursor::new(table, CursorKind::Table));
        }

        if uri.starts_with("index:") {
            let (collection, index_name) = parse_index_uri(uri)?;
            let table = self.get_primary_table(collection, false)?;
            let index_table = {
                let table_guard = table.read();
                let catalog = table_guard
                    .index_catalog()
                    .ok_or_else(|| StorageError("missing index catalog".into()))?;
                catalog
                    .index_handle(index_name)
                    .ok_or_else(|| StorageError("unknown index".into()))?
            };
            return Ok(Cursor::new(index_table, CursorKind::Index));
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI: {}",
            uri
        ))))
    }

    /// Begin a new transaction and return an RAII handle.
    ///
    /// The transaction will auto-rollback if not explicitly committed or aborted.
    ///
    /// # Example
    /// ```no_run
    /// use wrongodb::{Connection, ConnectionConfig};
    ///
    /// let conn = Connection::open("/tmp/test", ConnectionConfig::default()).unwrap();
    /// let mut session = conn.open_session();
    /// session.create("table:test").unwrap();
    ///
    /// let mut cursor = session.open_cursor("table:test").unwrap();
    /// {
    ///     let mut txn = session.transaction().unwrap();
    ///     let txn_id = txn.as_ref().id();
    ///     cursor.insert(b"key", b"value", txn_id).unwrap();
    ///     txn.commit().unwrap();
    /// }
    /// ```
    pub fn transaction(&mut self) -> Result<SessionTxn<'_>, WrongoDBError> {
        if self.txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        let txn = self.global_txn.begin_snapshot_txn();
        self.txn = Some(SessionTxnContext { txn });
        Ok(SessionTxn::new(self))
    }

    /// Get a reference to the current transaction if one is active.
    pub fn current_txn(&self) -> Option<&Transaction> {
        self.txn.as_ref().map(|ctx| &ctx.txn)
    }

    /// Get a mutable reference to the current transaction if one is active.
    pub fn current_txn_mut(&mut self) -> Option<&mut Transaction> {
        self.txn.as_mut().map(|ctx| &mut ctx.txn)
    }

    fn maybe_sync_wal_locked(&self, wal: &mut GlobalWal) -> Result<(), WrongoDBError> {
        if self.wal_sync_interval_ms == 0 {
            return wal.sync();
        }

        let now = now_millis();
        let last_sync = self.wal_last_sync_ms.load(Ordering::Acquire);
        if now.saturating_sub(last_sync) >= self.wal_sync_interval_ms
            && self
                .wal_last_sync_ms
                .compare_exchange(last_sync, now, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            if let Err(err) = wal.sync() {
                self.wal_last_sync_ms.store(last_sync, Ordering::Release);
                return Err(err);
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint_all(&mut self) -> Result<(), WrongoDBError> {
        let handles = self.cache.all_handles();
        for table in handles {
            let wait_start = Instant::now();
            let mut guard = table.write();
            record_lock_wait(LockStatKind::Checkpoint, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::Checkpoint);
            guard.checkpoint()?;
        }

        if self.wal_enabled && self.global_txn.has_active_transactions() {
            return Ok(());
        }

        if self.wal_enabled {
            if let Some(global_wal) = self.global_wal.as_ref() {
                let wait_start = Instant::now();
                let mut wal = global_wal.lock();
                record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
                let _hold = begin_lock_hold(LockStatKind::Wal);
                if self.global_txn.has_active_transactions() {
                    return Ok(());
                }

                let checkpoint_lsn = wal.log_checkpoint()?;
                wal.set_checkpoint_lsn(checkpoint_lsn)?;
                wal.sync()?;
                wal.truncate_to_checkpoint()?;
            }
        }

        Ok(())
    }
}

/// RAII transaction handle for Session.
///
/// The transaction will auto-rollback on drop if not explicitly committed or aborted.
/// This follows the pattern used by sled and other Rust database libraries.
pub struct SessionTxn<'a> {
    session: &'a mut Session,
    committed: bool,
}

impl<'a> SessionTxn<'a> {
    fn new(session: &'a mut Session) -> Self {
        Self {
            session,
            committed: false,
        }
    }

    /// Commit the transaction.
    ///
    /// After calling this, the transaction handle is consumed and cannot be used again.
    /// Commits the transaction across all touched tables.
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        if let Some(mut ctx) = self.session.txn.take() {
            let txn_id = ctx.txn.id();

            if self.session.wal_enabled {
                let pending_wal_ops = self.session.global_txn.take_pending_wal_ops(txn_id);
                if let Some(global_wal) = self.session.global_wal.as_ref() {
                    let wait_start = Instant::now();
                    let mut wal = global_wal.lock();
                    record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
                    let _hold = begin_lock_hold(LockStatKind::Wal);
                    Self::flush_pending_wal_ops(&mut wal, txn_id, pending_wal_ops)?;
                    wal.log_txn_commit(txn_id, txn_id)?;
                    self.session.maybe_sync_wal_locked(&mut wal)?;
                }
            }

            ctx.txn.commit(&self.session.global_txn)?;
        }
        self.committed = true;
        Ok(())
    }

    /// Abort/rollback the transaction.
    ///
    /// After calling this, the transaction handle is consumed and cannot be used again.
    /// Aborts the transaction across all touched tables.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        if let Some(mut ctx) = self.session.txn.take() {
            let touched_tables: Vec<String> = ctx.txn.touched_tables().iter().cloned().collect();
            let txn_id = ctx.txn.id();
            self.session.global_txn.clear_pending_wal_ops(txn_id);

            if self.session.wal_enabled {
                if let Some(global_wal) = self.session.global_wal.as_ref() {
                    let wait_start = Instant::now();
                    let mut wal = global_wal.lock();
                    record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
                    let _hold = begin_lock_hold(LockStatKind::Wal);
                    wal.log_txn_abort(txn_id)?;
                }
            }

            ctx.txn.abort(&self.session.global_txn)?;

            for uri in &touched_tables {
                if !uri.starts_with("table:") {
                    continue;
                }
                let collection = &uri[6..];
                let table = self.session.get_primary_table(collection, false)?;
                let wait_start = Instant::now();
                let mut table_guard = table.write();
                record_lock_wait(LockStatKind::Table, wait_start.elapsed());
                let _hold = begin_lock_hold(LockStatKind::Table);
                table_guard.mark_updates_aborted(txn_id)?;
                if let Some(catalog) = table_guard.index_catalog_mut() {
                    catalog.mark_updates_aborted(txn_id)?;
                }
            }
        }
        self.committed = true;
        Ok(())
    }

    /// Get a mutable reference to the underlying transaction.
    ///
    /// This is useful for accessing transaction metadata (e.g., txn id).
    pub fn as_mut(&mut self) -> &mut Transaction {
        self.session
            .txn
            .as_mut()
            .map(|ctx| &mut ctx.txn)
            .expect("transaction should exist")
    }

    /// Get a shared reference to the underlying transaction.
    ///
    /// This is useful for accessing transaction metadata (e.g., txn id).
    pub fn as_ref(&self) -> &Transaction {
        self.session
            .txn
            .as_ref()
            .map(|ctx| &ctx.txn)
            .expect("transaction should exist")
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.session
    }
}

impl<'a> Drop for SessionTxn<'a> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(mut ctx) = self.session.txn.take() {
                let touched_tables: Vec<String> =
                    ctx.txn.touched_tables().iter().cloned().collect();
                let txn_id = ctx.txn.id();
                self.session.global_txn.clear_pending_wal_ops(txn_id);
                if self.session.wal_enabled {
                    if let Some(global_wal) = self.session.global_wal.as_ref() {
                        let wait_start = Instant::now();
                        let mut wal = global_wal.lock();
                        record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
                        let _hold = begin_lock_hold(LockStatKind::Wal);
                        let _ = wal.log_txn_abort(txn_id);
                    }
                }
                let _ = ctx.txn.abort(&self.session.global_txn);

                for uri in &touched_tables {
                    if !uri.starts_with("table:") {
                        continue;
                    }
                    let collection = &uri[6..];
                    if let Ok(table) = self.session.get_primary_table(collection, false) {
                        let wait_start = Instant::now();
                        let mut table_guard = table.write();
                        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
                        let _hold = begin_lock_hold(LockStatKind::Table);
                        let _ = table_guard.mark_updates_aborted(txn_id);
                        if let Some(catalog) = table_guard.index_catalog_mut() {
                            let _ = catalog.mark_updates_aborted(txn_id);
                        }
                    }
                }
            }
        }
    }
}

impl<'a> SessionTxn<'a> {
    fn flush_pending_wal_ops(
        wal: &mut GlobalWal,
        txn_id: u64,
        pending_wal_ops: Vec<PendingWalOp>,
    ) -> Result<(), WrongoDBError> {
        for op in pending_wal_ops {
            match op {
                PendingWalOp::Put {
                    store_name,
                    key,
                    value,
                } => {
                    wal.log_put_owned(store_name, key, value, txn_id)?;
                }
                PendingWalOp::Delete { store_name, key } => {
                    wal.log_delete_owned(store_name, key, txn_id)?;
                }
            }
        }
        Ok(())
    }
}

fn parse_index_uri(uri: &str) -> Result<(&str, &str), WrongoDBError> {
    if !uri.starts_with("index:") {
        return Err(WrongoDBError::Storage(StorageError(format!(
            "invalid index URI: {}",
            uri
        ))));
    }
    let rest = &uri[6..];
    let mut parts = rest.splitn(2, ':');
    let collection = parts.next().unwrap_or("");
    let index = parts.next().unwrap_or("");
    if collection.is_empty() || index.is_empty() {
        return Err(WrongoDBError::Storage(StorageError(format!(
            "invalid index URI: {}",
            uri
        ))));
    }
    Ok((collection, index))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// SAFETY: SessionTxn holds &mut Session, so it's !Send by default.
// This is correct because Session is not thread-safe.
// Note: Negative trait bounds (!Send, !Sync) require unstable Rust.
// For now, we rely on the fact that Session contains non-Send types.
