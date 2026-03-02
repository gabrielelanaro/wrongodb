use std::sync::Arc;

use parking_lot::RwLock;

use crate::api::cursor::{Cursor, CursorKind};
use crate::api::data_handle_cache::DataHandleCache;
use crate::core::errors::StorageError;
use crate::recovery::RecoveryManager;
use crate::storage::table::Table;
use crate::txn::transaction_manager::TransactionManager;
use crate::txn::{Transaction, TxnId};
use crate::WrongoDBError;

struct SessionTxnContext {
    txn: Transaction,
}

pub struct Session {
    cache: Arc<DataHandleCache>,
    transaction_manager: Arc<TransactionManager>,
    recovery_manager: Arc<RecoveryManager>,
    txn: Option<SessionTxnContext>,
}

impl Session {
    pub(crate) fn new(
        cache: Arc<DataHandleCache>,
        transaction_manager: Arc<TransactionManager>,
        recovery_manager: Arc<RecoveryManager>,
    ) -> Self {
        Self {
            cache,
            transaction_manager,
            recovery_manager,
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
        let table = self.cache.get_or_open_primary(&uri, collection)?;
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
        let txn = self.transaction_manager.begin_snapshot_txn();
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

    pub(crate) fn checkpoint_all(&mut self) -> Result<(), WrongoDBError> {
        let handles = self.cache.all_handles();
        for table in handles {
            table.write().checkpoint()?;
        }
        self.recovery_manager
            .checkpoint_and_truncate_if_safe(self.transaction_manager.has_active_transactions())
    }

    fn finalize_touched_tables_locally(
        &mut self,
        touched_tables: &[String],
        txn_id: TxnId,
        committed: bool,
    ) -> Result<(), WrongoDBError> {
        for uri in touched_tables {
            if !uri.starts_with("table:") {
                continue;
            }
            let collection = &uri[6..];
            let table = self.get_primary_table(collection, false)?;
            let mut table_guard = table.write();
            if committed {
                table_guard.local_mark_updates_committed(txn_id)?;
            } else {
                table_guard.local_mark_updates_aborted(txn_id)?;
            }
            if let Some(catalog) = table_guard.index_catalog_mut() {
                if committed {
                    catalog.mark_updates_committed(txn_id)?;
                } else {
                    catalog.mark_updates_aborted(txn_id)?;
                }
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
        let Some(ctx) = self.session.txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let touched_tables: Vec<String> = ctx.txn.touched_tables().iter().cloned().collect();
        let txn_id = ctx.txn.id();

        self.session
            .recovery_manager
            .log_txn_commit_sync(txn_id, txn_id)?;

        let ctx =
            self.session.txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during commit".into())
            })?;
        self.session
            .transaction_manager
            .commit_txn_state(&mut ctx.txn)?;

        self.session.txn = None;
        self.committed = true;

        if !self.session.recovery_manager.wal_enabled() {
            self.session
                .finalize_touched_tables_locally(&touched_tables, txn_id, true)?;
        }
        Ok(())
    }

    /// Abort/rollback the transaction.
    ///
    /// After calling this, the transaction handle is consumed and cannot be used again.
    /// Aborts the transaction across all touched tables.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        let Some(ctx) = self.session.txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let touched_tables: Vec<String> = ctx.txn.touched_tables().iter().cloned().collect();
        let txn_id = ctx.txn.id();

        self.session.recovery_manager.log_txn_abort(txn_id)?;

        let ctx =
            self.session.txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during abort".into())
            })?;
        self.session
            .transaction_manager
            .abort_txn_state(&mut ctx.txn)?;

        self.session.txn = None;
        self.committed = true;

        if !self.session.recovery_manager.wal_enabled() {
            self.session
                .finalize_touched_tables_locally(&touched_tables, txn_id, false)?;
        }
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
                let _ = self.session.recovery_manager.log_txn_abort(txn_id);
                let _ = self
                    .session
                    .transaction_manager
                    .abort_txn_state(&mut ctx.txn);
                if !self.session.recovery_manager.wal_enabled() {
                    let _ = self.session.finalize_touched_tables_locally(
                        &touched_tables,
                        txn_id,
                        false,
                    );
                }
            }
        }
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

// SAFETY: SessionTxn holds &mut Session, so it's !Send by default.
// This is correct because Session is not thread-safe.
// Note: Negative trait bounds (!Send, !Sync) require unstable Rust.
// For now, we rely on the fact that Session contains non-Send types.
