use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::core::errors::StorageError;
use crate::cursor::Cursor;
use crate::datahandle_cache::DataHandleCache;
use crate::storage::table::Table;
use crate::txn::{GlobalTxnState, Transaction};
use crate::WrongoDBError;

pub struct Session {
    cache: Arc<DataHandleCache>,
    base_path: PathBuf,
    wal_enabled: bool,
    global_txn: Arc<GlobalTxnState>,
    txn: Option<Transaction>,
}

impl Session {
    pub(crate) fn new(
        cache: Arc<DataHandleCache>,
        base_path: PathBuf,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            cache,
            base_path,
            wal_enabled,
            global_txn,
            txn: None,
        }
    }

    /// Mark a table as touched in the current transaction.
    fn mark_table_touched(&mut self, uri: &str) {
        if let Some(ref mut txn) = self.txn {
            txn.mark_table_touched(uri);
        }
    }

    /// Get a table from the cache and mark it as touched if a transaction is active.
    fn get_table(&mut self, uri: &str) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        let table_path = self.base_path.join(&uri[6..]);
        let table = self.cache.get_or_open_table(
            uri,
            &table_path,
            self.wal_enabled,
            self.global_txn.clone(),
        )?;
        self.mark_table_touched(uri);
        Ok(table)
    }

    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if uri.starts_with("table:") {
            let _table = self.get_table(uri)?;
            Ok(())
        } else {
            Err(WrongoDBError::Storage(StorageError(
                format!("unsupported URI: {}", uri),
            )))
        }
    }

    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let table = self.get_table(uri)?;
        Ok(Cursor::new(table))
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
    ///     cursor.insert(b"key", b"value", txn.as_mut()).unwrap();
    ///     txn.commit().unwrap();
    /// }
    /// ```
    pub fn transaction(&mut self) -> Result<SessionTxn<'_>, WrongoDBError> {
        if self.txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        self.txn = Some(self.global_txn.begin_snapshot_txn());
        Ok(SessionTxn::new(self))
    }

    /// Get a reference to the current transaction if one is active.
    pub fn current_txn(&self) -> Option<&Transaction> {
        self.txn.as_ref()
    }

    /// Get a mutable reference to the current transaction if one is active.
    pub fn current_txn_mut(&mut self) -> Option<&mut Transaction> {
        self.txn.as_mut()
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
        if let Some(mut txn) = self.session.txn.take() {
            // Get touched tables before we consume txn
            let touched_tables: Vec<String> = txn.touched_tables().iter().cloned().collect();

            // Step 1: Update global transaction state
            txn.commit(&self.session.global_txn)?;

            // Step 2: Commit each touched table
            // The first table writes the commit record to WAL
            // Subsequent tables just sync their WAL
            let mut is_first = true;
            for uri in &touched_tables {
                let table_path = self.session.base_path.join(&uri[6..]);
                let table = self
                    .session
                    .cache
                    .get_or_open_table(
                        uri,
                        &table_path,
                        self.session.wal_enabled,
                        self.session.global_txn.clone(),
                    )?;
                let mut table_guard = table.write();
                if is_first {
                    table_guard.commit_txn(&mut txn)?;
                    is_first = false;
                } else {
                    table_guard.sync_wal()?;
                }
            }
        }
        self.committed = true;
        Ok(())
    }

    /// Abort/rollback the transaction.
    ///
    /// After calling this, the transaction handle is consumed and cannot be used again.
    /// Aborts the transaction across all touched tables.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        if let Some(mut txn) = self.session.txn.take() {
            // Get touched tables before we consume txn
            let touched_tables: Vec<String> = txn.touched_tables().iter().cloned().collect();

            // Step 1: Update global transaction state
            txn.abort(&self.session.global_txn)?;

            // Step 2: Abort each touched table
            for uri in &touched_tables {
                let table_path = self.session.base_path.join(&uri[6..]);
                let table = self
                    .session
                    .cache
                    .get_or_open_table(
                        uri,
                        &table_path,
                        self.session.wal_enabled,
                        self.session.global_txn.clone(),
                    )?;
                let mut table_guard = table.write();
                table_guard.abort_txn(&mut txn)?;
            }
        }
        self.committed = true; // Mark as handled so drop doesn't rollback
        Ok(())
    }

    /// Get a mutable reference to the underlying transaction.
    /// 
    /// This is useful for passing to cursor operations that need &mut Transaction.
    pub fn as_mut(&mut self) -> &mut Transaction {
        self.session.txn.as_mut().expect("transaction should exist")
    }

    /// Get a shared reference to the underlying transaction.
    /// 
    /// This is useful for cursor operations that only need &Transaction.
    pub fn as_ref(&self) -> &Transaction {
        self.session.txn.as_ref().expect("transaction should exist")
    }
}

impl<'a> Drop for SessionTxn<'a> {
    fn drop(&mut self) {
        // If transaction wasn't committed or aborted, auto-rollback
        if !self.committed {
            if let Some(mut txn) = self.session.txn.take() {
                let _ = txn.abort(&self.session.global_txn);
            }
            // Note: touched_tables are in Transaction, not Session
            // They will be dropped when txn is dropped
        }
    }
}

// SAFETY: SessionTxn holds &mut Session, so it's !Send by default.
// This is correct because Session is not thread-safe.
// Note: Negative trait bounds (!Send, !Sync) require unstable Rust.
// For now, we rely on the fact that Session contains non-Send types.