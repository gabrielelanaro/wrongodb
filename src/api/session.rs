use std::sync::Arc;

use parking_lot::RwLock;

use crate::api::cursor::{Cursor, CursorKind};
use crate::core::errors::StorageError;
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp};
use crate::hooks::MutationHooks;
use crate::storage::table::Table;
use crate::storage::table_cache::TableCache;
use crate::txn::{Transaction, TransactionManager, TxnId};
use crate::WrongoDBError;

/// A request-scoped execution context over shared connection infrastructure.
///
/// `Connection` owns long-lived shared components (storage handles, global
/// transaction state, and durability machinery). `Session` exists to own mutable
/// per-request state that must not be global, especially the currently active
/// transaction and its lifecycle.
///
/// Why this type exists:
/// - It gives each unit of work a single owner for begin/commit/abort.
/// - It keeps request-local mutable state out of shared global state.
/// - It centralizes transaction visibility + durability orchestration while
///   lower layers (`Table`, `Cursor`) stay focused on storage access.
pub struct Session {
    table_cache: Arc<TableCache>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    mutation_hooks: Arc<dyn MutationHooks>,
    active_txn: Option<Transaction>,
}

impl Session {
    // Public API
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Self {
        Self {
            table_cache,
            transaction_manager,
            durability_backend,
            mutation_hooks,
            active_txn: None,
        }
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
            return Ok(Cursor::new(
                table,
                CursorKind::Table,
                self.mutation_hooks.clone(),
            ));
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
            return Ok(Cursor::new(
                index_table,
                CursorKind::Index,
                self.mutation_hooks.clone(),
            ));
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI: {}",
            uri
        ))))
    }

    pub(crate) fn table_handle(
        &mut self,
        collection: &str,
        mark_touched: bool,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        self.get_primary_table(collection, mark_touched)
    }

    pub(crate) fn mutation_hooks(&self) -> &dyn MutationHooks {
        self.mutation_hooks.as_ref()
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
        if self.active_txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }
        let txn = self.transaction_manager.begin_snapshot_txn();
        self.active_txn = Some(txn);
        Ok(SessionTxn::new(self))
    }

    /// Get a reference to the current transaction if one is active.
    pub fn current_txn(&self) -> Option<&Transaction> {
        self.active_txn.as_ref()
    }

    /// Get a mutable reference to the current transaction if one is active.
    pub fn current_txn_mut(&mut self) -> Option<&mut Transaction> {
        self.active_txn.as_mut()
    }

    #[allow(dead_code)]
    pub(crate) fn checkpoint_all(&mut self) -> Result<(), WrongoDBError> {
        let handles = self.table_cache.all_handles();
        for table in handles {
            table.write().checkpoint()?;
        }
        if self.transaction_manager.has_active_transactions()
            || !self.durability_backend.is_enabled()
        {
            return Ok(());
        }

        self.durability_backend
            .record(DurableOp::Checkpoint, DurabilityGuarantee::Sync)?;
        self.durability_backend.truncate_to_checkpoint()
    }

    // Private helpers
    fn get_primary_table(
        &mut self,
        collection: &str,
        mark_touched: bool,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        let table = self.table_cache.get_or_open_primary(collection)?;
        if mark_touched {
            self.mark_table_touched(&format!("table:{}", collection));
        }
        Ok(table)
    }

    /// Mark a table as touched in the current transaction.
    fn mark_table_touched(&mut self, uri: &str) {
        if let Some(ref mut txn) = self.active_txn {
            txn.mark_table_touched(uri);
        }
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
    /// Commit the transaction.
    ///
    /// After calling this, the transaction handle is consumed and cannot be used again.
    /// Commits the transaction across all touched tables.
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        let Some(txn) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let touched_tables: Vec<String> = txn.touched_tables().iter().cloned().collect();
        let txn_id = txn.id();

        self.session.mutation_hooks.before_commit(txn_id, txn_id)?;

        let txn =
            self.session.active_txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during commit".into())
            })?;
        self.session.transaction_manager.commit_txn_state(txn)?;

        self.session.active_txn = None;
        self.committed = true;

        if self.session.mutation_hooks.should_apply_locally() {
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
        let Some(txn) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let touched_tables: Vec<String> = txn.touched_tables().iter().cloned().collect();
        let txn_id = txn.id();

        self.session.mutation_hooks.before_abort(txn_id)?;

        let txn =
            self.session.active_txn.as_mut().ok_or_else(|| {
                StorageError("transaction context disappeared during abort".into())
            })?;
        self.session.transaction_manager.abort_txn_state(txn)?;

        self.session.active_txn = None;
        self.committed = true;

        if self.session.mutation_hooks.should_apply_locally() {
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
            .active_txn
            .as_mut()
            .expect("transaction should exist")
    }

    /// Get a shared reference to the underlying transaction.
    ///
    /// This is useful for accessing transaction metadata (e.g., txn id).
    pub fn as_ref(&self) -> &Transaction {
        self.session
            .active_txn
            .as_ref()
            .expect("transaction should exist")
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.session
    }

    fn new(session: &'a mut Session) -> Self {
        Self {
            session,
            committed: false,
        }
    }
}

impl<'a> Drop for SessionTxn<'a> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(mut txn) = self.session.active_txn.take() {
                let touched_tables: Vec<String> = txn.touched_tables().iter().cloned().collect();
                let txn_id = txn.id();
                let _ = self.session.mutation_hooks.before_abort(txn_id);
                let _ = self.session.transaction_manager.abort_txn_state(&mut txn);
                if self.session.mutation_hooks.should_apply_locally() {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tempfile::tempdir;

    use super::*;
    use crate::api::connection::{Connection, ConnectionConfig};
    use crate::durability::DurabilityBackend;
    use crate::hooks::MutationHooks;
    use crate::storage::table_cache::TableCache;
    use crate::storage::wal::GlobalWal;
    use crate::txn::GlobalTxnState;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HookCall {
        Commit { txn_id: TxnId, commit_ts: TxnId },
        Abort { txn_id: TxnId },
    }

    #[derive(Debug, Default)]
    struct MockHooks {
        calls: Mutex<Vec<HookCall>>,
    }

    impl MutationHooks for MockHooks {
        fn before_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
            self.calls
                .lock()
                .push(HookCall::Commit { txn_id, commit_ts });
            Ok(())
        }

        fn before_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
            self.calls.lock().push(HookCall::Abort { txn_id });
            Ok(())
        }
    }

    fn new_session_with_hooks(hooks: Arc<dyn MutationHooks>) -> Session {
        let dir = tempdir().unwrap();
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_cache = Arc::new(TableCache::new(
            dir.path().to_path_buf(),
            transaction_manager.clone(),
        ));
        Session::new(
            table_cache,
            transaction_manager,
            Arc::new(DurabilityBackend::Disabled),
            hooks,
        )
    }

    #[test]
    fn checkpoint_all_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();

        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut cursor = session.open_cursor("table:items").unwrap();
            let txn = session.transaction().unwrap();
            let txn_id = txn.as_ref().id();
            cursor.insert(b"k1", b"v1", txn_id).unwrap();
            txn.commit().unwrap();
        }

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        let mut active_session = conn.open_session();
        let _txn = active_session.transaction().unwrap();
        session.checkpoint_all().unwrap();

        let after = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(after, before);
    }

    #[test]
    fn checkpoint_all_truncates_when_no_active_transactions() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();

        let mut session = conn.open_session();
        session.create("table:items").unwrap();

        {
            let mut cursor = session.open_cursor("table:items").unwrap();
            let txn = session.transaction().unwrap();
            let txn_id = txn.as_ref().id();
            cursor.insert(b"k1", b"v1", txn_id).unwrap();
            txn.commit().unwrap();
        }

        let wal_path = GlobalWal::path_for_db(dir.path());
        let before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(before > 512);

        session.checkpoint_all().unwrap();

        let after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after <= 512);
    }

    #[test]
    fn commit_calls_before_commit_hook() {
        let hooks = Arc::new(MockHooks::default());
        let mut session = new_session_with_hooks(hooks.clone());
        let txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();

        txn.commit().unwrap();

        let calls = hooks.calls.lock();
        assert_eq!(
            *calls,
            vec![HookCall::Commit {
                txn_id,
                commit_ts: txn_id
            }]
        );
    }

    #[test]
    fn abort_and_drop_call_before_abort_hook() {
        let hooks = Arc::new(MockHooks::default());
        let mut session = new_session_with_hooks(hooks.clone());
        let txn = session.transaction().unwrap();
        let abort_txn_id = txn.as_ref().id();
        txn.abort().unwrap();

        {
            let dropped = session.transaction().unwrap();
            let drop_txn_id = dropped.as_ref().id();
            drop(dropped);

            let calls = hooks.calls.lock();
            assert_eq!(
                *calls,
                vec![
                    HookCall::Abort {
                        txn_id: abort_txn_id
                    },
                    HookCall::Abort {
                        txn_id: drop_txn_id
                    }
                ]
            );
        }
    }
}
