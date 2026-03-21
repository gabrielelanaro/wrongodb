use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::btree::BTreeCursor;
use crate::storage::table::{
    apply_delete_autocommit, apply_delete_in_txn, apply_put_autocommit, apply_put_in_txn,
    contains_key, get_version, scan_range, TableMetadata,
};
use crate::storage::RecoveryUnit;
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

/// A single key/value entry returned by [`TableCursor::next`].
///
/// The public table cursor surface is deliberately key/value-shaped because it is the
/// low-level storage API, not the document/query API.
///
/// This type alias exists to keep that low-level API readable without
/// introducing a heavier public wrapper type.
pub type CursorEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableCursorWriteAccess {
    #[cfg(test)]
    ReadOnly,
    ReadWrite,
}

/// Table cursor over a single physical store.
///
/// A table cursor is always bound to one store and consults the current
/// session transaction context at call time. When no transaction is active,
/// writes autocommit; when a transaction is active, the cursor participates in
/// that transaction.
///
/// The reason this type exists publicly is to expose a small, explicit
/// storage interface for one store at a time. It is intentionally not
/// responsible for collection semantics or commit/abort orchestration.
///
/// If a caller needs document-level writes, they should go through the
/// higher-level command path, not extend this type.
pub struct TableCursor {
    table: TableMetadata,
    main_table_btree: Arc<RwLock<BTreeCursor>>,
    transaction_manager: Arc<TransactionManager>,
    txn_handle: Option<Weak<Mutex<Transaction>>>,
    recovery_unit: Arc<dyn RecoveryUnit>,
    write_access: TableCursorWriteAccess,
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl TableCursor {
    pub(crate) fn new(
        table: TableMetadata,
        main_table_btree: Arc<RwLock<BTreeCursor>>,
        transaction_manager: Arc<TransactionManager>,
        txn_handle: Option<Weak<Mutex<Transaction>>>,
        recovery_unit: Arc<dyn RecoveryUnit>,
        write_access: TableCursorWriteAccess,
    ) -> Self {
        Self {
            table,
            main_table_btree,
            transaction_manager,
            txn_handle,
            recovery_unit,
            write_access,
            buffered_entries: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
            range_start: None,
            range_end: None,
        }
    }

    /// Restrict iteration to the given half-open key range.
    ///
    /// Range state lives on the cursor so scans can be resumed incrementally
    /// without pushing iterator state into `Session`.
    ///
    /// The method exists on `TableCursor` because scan position is cursor-local
    /// state, not session state.
    pub fn set_range(&mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) {
        self.range_start = start;
        self.range_end = end;
        self.reset();
    }

    fn ensure_writable(&self) -> Result<(), WrongoDBError> {
        if self.write_access == TableCursorWriteAccess::ReadWrite {
            return Ok(());
        }
        Err(WrongoDBError::Storage(StorageError(
            "cursor is read-only in this context".into(),
        )))
    }

    fn current_txn_id(&self) -> TxnId {
        self.txn_handle
            .as_ref()
            .and_then(|handle| handle.upgrade())
            .map(|txn| txn.lock().id())
            .unwrap_or(TXN_NONE)
    }

    fn current_txn_context(&self) -> (TxnId, Option<Arc<Mutex<Transaction>>>) {
        match self.txn_handle.as_ref().and_then(|handle| handle.upgrade()) {
            Some(txn_handle) => {
                let txn_id = txn_handle.lock().id();
                (txn_id, Some(txn_handle))
            }
            None => (TXN_NONE, None),
        }
    }

    fn apply_put(
        &self,
        btree: &mut BTreeCursor,
        key: &[u8],
        value: &[u8],
        txn_handle: Option<Arc<Mutex<Transaction>>>,
    ) -> Result<(), WrongoDBError> {
        match txn_handle {
            Some(txn_handle) => {
                let mut txn = txn_handle.lock();
                apply_put_in_txn(btree, key, value, &mut txn)
            }
            None => apply_put_autocommit(btree, self.transaction_manager.as_ref(), key, value),
        }
    }

    fn apply_delete(
        &self,
        btree: &mut BTreeCursor,
        key: &[u8],
        txn_handle: Option<Arc<Mutex<Transaction>>>,
    ) -> Result<bool, WrongoDBError> {
        match txn_handle {
            Some(txn_handle) => {
                let mut txn = txn_handle.lock();
                apply_delete_in_txn(btree, key, &mut txn)
            }
            None => apply_delete_autocommit(btree, self.transaction_manager.as_ref(), key),
        }
    }

    /// Insert a new key/value pair into the bound store.
    ///
    /// The cursor performs only store-local semantics here: duplicate-key
    /// validation, local durability recording for local write modes, and MVCC
    /// application through the current transaction context.
    ///
    /// It exists on `TableCursor` because insert is a one-store mutation. Higher
    /// layers remain responsible for multi-store work such as index
    /// maintenance.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let (txn_id, txn_handle) = self.current_txn_context();
        self.ensure_writable()?;
        let mut btree = self.main_table_btree.write();
        if contains_key(&mut btree, key, txn_id)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }
        self.recovery_unit
            .record_put(self.table.uri(), key, value, txn_id)?;
        self.apply_put(&mut btree, key, value, txn_handle)?;
        Ok(())
    }

    /// Replace the value for an existing key in the bound store.
    ///
    /// Missing-key validation happens here because this is still a one-store
    /// operation. Higher-level document/index orchestration lives elsewhere.
    ///
    /// This separation keeps update semantics local to one store and prevents
    /// `TableCursor` from turning into a document write controller.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let (txn_id, txn_handle) = self.current_txn_context();
        self.ensure_writable()?;
        let mut btree = self.main_table_btree.write();
        if !contains_key(&mut btree, key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for update".to_string(),
            )));
        }
        self.recovery_unit
            .record_put(self.table.uri(), key, value, txn_id)?;
        self.apply_put(&mut btree, key, value, txn_handle)?;
        Ok(())
    }

    /// Delete an existing key from the bound store.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let (txn_id, txn_handle) = self.current_txn_context();
        self.ensure_writable()?;
        let mut btree = self.main_table_btree.write();
        if !contains_key(&mut btree, key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        self.recovery_unit
            .record_delete(self.table.uri(), key, txn_id)?;
        let deleted = self.apply_delete(&mut btree, key, txn_handle)?;
        if !deleted {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    /// Fetch the visible value for `key` in the cursor's current transaction context.
    ///
    /// The method lives here because visibility is still a one-store,
    /// one-transaction question.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let txn_id = self.current_txn_id();
        let mut btree = self.main_table_btree.write();
        get_version(&mut btree, key, txn_id)
    }

    #[allow(clippy::should_implement_trait)]
    /// Return the next visible key/value pair in the configured range.
    ///
    /// Iteration is cursor-owned so callers can scan a store incrementally
    /// without materializing all results in `Session`.
    ///
    /// Keeping this state on the cursor is what makes the cursor abstraction
    /// useful in the first place.
    pub fn next(&mut self) -> Result<Option<CursorEntry>, WrongoDBError> {
        let txn_id = self.current_txn_id();
        if self.exhausted {
            return Ok(None);
        }

        if self.buffer_pos >= self.buffered_entries.len() {
            self.refill_buffer(txn_id)?;

            if self.buffered_entries.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }

            self.buffer_pos = 0;
        }

        if self.buffer_pos < self.buffered_entries.len() {
            let entry = self.buffered_entries[self.buffer_pos].clone();
            self.buffer_pos += 1;
            Ok(Some(entry))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }

    fn refill_buffer(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let mut btree = self.main_table_btree.write();

        let mut start_key = self.range_start.as_deref();
        let mut skip_start = false;
        if let Some((last_key, _)) = self.buffered_entries.last() {
            start_key = Some(last_key.as_slice());
            skip_start = true;
        }

        let entries = scan_range(&mut btree, start_key, self.range_end.as_deref(), txn_id)?;

        if skip_start && !entries.is_empty() {
            if let Some(start) = start_key {
                let mut iter = entries.into_iter();
                let first = iter.next();
                self.buffered_entries = if let Some((key, value)) = first {
                    if key.as_slice() == start {
                        iter.collect()
                    } else {
                        let mut out = Vec::new();
                        out.push((key, value));
                        out.extend(iter);
                        out
                    }
                } else {
                    Vec::new()
                };
            } else {
                self.buffered_entries = entries;
            }
        } else {
            self.buffered_entries = entries;
        }
        self.buffer_pos = 0;

        Ok(())
    }

    /// Reset the cursor position to the beginning.
    ///
    /// This keeps cursor state reusable for repeated scans over the same store
    /// without reopening the cursor.
    ///
    /// The method exists because cursor position is mutable operational state,
    /// not part of the store itself.
    pub fn reset(&mut self) {
        self.buffered_entries.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::storage::table::{apply_put_autocommit, open_or_create_btree};
    use crate::storage::NoopRecoveryUnit;
    use crate::txn::{GlobalTxnState, TXN_NONE};

    fn open_index_table() -> (
        TempDir,
        Arc<RwLock<BTreeCursor>>,
        Arc<TransactionManager>,
        TableMetadata,
    ) {
        let tmp = tempdir().unwrap();
        let uri = "index:test:cursor".to_string();
        let store_name = "cursor.idx.wt".to_string();
        let path = tmp.path().join(&store_name);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let btree = Arc::new(RwLock::new(open_or_create_btree(path).unwrap()));
        (tmp, btree, transaction_manager, TableMetadata::new(uri))
    }

    #[test]
    fn insert_applies_locally() {
        let (_tmp, btree, transaction_manager, table) = open_index_table();
        let mut cursor = TableCursor::new(
            table,
            btree.clone(),
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        );

        cursor.insert(b"k1", b"v1").unwrap();

        assert_eq!(
            get_version(&mut btree.write(), b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn delete_applies_locally() {
        let (_tmp, btree, transaction_manager, table) = open_index_table();
        apply_put_autocommit(
            &mut btree.write(),
            transaction_manager.as_ref(),
            b"k1",
            b"v1",
        )
        .unwrap();
        let mut cursor = TableCursor::new(
            table,
            btree.clone(),
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        );

        cursor.delete(b"k1").unwrap();

        assert_eq!(
            get_version(&mut btree.write(), b"k1", TXN_NONE).unwrap(),
            None
        );
    }

    #[test]
    fn read_only_cursor_rejects_writes() {
        let (_tmp, btree, transaction_manager, table) = open_index_table();
        let mut cursor = TableCursor::new(
            table,
            btree,
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadOnly,
        );

        let err = cursor.insert(b"k1", b"v1").unwrap_err();
        assert!(err.to_string().contains("cursor is read-only"));
    }

    #[test]
    fn transaction_bound_cursor_falls_back_to_autocommit_after_transaction_handle_drops() {
        let (_tmp, btree, transaction_manager, table) = open_index_table();
        let txn_handle = Arc::new(Mutex::new(transaction_manager.begin_snapshot_txn()));
        let mut cursor = TableCursor::new(
            table,
            btree,
            transaction_manager,
            Some(Arc::downgrade(&txn_handle)),
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        );

        drop(txn_handle);

        cursor.insert(b"k1", b"v1").unwrap();
        assert_eq!(cursor.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }
}
