use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::table::Table;
use crate::storage::RecoveryUnit;
use crate::txn::{Transaction, TxnId};
use crate::WrongoDBError;

/// A single key/value entry returned by [`Cursor::next`].
///
/// The public cursor surface is deliberately key/value-shaped because it is the
/// low-level storage API, not the document/query API.
///
/// This type alias exists to keep that low-level API readable without
/// introducing a heavier public wrapper type.
pub type CursorEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorWriteAccess {
    #[cfg(test)]
    ReadOnly,
    ReadWrite,
}

/// Cursor over a single physical store.
///
/// A cursor is always bound to one store and one transaction context. Cursors
/// opened from [`Session`](crate::Session) are non-transactional; cursors
/// opened from [`WriteUnitOfWork`](crate::WriteUnitOfWork) are bound to the
/// active transaction.
///
/// The reason this type exists publicly is to expose a small, explicit
/// storage interface for one store at a time. It is intentionally not
/// responsible for collection semantics or commit/abort orchestration.
///
/// If a caller needs document-level writes, they should go through the
/// higher-level command path, not extend this type.
pub struct Cursor {
    table: Arc<RwLock<Table>>,
    uri: String,
    bound_txn_id: TxnId,
    active_txn: Option<Weak<Mutex<Transaction>>>,
    recovery_unit: Arc<dyn RecoveryUnit>,
    write_access: CursorWriteAccess,
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl Cursor {
    pub(crate) fn new(
        table: Arc<RwLock<Table>>,
        uri: String,
        bound_txn_id: TxnId,
        active_txn: Option<Weak<Mutex<Transaction>>>,
        recovery_unit: Arc<dyn RecoveryUnit>,
        write_access: CursorWriteAccess,
    ) -> Self {
        Self {
            table,
            uri,
            bound_txn_id,
            active_txn,
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
    /// The method exists on `Cursor` because scan position is cursor-local
    /// state, not session state.
    pub fn set_range(&mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) {
        self.range_start = start;
        self.range_end = end;
        self.reset();
    }

    fn ensure_writable(&self) -> Result<(), WrongoDBError> {
        if self.write_access == CursorWriteAccess::ReadWrite {
            return Ok(());
        }
        Err(WrongoDBError::Storage(StorageError(
            "cursor is read-only in this context".into(),
        )))
    }

    fn apply_put(
        &self,
        table: &mut Table,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        match &self.active_txn {
            Some(active_txn) => {
                let txn_handle = active_txn.upgrade().ok_or_else(|| {
                    StorageError(format!(
                        "transaction cursor for txn {txn_id} is no longer active"
                    ))
                })?;
                let mut txn = txn_handle.lock();
                table.local_apply_put_in_txn(key, value, &mut txn)
            }
            None => table.local_apply_put_autocommit(key, value),
        }
    }

    fn apply_delete(
        &self,
        table: &mut Table,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        match &self.active_txn {
            Some(active_txn) => {
                let txn_handle = active_txn.upgrade().ok_or_else(|| {
                    StorageError(format!(
                        "transaction cursor for txn {txn_id} is no longer active"
                    ))
                })?;
                let mut txn = txn_handle.lock();
                table.local_apply_delete_in_txn(key, &mut txn)
            }
            None => table.local_apply_delete_autocommit(key),
        }
    }

    /// Insert a new key/value pair into the bound store.
    ///
    /// The cursor performs only store-local semantics here: duplicate-key
    /// validation, local durability recording for local write modes, and MVCC
    /// application through the bound transaction context.
    ///
    /// It exists on `Cursor` because insert is a one-store mutation. Higher
    /// layers remain responsible for multi-store work such as index
    /// maintenance.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let txn_id = self.bound_txn_id;
        self.ensure_writable()?;
        let mut table = self.table.write();
        if table.contains_key(key, txn_id)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }
        self.recovery_unit
            .record_put(&self.uri, key, value, txn_id)?;
        self.apply_put(&mut table, key, value, txn_id)?;
        Ok(())
    }

    /// Replace the value for an existing key in the bound store.
    ///
    /// Missing-key validation happens here because this is still a one-store
    /// operation. Higher-level document/index orchestration lives elsewhere.
    ///
    /// This separation keeps update semantics local to one store and prevents
    /// `Cursor` from turning into a document write controller.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let txn_id = self.bound_txn_id;
        self.ensure_writable()?;
        let mut table = self.table.write();
        if !table.contains_key(key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for update".to_string(),
            )));
        }
        self.recovery_unit
            .record_put(&self.uri, key, value, txn_id)?;
        self.apply_put(&mut table, key, value, txn_id)?;
        Ok(())
    }

    /// Delete an existing key from the bound store.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let txn_id = self.bound_txn_id;
        self.ensure_writable()?;
        let mut table = self.table.write();
        if !table.contains_key(key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        self.recovery_unit.record_delete(&self.uri, key, txn_id)?;
        let deleted = self.apply_delete(&mut table, key, txn_id)?;
        if !deleted {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    /// Fetch the visible value for `key` in the cursor's bound transaction.
    ///
    /// The method lives here because visibility is still a one-store,
    /// one-transaction question.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let txn_id = self.bound_txn_id;
        let mut table = self.table.write();
        table.get_version(key, txn_id)
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
        let txn_id = self.bound_txn_id;
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
        let mut table = self.table.write();

        let mut start_key = self.range_start.as_deref();
        let mut skip_start = false;
        if let Some((last_key, _)) = self.buffered_entries.last() {
            start_key = Some(last_key.as_slice());
            skip_start = true;
        }

        let entries = table.scan_range(start_key, self.range_end.as_deref(), txn_id)?;

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
    use crate::storage::NoopRecoveryUnit;
    use crate::txn::{GlobalTxnState, TransactionManager, TXN_NONE};

    fn open_index_table() -> (TempDir, Arc<RwLock<Table>>, String) {
        let tmp = tempdir().unwrap();
        let store_name = "cursor.idx.wt".to_string();
        let path = tmp.path().join(&store_name);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table = Arc::new(RwLock::new(
            Table::open_or_create_store(path, transaction_manager).unwrap(),
        ));
        (tmp, table, store_name)
    }

    #[test]
    fn insert_applies_locally() {
        let (_tmp, table, store_name) = open_index_table();
        let mut cursor = Cursor::new(
            table.clone(),
            store_name,
            TXN_NONE,
            None,
            Arc::new(NoopRecoveryUnit),
            CursorWriteAccess::ReadWrite,
        );

        cursor.insert(b"k1", b"v1").unwrap();

        assert_eq!(
            table.write().get_version(b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn delete_applies_locally() {
        let (_tmp, table, store_name) = open_index_table();
        table
            .write()
            .local_apply_put_autocommit(b"k1", b"v1")
            .unwrap();
        let mut cursor = Cursor::new(
            table.clone(),
            store_name,
            TXN_NONE,
            None,
            Arc::new(NoopRecoveryUnit),
            CursorWriteAccess::ReadWrite,
        );

        cursor.delete(b"k1").unwrap();

        assert_eq!(table.write().get_version(b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn read_only_cursor_rejects_writes() {
        let (_tmp, table, store_name) = open_index_table();
        let mut cursor = Cursor::new(
            table,
            store_name,
            TXN_NONE,
            None,
            Arc::new(NoopRecoveryUnit),
            CursorWriteAccess::ReadOnly,
        );

        let err = cursor.insert(b"k1", b"v1").unwrap_err();
        assert!(err.to_string().contains("cursor is read-only"));
    }

    #[test]
    fn transaction_bound_cursor_rejects_writes_after_transaction_handle_drops() {
        let (_tmp, table, store_name) = open_index_table();
        let txn_manager = Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let txn_handle = Arc::new(Mutex::new(txn_manager.begin_snapshot_txn()));
        let txn_id = txn_handle.lock().id();
        let mut cursor = Cursor::new(
            table,
            store_name,
            txn_id,
            Some(Arc::downgrade(&txn_handle)),
            Arc::new(NoopRecoveryUnit),
            CursorWriteAccess::ReadWrite,
        );

        drop(txn_handle);

        let err = cursor.insert(b"k1", b"v1").unwrap_err();
        assert!(err.to_string().contains("transaction cursor for txn"));
    }
}
