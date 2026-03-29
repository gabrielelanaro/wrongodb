use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::api::session::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::reserved_store::StoreId;
use crate::storage::table::{contains_key, get_version, scan_range};
use crate::txn::TxnId;
use crate::WrongoDBError;

use super::{CursorEntry, TableCursorState, TableCursorWriteAccess};

/// Logical cursor over one managed `file:` object.
///
/// `FileCursor` exposes the low-level single-store API without leaking the raw
/// [`BTreeCursor`] or transaction primitives. Reads and writes operate against
/// exactly one managed file object in the session's current transaction
/// context.
pub struct FileCursor<'session> {
    session: &'session Session,
    file_uri: String,
    store_id: StoreId,
    store: Arc<RwLock<BTreeCursor>>,
    state: Arc<Mutex<TableCursorState>>,
    write_access: TableCursorWriteAccess,
}

impl<'session> FileCursor<'session> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        session: &'session Session,
        file_uri: impl Into<String>,
        store_id: StoreId,
        store: Arc<RwLock<BTreeCursor>>,
        write_access: TableCursorWriteAccess,
        state: Arc<Mutex<TableCursorState>>,
    ) -> Self {
        Self {
            session,
            file_uri: file_uri.into(),
            store_id,
            store,
            state,
            write_access,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn set_range(&mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) {
        let mut state = self.state.lock();
        state.range_start = start;
        state.range_end = end;
        state.reset_runtime();
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;

        self.session.with_write_transaction(|txn_id, txn| {
            let mut store = self.store.write();
            if contains_key(&mut store, key, txn_id)? {
                return Err(DocumentValidationError("duplicate key error".into()).into());
            }
            store.put(self.store_id, key, value, txn)
        })?;

        self.reset();
        Ok(())
    }

    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;

        self.session.with_write_transaction(|txn_id, txn| {
            let mut store = self.store.write();
            if !contains_key(&mut store, key, txn_id)? {
                return Err(WrongoDBError::Storage(StorageError(
                    "key not found for update".to_string(),
                )));
            }
            store.put(self.store_id, key, value, txn)
        })?;

        self.reset();
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;

        self.session.with_write_transaction(|txn_id, txn| {
            let mut store = self.store.write();
            if !contains_key(&mut store, key, txn_id)? {
                return Err(WrongoDBError::Storage(StorageError(
                    "key not found for delete".to_string(),
                )));
            }
            store.delete(self.store_id, key, txn).map(|_| ())
        })?;

        self.reset();
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.visible_value(key, self.current_txn_id())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<CursorEntry>, WrongoDBError> {
        let txn_id = self.current_txn_id();
        let needs_refill = {
            let state = self.state.lock();
            if state.exhausted {
                return Ok(None);
            }
            state.buffer_pos >= state.buffered_entries.len()
        };

        if needs_refill {
            self.refill_buffer(txn_id)?;

            let mut state = self.state.lock();
            if state.buffered_entries.is_empty() {
                state.exhausted = true;
                return Ok(None);
            }
        }

        let mut state = self.state.lock();
        if state.buffer_pos < state.buffered_entries.len() {
            let entry = state.buffered_entries[state.buffer_pos].clone();
            state.buffer_pos += 1;
            Ok(Some(entry))
        } else {
            state.exhausted = true;
            Ok(None)
        }
    }

    pub fn reset(&mut self) {
        self.state.lock().reset_runtime();
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn ensure_writable(&self) -> Result<(), WrongoDBError> {
        if self.write_access == TableCursorWriteAccess::ReadWrite {
            return Ok(());
        }
        Err(WrongoDBError::Storage(StorageError(
            "cursor is read-only in this context".into(),
        )))
    }

    fn current_txn_id(&self) -> TxnId {
        self.session.current_txn_id()
    }

    fn visible_value(&self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut store = self.store.write();
        get_version(&mut store, key, txn_id)
    }

    fn refill_buffer(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let (start_key, range_end, skip_start) = {
            let state = self.state.lock();
            let resumed_key = state.buffered_entries.last().map(|(key, _)| key.clone());
            let skip_start = resumed_key.is_some();
            let start_key = resumed_key.or_else(|| state.range_start.clone());
            (start_key, state.range_end.clone(), skip_start)
        };

        let mut store = self.store.write();
        let entries = scan_range(
            &mut store,
            start_key.as_deref(),
            range_end.as_deref(),
            txn_id,
        )?;

        let buffered_entries = if skip_start && !entries.is_empty() {
            if let Some(start) = start_key.as_deref() {
                let mut iter = entries.into_iter();
                let first = iter.next();
                if let Some((key, value)) = first {
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
                }
            } else {
                entries
            }
        } else {
            entries
        };

        let mut state = self.state.lock();
        state.buffered_entries = buffered_entries;
        state.buffer_pos = 0;
        Ok(())
    }
}

impl std::fmt::Debug for FileCursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCursor")
            .field("file_uri", &self.file_uri)
            .field("store_id", &self.store_id)
            .field("write_access", &self.write_access)
            .finish_non_exhaustive()
    }
}
