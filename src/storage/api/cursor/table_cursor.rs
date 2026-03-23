use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::api::session::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::row::{index_key_from_decoded_row, DecodedRow};
use crate::storage::table::{contains_key, get_version, scan_range, IndexMetadata, TableMetadata};
use crate::txn::TxnId;
use crate::WrongoDBError;

use super::{CursorEntry, IndexEntry, TableCursorState, TableCursorWriteAccess};

/// Logical table cursor over one primary store and its secondary indexes.
///
/// `TableCursor` is the storage-layer table abstraction. Reads and scans operate
/// over the primary B-tree only, while writes keep the configured secondary
/// indexes in sync inside the same transaction/WAL context.
pub struct TableCursor<'session> {
    session: &'session Session,
    table: TableMetadata,
    primary: Arc<RwLock<BTreeCursor>>,
    indexes: Vec<Arc<RwLock<BTreeCursor>>>,
    state: Arc<Mutex<TableCursorState>>,
    write_access: TableCursorWriteAccess,
}

impl<'session> TableCursor<'session> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        session: &'session Session,
        table: TableMetadata,
        primary: Arc<RwLock<BTreeCursor>>,
        indexes: Vec<Arc<RwLock<BTreeCursor>>>,
        write_access: TableCursorWriteAccess,
        state: Arc<Mutex<TableCursorState>>,
    ) -> Self {
        Self {
            session,
            table,
            primary,
            indexes,
            state,
            write_access,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Restrict iteration to the given half-open key range.
    ///
    /// Range state lives on the cursor so scans can be resumed incrementally
    /// without pushing iterator state into `Session`.
    ///
    /// The method exists on `TableCursor` because scan position is cursor-local
    /// state, not session state.
    pub fn set_range(&mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) {
        let mut state = self.state.lock();
        state.range_start = start;
        state.range_end = end;
        state.reset_runtime();
    }

    /// Insert a new primary row and its derived secondary index entries.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let index_entries = self.collect_index_entries(key, value)?;

        self.session.with_write_transaction(|txn_id, txn| {
            {
                let mut primary = self.primary.write();
                if contains_key(&mut primary, key, txn_id)? {
                    return Err(DocumentValidationError("duplicate key error".into()).into());
                }
                primary.put(self.table.store_id(), key, value, txn)?;
            }

            self.insert_secondary_indexes(&index_entries, txn_id, txn)
        })?;

        self.reset();
        Ok(())
    }

    /// Replace the primary value for an existing key and rewrite its index entries.
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let new_index_entries = self.collect_index_entries(key, value)?;

        self.session.with_write_transaction(|txn_id, txn| {
            let old_value = self.visible_primary_value(key, txn_id)?.ok_or_else(|| {
                WrongoDBError::Storage(StorageError("key not found for update".to_string()))
            })?;
            let old_index_entries = self.collect_index_entries(key, &old_value)?;

            self.delete_secondary_indexes(&old_index_entries, txn_id, txn)?;

            {
                let mut primary = self.primary.write();
                primary.put(self.table.store_id(), key, value, txn)?;
            }

            self.insert_secondary_indexes(&new_index_entries, txn_id, txn)
        })?;

        self.reset();
        Ok(())
    }

    /// Delete an existing primary row and its derived secondary index entries.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;

        self.session.with_write_transaction(|txn_id, txn| {
            let old_value = self.visible_primary_value(key, txn_id)?.ok_or_else(|| {
                WrongoDBError::Storage(StorageError("key not found for delete".to_string()))
            })?;
            let old_index_entries = self.collect_index_entries(key, &old_value)?;

            self.delete_secondary_indexes(&old_index_entries, txn_id, txn)?;

            let mut primary = self.primary.write();
            primary.delete(self.table.store_id(), key, txn).map(|_| ())
        })?;

        self.reset();
        Ok(())
    }

    /// Fetch the visible primary value for `key` in the cursor's current transaction context.
    ///
    /// The method lives here because visibility is still a one-store,
    /// one-transaction question.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.visible_primary_value(key, self.current_txn_id())
    }

    #[allow(clippy::should_implement_trait)]
    /// Return the next visible primary key/value pair in the configured range.
    ///
    /// Iteration is cursor-owned so callers can scan a store incrementally
    /// without materializing all results in `Session`.
    ///
    /// Keeping this state on the cursor is what makes the cursor abstraction
    /// useful in the first place.
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

    /// Reset the cursor position to the beginning.
    ///
    /// This keeps cursor state reusable for repeated scans over the same store
    /// without reopening the cursor.
    ///
    /// The method exists because cursor position is mutable operational state,
    /// not part of the store itself.
    pub fn reset(&mut self) {
        self.state.lock().reset_runtime();
    }

    pub(crate) fn table(&self) -> &TableMetadata {
        &self.table
    }

    // ------------------------------------------------------------------------
    // Transaction helpers
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

    // ------------------------------------------------------------------------
    // Primary-store reads
    // ------------------------------------------------------------------------

    fn visible_primary_value(
        &self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut primary = self.primary.write();
        get_version(&mut primary, key, txn_id)
    }

    fn refill_buffer(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let (start_key, range_end, skip_start) = {
            let state = self.state.lock();
            let resumed_key = state.buffered_entries.last().map(|(key, _)| key.clone());
            let skip_start = resumed_key.is_some();
            let start_key = resumed_key.or_else(|| state.range_start.clone());
            (start_key, state.range_end.clone(), skip_start)
        };

        let mut primary = self.primary.write();
        let entries = scan_range(
            &mut primary,
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

    // ------------------------------------------------------------------------
    // Secondary-index maintenance
    // ------------------------------------------------------------------------

    fn collect_index_entries(
        &self,
        primary_key: &[u8],
        primary_value: &[u8],
    ) -> Result<Vec<IndexEntry>, WrongoDBError> {
        if self.indexes.is_empty() {
            return Ok(Vec::new());
        }

        let row = DecodedRow::from_bytes(
            self.table.row_format(),
            self.table.value_columns(),
            primary_value,
        )?;
        let mut entries = Vec::new();

        for (index_pos, metadata) in self.table.indexes().iter().enumerate() {
            let Some(index_key) = self.index_key_for_row(metadata, primary_key, &row)? else {
                continue;
            };
            entries.push((index_pos, index_key));
        }

        Ok(entries)
    }

    fn index_key_for_row(
        &self,
        metadata: &IndexMetadata,
        primary_key: &[u8],
        row: &DecodedRow,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        index_key_from_decoded_row(&self.table, metadata, primary_key, row)
    }

    fn insert_secondary_indexes(
        &self,
        entries: &[IndexEntry],
        _txn_id: TxnId,
        txn: &mut crate::txn::Transaction,
    ) -> Result<(), WrongoDBError> {
        for (index_pos, key) in entries {
            let metadata = &self.table.indexes()[*index_pos];
            let mut btree = self.indexes[*index_pos].write();
            btree.put(metadata.store_id(), key, &[], txn)?;
        }
        Ok(())
    }

    fn delete_secondary_indexes(
        &self,
        entries: &[IndexEntry],
        txn_id: TxnId,
        txn: &mut crate::txn::Transaction,
    ) -> Result<(), WrongoDBError> {
        for (index_pos, key) in entries {
            let metadata = &self.table.indexes()[*index_pos];
            let mut btree = self.indexes[*index_pos].write();
            if !contains_key(&mut btree, key, txn_id)? {
                continue;
            }
            btree.delete(metadata.store_id(), key, txn)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for TableCursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCursor")
            .field("table", &self.table)
            .field("write_access", &self.write_access)
            .finish_non_exhaustive()
    }
}
