use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::storage::table::Table;
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

pub type CursorEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone)]
struct TrackedTxn {
    txn_id: TxnId,
    touched_stores: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StoreWriteTracker {
    active_txn: Arc<Mutex<Option<TrackedTxn>>>,
}

impl StoreWriteTracker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn begin(&self, txn_id: TxnId, touched_stores: Arc<Mutex<HashSet<String>>>) {
        *self.active_txn.lock() = Some(TrackedTxn {
            txn_id,
            touched_stores,
        });
    }

    pub(crate) fn clear(&self) {
        *self.active_txn.lock() = None;
    }

    fn record(&self, store_name: &str, txn_id: TxnId) {
        if txn_id == TXN_NONE {
            return;
        }
        let active = self.active_txn.lock().clone();
        let Some(active) = active else {
            return;
        };
        if active.txn_id == txn_id {
            active.touched_stores.lock().insert(store_name.to_string());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorWriteAccess {
    ReadOnly,
    ReadWrite,
}

pub struct Cursor {
    table: Arc<RwLock<Table>>,
    store_name: String,
    write_tracker: StoreWriteTracker,
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
        store_name: String,
        write_tracker: StoreWriteTracker,
        write_access: CursorWriteAccess,
    ) -> Self {
        Self {
            table,
            store_name,
            write_tracker,
            write_access,
            buffered_entries: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
            range_start: None,
            range_end: None,
        }
    }

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
            "cursor writes are not available when durability defers apply; use Session::insert/update/delete"
                .into(),
        )))
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let mut table = self.table.write();
        if table.contains_key(key, txn_id)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }
        table.local_apply_put_with_txn(key, value, txn_id)?;
        drop(table);
        self.write_tracker.record(&self.store_name, txn_id);
        Ok(())
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let mut table = self.table.write();
        if !table.contains_key(key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for update".to_string(),
            )));
        }
        table.local_apply_put_with_txn(key, value, txn_id)?;
        drop(table);
        self.write_tracker.record(&self.store_name, txn_id);
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let mut table = self.table.write();
        if !table.contains_key(key, txn_id)? {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        let deleted = table.local_apply_delete_with_txn(key, txn_id)?;
        if !deleted {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        drop(table);
        self.write_tracker.record(&self.store_name, txn_id);
        Ok(())
    }

    pub fn get(&mut self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut table = self.table.write();
        table.get_version(key, txn_id)
    }

    pub fn next(&mut self, txn_id: TxnId) -> Result<Option<CursorEntry>, WrongoDBError> {
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
            StoreWriteTracker::new(),
            CursorWriteAccess::ReadWrite,
        );

        cursor.insert(b"k1", b"v1", TXN_NONE).unwrap();

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
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();
        let mut cursor = Cursor::new(
            table.clone(),
            store_name,
            StoreWriteTracker::new(),
            CursorWriteAccess::ReadWrite,
        );

        cursor.delete(b"k1", TXN_NONE).unwrap();

        assert_eq!(table.write().get_version(b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn write_tracker_records_touched_store_for_transactional_writes() {
        let (_tmp, table, store_name) = open_index_table();
        let touched_stores = Arc::new(Mutex::new(HashSet::new()));
        let write_tracker = StoreWriteTracker::new();
        write_tracker.begin(7, touched_stores.clone());
        let mut cursor = Cursor::new(
            table,
            store_name.clone(),
            write_tracker,
            CursorWriteAccess::ReadWrite,
        );

        cursor.insert(b"k1", b"v1", 7).unwrap();

        assert!(touched_stores.lock().contains(&store_name));
    }

    #[test]
    fn read_only_cursor_rejects_writes() {
        let (_tmp, table, store_name) = open_index_table();
        let mut cursor = Cursor::new(
            table,
            store_name,
            StoreWriteTracker::new(),
            CursorWriteAccess::ReadOnly,
        );

        let err = cursor.insert(b"k1", b"v1", TXN_NONE).unwrap_err();
        assert!(err
            .to_string()
            .contains("cursor writes are not available when durability defers apply"));
    }
}
