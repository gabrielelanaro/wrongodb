use std::sync::Arc;
use parking_lot::RwLock;
use crate::storage::table::Table;
use crate::txn::Transaction;
use crate::WrongoDBError;

/// Cursor for reading and writing to a table.
/// 
/// Cursors can be created with or without an active transaction:
/// - Without transaction: Reads see committed data, writes fail
/// - With transaction: Reads use MVCC visibility, writes track modifications
/// 
/// For writes, you need to pass &mut Transaction explicitly.
pub struct Cursor {
    table: Arc<RwLock<Table>>,
    current_key: Option<Vec<u8>>,
}

impl Cursor {
    pub(crate) fn new(table: Arc<RwLock<Table>>) -> Self {
        Self {
            table,
            current_key: None,
        }
    }

    /// Insert a key/value pair.
    /// 
    /// Requires a mutable transaction reference.
    pub fn insert(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        table.insert_mvcc(key, value, txn)
    }

    /// Update a key/value pair.
    /// 
    /// Requires a mutable transaction reference.
    pub fn update(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        let result = table.update_mvcc(key, value, txn)?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        Ok(())
    }

    /// Delete a key.
    /// 
    /// Requires a mutable transaction reference.
    pub fn delete(&mut self, key: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        let result = table.delete_mvcc(key, txn)?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    /// Get a value by key.
    /// 
    /// Uses the provided transaction for MVCC visibility.
    pub fn get(&mut self, key: &[u8], txn: &Transaction) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut table = self.table.write();
        table.get_mvcc(key, txn)
    }

    /// Move to the next key/value pair.
    /// 
    /// Uses the provided transaction for MVCC visibility.
    pub fn next(&mut self, txn: &Transaction) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut table = self.table.write();

        let start_key = self.current_key.as_deref();
        let entries = table.scan_txn(txn)?;

        let mut iter = entries.into_iter();
        if let Some(current_key) = start_key {
            while let Some((k, _)) = iter.next() {
                if k == current_key {
                    break;
                }
            }
        }

        if let Some((k, v)) = iter.next() {
            self.current_key = Some(k.clone());
            Ok(Some((k, v)))
        } else {
            Ok(None)
        }
    }

    /// Reset the cursor position.
    pub fn reset(&mut self) {
        self.current_key = None;
    }
}