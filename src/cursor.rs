use std::sync::Arc;
use parking_lot::RwLock;
use crate::storage::table::Table;
use crate::txn::Transaction;
use crate::WrongoDBError;

/// Cursor for reading and writing to a table.
///
/// Cursors maintain position state for efficient iteration and provide
/// the primary interface for CRUD operations within a transaction context.
///
/// The cursor maintains:
/// - Current position in the BTree (for resumable iteration)
/// - Buffered entries for efficient MVCC filtering during scans
/// - Reference to the underlying table for data operations
///
/// For all operations, you need to pass a Transaction reference explicitly.
pub struct Cursor {
    table: Arc<RwLock<Table>>,
    /// Buffered entries from the current scan position
    buffered_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position in the buffer
    buffer_pos: usize,
    /// Whether we've reached the end of the table
    exhausted: bool,
}

impl Cursor {
    pub(crate) fn new(table: Arc<RwLock<Table>>) -> Self {
        Self {
            table,
            buffered_entries: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
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
    /// Maintains position state for efficient resumable iteration.
    pub fn next(&mut self, txn: &Transaction) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        // If we've exhausted the table, return None
        if self.exhausted {
            return Ok(None);
        }

        // If buffer is empty or we've consumed it, refill from table
        if self.buffer_pos >= self.buffered_entries.len() {
            self.refill_buffer(txn)?;

            // Check if we got any entries
            if self.buffered_entries.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }

            self.buffer_pos = 0;
        }

        // Return the next entry from buffer
        if self.buffer_pos < self.buffered_entries.len() {
            let entry = self.buffered_entries[self.buffer_pos].clone();
            self.buffer_pos += 1;
            Ok(Some(entry))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }

    /// Refill the entry buffer from the table.
    ///
    /// This fetches the next batch of entries visible to the transaction.
    fn refill_buffer(&mut self, txn: &Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();

        // Get the last key we returned to resume from that position
        let start_key = if !self.buffered_entries.is_empty() {
            self.buffered_entries.last().map(|(k, _)| k.as_slice())
        } else {
            None
        };

        // Fetch next batch of entries from table
        let entries = table.scan_txn_from(start_key, txn)?;

        // Clear old buffer and store new entries
        self.buffered_entries = entries;
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