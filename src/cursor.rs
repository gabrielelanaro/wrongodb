use std::sync::Arc;
use parking_lot::RwLock;
use crate::storage::table::Table;
use crate::txn::Transaction;
use crate::WrongoDBError;

pub struct Cursor {
    table: Arc<RwLock<Table>>,
    buffered_entries: Vec<(Vec<u8>, Vec<u8>)>,
    buffer_pos: usize,
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

    pub fn insert(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        if table.get_version(key, txn.id())?.is_some() {
            return Err(crate::core::errors::DocumentValidationError("duplicate key error".into()).into());
        }
        table.insert_mvcc(key, value, txn.id())
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        let result = table.update_mvcc(key, value, txn.id())?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], txn: &mut Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        let result = table.delete_mvcc(key, txn.id())?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    pub fn get(&mut self, key: &[u8], txn: &Transaction) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut table = self.table.write();
        table.get_version(key, txn.id())
    }

    pub fn next(&mut self, txn: &Transaction) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        if self.exhausted {
            return Ok(None);
        }

        if self.buffer_pos >= self.buffered_entries.len() {
            self.refill_buffer(txn)?;

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

    fn refill_buffer(&mut self, txn: &Transaction) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();

        let start_key = if !self.buffered_entries.is_empty() {
            self.buffered_entries.last().map(|(k, _)| k.as_slice())
        } else {
            None
        };

        let entries = table.scan_from(start_key, txn.id())?;

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