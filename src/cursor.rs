use std::sync::Arc;
use parking_lot::RwLock;
use crate::storage::table::Table;
use crate::txn::NonTransactional;
use crate::WrongoDBError;

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

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        table.insert(key, value)
    }

    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        if !table.update(key, value)? {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        if !table.delete(key)? {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut table = self.table.write();
        table.get(key, &NonTransactional)
    }

    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut table = self.table.write();

        let start_key = self.current_key.as_deref();
        let entries = table.scan(&NonTransactional)?;

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

    pub fn reset(&mut self) {
        self.current_key = None;
    }
}
