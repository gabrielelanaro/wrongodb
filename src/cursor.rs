use std::sync::Arc;

use crate::data::table::Table;
use parking_lot::RwLock;

pub struct Cursor {
    table: Arc<RwLock<Table>>,
    kind: CursorKind,
}

pub enum CursorKind {
    Table,
}

impl Cursor {
    pub(crate) fn new_table(table: Arc<RwLock<Table>>) -> Self {
        Self {
            table,
            kind: CursorKind::Table,
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), crate::WrongoDBError> {
        let mut table = self.table.write();
        match &self.kind {
            CursorKind::Table => {
                table.insert(key, value)?;
            }
        }
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, crate::WrongoDBError> {
        let mut table = self.table.write();
        match &self.kind {
            CursorKind::Table => {
                table.get_non_txn(key)
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, crate::WrongoDBError> {
        let mut table = self.table.write();
        match &self.kind {
            CursorKind::Table => {
                table.delete(key)
            }
        }
    }

    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, crate::WrongoDBError> {
        Err(crate::WrongoDBError::Storage(crate::core::errors::StorageError(
            "next not implemented yet".to_string(),
        )))
    }

    pub fn reset(&mut self) -> Result<(), crate::WrongoDBError> {
        Err(crate::WrongoDBError::Storage(crate::core::errors::StorageError(
            "reset not implemented yet".to_string(),
        )))
    }
}
