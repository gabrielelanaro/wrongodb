use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::core::lock_stats::{begin_lock_hold, record_lock_wait, LockStatKind};
use crate::storage::table::Table;
use crate::txn::TxnId;
use crate::WrongoDBError;

type CursorEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorKind {
    Table,
    Index,
}

pub struct Cursor {
    table: Arc<RwLock<Table>>,
    kind: CursorKind,
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl Cursor {
    pub(crate) fn new(table: Arc<RwLock<Table>>, kind: CursorKind) -> Self {
        Self {
            table,
            kind,
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

    pub fn insert(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        {
            let wait_start = Instant::now();
            let table = self.table.read();
            record_lock_wait(LockStatKind::Table, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::Table);

            if !table.base_may_have_keys() {
                if !table.insert_mvcc_if_absent(key, value, txn_id)? {
                    return Err(crate::core::errors::DocumentValidationError(
                        "duplicate key error".into(),
                    )
                    .into());
                }
                return Ok(());
            }
        }

        let duplicate = {
            let wait_start = Instant::now();
            let mut table = self.table.write();
            record_lock_wait(LockStatKind::Table, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::Table);
            table.get_version(key, txn_id)?.is_some()
        };
        if duplicate {
            return Err(
                crate::core::errors::DocumentValidationError("duplicate key error".into()).into(),
            );
        }

        let wait_start = Instant::now();
        let table = self.table.read();
        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::Table);
        if !table.insert_mvcc_if_absent(key, value, txn_id)? {
            return Err(
                crate::core::errors::DocumentValidationError("duplicate key error".into()).into(),
            );
        }
        Ok(())
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let wait_start = Instant::now();
        let table = self.table.read();
        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::Table);
        let result = table.update_mvcc(key, value, txn_id)?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let wait_start = Instant::now();
        let table = self.table.read();
        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::Table);
        let result = table.delete_mvcc(key, txn_id)?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }

    pub fn get(&mut self, key: &[u8], txn_id: TxnId) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let wait_start = Instant::now();
        let mut table = self.table.write();
        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::Table);
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
        let wait_start = Instant::now();
        let mut table = self.table.write();
        record_lock_wait(LockStatKind::Table, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::Table);

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

    fn ensure_writable(&self) -> Result<(), WrongoDBError> {
        if self.kind == CursorKind::Index {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "index cursors are read-only".to_string(),
            )));
        }
        Ok(())
    }
}
