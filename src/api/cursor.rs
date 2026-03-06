use std::sync::Arc;

use parking_lot::RwLock;

use crate::hooks::MutationHooks;
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
    mutation_hooks: Arc<dyn MutationHooks>,
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl Cursor {
    pub(crate) fn new(
        table: Arc<RwLock<Table>>,
        kind: CursorKind,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Self {
        Self {
            table,
            kind,
            mutation_hooks,
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
        let store_name = {
            let mut table = self.table.write();
            if table.get_version(key, txn_id)?.is_some() {
                return Err(crate::core::errors::DocumentValidationError(
                    "duplicate key error".into(),
                )
                .into());
            }
            table.store_name().to_string()
        };
        self.mutation_hooks
            .before_put(&store_name, key, value, txn_id)?;
        if !self.mutation_hooks.should_apply_locally() {
            return Ok(());
        }
        let mut table = self.table.write();
        if table.get_version(key, txn_id)?.is_some() {
            return Err(
                crate::core::errors::DocumentValidationError("duplicate key error".into()).into(),
            );
        }
        table.local_apply_put_with_txn(key, value, txn_id)
    }

    pub fn update(&mut self, key: &[u8], value: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let (store_name, exists) = {
            let mut table = self.table.write();
            (
                table.store_name().to_string(),
                table.get_version(key, txn_id)?.is_some(),
            )
        };
        if !exists {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        self.mutation_hooks
            .before_put(&store_name, key, value, txn_id)?;
        if !self.mutation_hooks.should_apply_locally() {
            return Ok(());
        }
        let mut table = self.table.write();
        if table.get_version(key, txn_id)?.is_none() {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for update".to_string(),
            )));
        }
        table.local_apply_put_with_txn(key, value, txn_id)
    }

    pub fn delete(&mut self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let (store_name, exists) = {
            let mut table = self.table.write();
            (
                table.store_name().to_string(),
                table.get_version(key, txn_id)?.is_some(),
            )
        };
        if !exists {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        self.mutation_hooks
            .before_delete(&store_name, key, txn_id)?;
        if !self.mutation_hooks.should_apply_locally() {
            return Ok(());
        }
        let mut table = self.table.write();
        if table.get_version(key, txn_id)?.is_none() {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
        let result = table.local_apply_delete_with_txn(key, txn_id)?;
        if !result {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "key not found for delete".to_string(),
            )));
        }
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

    fn ensure_writable(&self) -> Result<(), WrongoDBError> {
        if self.kind == CursorKind::Index {
            return Err(WrongoDBError::Storage(crate::core::errors::StorageError(
                "index cursors are read-only".to_string(),
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::hooks::MutationHooks;
    use crate::txn::{GlobalTxnState, TransactionManager, TXN_NONE};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HookOp {
        Put {
            store_name: String,
            key: Vec<u8>,
            value: Vec<u8>,
            txn_id: TxnId,
        },
        Delete {
            store_name: String,
            key: Vec<u8>,
            txn_id: TxnId,
        },
    }

    #[derive(Debug)]
    struct MockHooks {
        apply_locally: bool,
        ops: Mutex<Vec<HookOp>>,
    }

    impl MockHooks {
        fn new(apply_locally: bool) -> Self {
            Self {
                apply_locally,
                ops: Mutex::new(Vec::new()),
            }
        }
    }

    impl MutationHooks for MockHooks {
        fn before_put(
            &self,
            store_name: &str,
            key: &[u8],
            value: &[u8],
            txn_id: TxnId,
        ) -> Result<(), WrongoDBError> {
            self.ops.lock().push(HookOp::Put {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            });
            Ok(())
        }

        fn before_delete(
            &self,
            store_name: &str,
            key: &[u8],
            txn_id: TxnId,
        ) -> Result<(), WrongoDBError> {
            self.ops.lock().push(HookOp::Delete {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                txn_id,
            });
            Ok(())
        }

        fn should_apply_locally(&self) -> bool {
            self.apply_locally
        }
    }

    fn open_index_table() -> (TempDir, Arc<RwLock<Table>>) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("cursor.idx.wt");
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table = Arc::new(RwLock::new(
            Table::open_or_create_index(path, transaction_manager).unwrap(),
        ));
        (tmp, table)
    }

    #[test]
    fn insert_calls_hooks_and_skips_local_apply_when_disabled() {
        let hooks = Arc::new(MockHooks::new(false));
        let (_tmp, table) = open_index_table();
        let mut cursor = Cursor::new(table.clone(), CursorKind::Table, hooks.clone());

        cursor.insert(b"k1", b"v1", TXN_NONE).unwrap();

        let ops = hooks.ops.lock();
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], HookOp::Put { .. }));
        drop(ops);
        assert_eq!(table.write().get_version(b"k1", TXN_NONE).unwrap(), None);
    }

    #[test]
    fn delete_calls_hooks_and_applies_locally_when_enabled() {
        let hooks = Arc::new(MockHooks::new(true));
        let (_tmp, table) = open_index_table();
        table
            .write()
            .local_apply_put_with_txn(b"k1", b"v1", TXN_NONE)
            .unwrap();
        let mut cursor = Cursor::new(table.clone(), CursorKind::Table, hooks.clone());

        cursor.delete(b"k1", TXN_NONE).unwrap();

        let ops = hooks.ops.lock();
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], HookOp::Delete { .. }));
        drop(ops);
        assert_eq!(table.write().get_version(b"k1", TXN_NONE).unwrap(), None);
    }
}
