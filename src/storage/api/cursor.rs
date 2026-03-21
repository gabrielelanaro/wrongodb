use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::core::bson::{decode_document, decode_id_value};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::index::encode_index_key;
use crate::storage::btree::BTreeCursor;
use crate::storage::table::{
    apply_delete_in_txn, apply_put_in_txn, contains_key, get_version, scan_range, IndexMetadata,
    TableMetadata,
};
use crate::storage::RecoveryUnit;
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::{Document, WrongoDBError};

/// A single key/value entry returned by [`TableCursor::next`].
///
/// The public table cursor surface is deliberately key/value-shaped because it is the
/// low-level storage API, not the document/query API.
///
/// This type alias exists to keep that low-level API readable without
/// introducing a heavier public wrapper type.
pub type CursorEntry = (Vec<u8>, Vec<u8>);

type IndexEntry = (usize, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableCursorWriteAccess {
    #[cfg(test)]
    ReadOnly,
    ReadWrite,
}

/// Logical table cursor over one primary store and its secondary indexes.
///
/// `TableCursor` is the storage-layer table abstraction. Reads and scans operate
/// over the primary B-tree only, while writes keep the configured secondary
/// indexes in sync inside the same transaction/WAL context.
#[derive(Debug)]
pub struct TableCursor {
    table: TableMetadata,
    primary: Arc<RwLock<BTreeCursor>>,
    indexes: Vec<Arc<RwLock<BTreeCursor>>>,
    transaction_manager: Arc<TransactionManager>,
    txn_handle: Option<Weak<Mutex<Transaction>>>,
    recovery_unit: Arc<dyn RecoveryUnit>,
    write_access: TableCursorWriteAccess,
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl TableCursor {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        table: TableMetadata,
        primary: Arc<RwLock<BTreeCursor>>,
        indexes: Vec<Arc<RwLock<BTreeCursor>>>,
        transaction_manager: Arc<TransactionManager>,
        txn_handle: Option<Weak<Mutex<Transaction>>>,
        recovery_unit: Arc<dyn RecoveryUnit>,
        write_access: TableCursorWriteAccess,
    ) -> Result<Self, WrongoDBError> {
        if table.indexes().len() != indexes.len() {
            return Err(StorageError(format!(
                "table cursor index handle mismatch: metadata has {}, runtime has {}",
                table.indexes().len(),
                indexes.len()
            ))
            .into());
        }

        Ok(Self {
            table,
            primary,
            indexes,
            transaction_manager,
            txn_handle,
            recovery_unit,
            write_access,
            buffered_entries: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
            range_start: None,
            range_end: None,
        })
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
        self.range_start = start;
        self.range_end = end;
        self.reset();
    }

    /// Insert a new primary row and its derived secondary index entries.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;
        let index_entries = self.collect_index_entries(key, value)?;

        self.with_write_txn(|txn_id, txn| {
            {
                let mut primary = self.primary.write();
                if contains_key(&mut primary, key, txn_id)? {
                    return Err(DocumentValidationError("duplicate key error".into()).into());
                }
                self.record_and_put(self.table.uri(), &mut primary, key, value, txn_id, txn)?;
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

        self.with_write_txn(|txn_id, txn| {
            let old_value = self.visible_primary_value(key, txn_id)?.ok_or_else(|| {
                WrongoDBError::Storage(StorageError("key not found for update".to_string()))
            })?;
            let old_index_entries = self.collect_index_entries(key, &old_value)?;

            self.delete_secondary_indexes(&old_index_entries, txn_id, txn)?;

            {
                let mut primary = self.primary.write();
                self.record_and_put(self.table.uri(), &mut primary, key, value, txn_id, txn)?;
            }

            self.insert_secondary_indexes(&new_index_entries, txn_id, txn)
        })?;

        self.reset();
        Ok(())
    }

    /// Delete an existing primary row and its derived secondary index entries.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), WrongoDBError> {
        self.ensure_writable()?;

        self.with_write_txn(|txn_id, txn| {
            let old_value = self.visible_primary_value(key, txn_id)?.ok_or_else(|| {
                WrongoDBError::Storage(StorageError("key not found for delete".to_string()))
            })?;
            let old_index_entries = self.collect_index_entries(key, &old_value)?;

            self.delete_secondary_indexes(&old_index_entries, txn_id, txn)?;

            let mut primary = self.primary.write();
            self.record_and_delete(self.table.uri(), &mut primary, key, txn_id, txn)
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

    /// Reset the cursor position to the beginning.
    ///
    /// This keeps cursor state reusable for repeated scans over the same store
    /// without reopening the cursor.
    ///
    /// The method exists because cursor position is mutable operational state,
    /// not part of the store itself.
    pub fn reset(&mut self) {
        self.buffered_entries.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
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
        self.txn_handle
            .as_ref()
            .and_then(|handle| handle.upgrade())
            .map(|txn| txn.lock().id())
            .unwrap_or(TXN_NONE)
    }

    fn with_write_txn<R, F>(&self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(TxnId, &mut Transaction) -> Result<R, WrongoDBError>,
    {
        if let Some(txn_handle) = self.txn_handle.as_ref().and_then(|handle| handle.upgrade()) {
            let mut txn = txn_handle.lock();
            return f(txn.id(), &mut txn);
        }

        let mut txn = self.transaction_manager.begin_snapshot_txn();
        let txn_id = txn.id();
        if let Err(err) = self.recovery_unit.begin_unit_of_work() {
            let _ = self.transaction_manager.abort_txn_state(&mut txn);
            return Err(err);
        }

        let result = f(txn_id, &mut txn);
        match result {
            Ok(value) => {
                if let Err(err) = self.recovery_unit.commit_unit_of_work(txn_id, txn_id) {
                    let _ = self.transaction_manager.abort_txn_state(&mut txn);
                    return Err(err);
                }
                self.transaction_manager.commit_txn_state(&mut txn)?;
                Ok(value)
            }
            Err(err) => {
                let _ = self.recovery_unit.abort_unit_of_work(txn_id);
                let _ = self.transaction_manager.abort_txn_state(&mut txn);
                Err(err)
            }
        }
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
        let mut primary = self.primary.write();

        let mut start_key = self.range_start.as_deref();
        let mut skip_start = false;
        if let Some((last_key, _)) = self.buffered_entries.last() {
            start_key = Some(last_key.as_slice());
            skip_start = true;
        }

        let entries = scan_range(&mut primary, start_key, self.range_end.as_deref(), txn_id)?;

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

        let doc = decode_document(primary_value)?;
        let id = decode_id_value(primary_key)?;
        let mut entries = Vec::new();

        for (index_pos, metadata) in self.table.indexes().iter().enumerate() {
            let Some(index_key) = self.index_key_for_document(metadata, &doc, &id)? else {
                continue;
            };
            entries.push((index_pos, index_key));
        }

        Ok(entries)
    }

    fn index_key_for_document(
        &self,
        metadata: &IndexMetadata,
        doc: &Document,
        id: &serde_json::Value,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let field = metadata.columns().first().ok_or_else(|| {
            StorageError(format!(
                "index {} is missing indexed columns",
                metadata.uri()
            ))
        })?;
        if metadata.columns().len() != 1 {
            return Err(StorageError(format!(
                "index {} has unsupported composite definition with {} columns",
                metadata.uri(),
                metadata.columns().len()
            ))
            .into());
        }

        let Some(value) = doc.get(field) else {
            return Ok(None);
        };
        encode_index_key(value, id)
    }

    fn insert_secondary_indexes(
        &self,
        entries: &[IndexEntry],
        txn_id: TxnId,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        for (index_pos, key) in entries {
            let metadata = &self.table.indexes()[*index_pos];
            let mut btree = self.indexes[*index_pos].write();
            self.record_and_put(metadata.uri(), &mut btree, key, &[], txn_id, txn)?;
        }
        Ok(())
    }

    fn delete_secondary_indexes(
        &self,
        entries: &[IndexEntry],
        txn_id: TxnId,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        for (index_pos, key) in entries {
            let metadata = &self.table.indexes()[*index_pos];
            let mut btree = self.indexes[*index_pos].write();
            if !contains_key(&mut btree, key, txn_id)? {
                continue;
            }
            self.record_and_delete(metadata.uri(), &mut btree, key, txn_id, txn)?;
        }
        Ok(())
    }

    fn record_and_put(
        &self,
        uri: &str,
        btree: &mut BTreeCursor,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        self.recovery_unit.record_put(uri, key, value, txn_id)?;
        apply_put_in_txn(btree, key, value, txn)
    }

    fn record_and_delete(
        &self,
        uri: &str,
        btree: &mut BTreeCursor,
        key: &[u8],
        txn_id: TxnId,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        self.recovery_unit.record_delete(uri, key, txn_id)?;
        let deleted = apply_delete_in_txn(btree, key, txn)?;
        if !deleted {
            return Err(WrongoDBError::Storage(StorageError(
                "key not found for delete".to_string(),
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use serde_json::json;
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::core::bson::{encode_document, encode_id_value};
    use crate::storage::table::{apply_put_in_txn, open_or_create_btree};
    use crate::storage::NoopRecoveryUnit;
    use crate::txn::{GlobalTxnState, TXN_NONE};

    fn open_indexless_table() -> (
        TempDir,
        Arc<RwLock<BTreeCursor>>,
        Arc<TransactionManager>,
        TableMetadata,
    ) {
        let tmp = tempdir().unwrap();
        let uri = "table:test".to_string();
        let store_name = "cursor.main.wt".to_string();
        let path = tmp.path().join(&store_name);
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let btree = Arc::new(RwLock::new(open_or_create_btree(path).unwrap()));
        (tmp, btree, transaction_manager, TableMetadata::new(uri))
    }

    fn open_table_with_secondary_index() -> (
        TempDir,
        Arc<RwLock<BTreeCursor>>,
        Arc<RwLock<BTreeCursor>>,
        Arc<TransactionManager>,
        TableMetadata,
    ) {
        let tmp = tempdir().unwrap();
        let primary_path = tmp.path().join("users.main.wt");
        let index_path = tmp.path().join("users.name.idx.wt");
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let primary = Arc::new(RwLock::new(open_or_create_btree(primary_path).unwrap()));
        let index = Arc::new(RwLock::new(open_or_create_btree(index_path).unwrap()));
        let table = TableMetadata::with_indexes(
            "table:users",
            vec![IndexMetadata::new(
                "name",
                "index:users:name",
                vec!["name".to_string()],
            )],
        );
        (tmp, primary, index, transaction_manager, table)
    }

    fn make_document_bytes(id: i64, name: &str) -> Vec<u8> {
        encode_document(&serde_json::from_value(json!({"_id": id, "name": name})).unwrap()).unwrap()
    }

    fn index_key_for_name(id: i64, name: &str) -> Vec<u8> {
        encode_index_key(&json!(name), &json!(id)).unwrap().unwrap()
    }

    #[test]
    fn insert_applies_locally() {
        let (_tmp, btree, transaction_manager, table) = open_indexless_table();
        let mut cursor = TableCursor::new(
            table,
            btree.clone(),
            Vec::new(),
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        cursor.insert(b"k1", b"v1").unwrap();

        assert_eq!(
            get_version(&mut btree.write(), b"k1", TXN_NONE).unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn delete_applies_locally() {
        let (_tmp, btree, transaction_manager, table) = open_indexless_table();
        let mut seed_txn = transaction_manager.begin_snapshot_txn();
        apply_put_in_txn(&mut btree.write(), b"k1", b"v1", &mut seed_txn).unwrap();
        transaction_manager.commit_txn_state(&mut seed_txn).unwrap();

        let mut cursor = TableCursor::new(
            table,
            btree.clone(),
            Vec::new(),
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        cursor.delete(b"k1").unwrap();

        assert_eq!(
            get_version(&mut btree.write(), b"k1", TXN_NONE).unwrap(),
            None
        );
    }

    #[test]
    fn insert_writes_primary_and_secondary_index() {
        let (_tmp, primary, index, transaction_manager, table) = open_table_with_secondary_index();
        let mut cursor = TableCursor::new(
            table,
            primary.clone(),
            vec![index.clone()],
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");
        cursor.insert(&key, &value).unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, TXN_NONE).unwrap(),
            Some(value)
        );
        assert_eq!(
            get_version(
                &mut index.write(),
                &index_key_for_name(1, "alice"),
                TXN_NONE
            )
            .unwrap(),
            Some(Vec::new())
        );
    }

    #[test]
    fn update_rewrites_secondary_index() {
        let (_tmp, primary, index, transaction_manager, table) = open_table_with_secondary_index();
        let key = encode_id_value(&json!(1)).unwrap();
        let old_value = make_document_bytes(1, "alice");
        let new_value = make_document_bytes(1, "bob");

        let mut seed_txn = transaction_manager.begin_snapshot_txn();
        apply_put_in_txn(&mut primary.write(), &key, &old_value, &mut seed_txn).unwrap();
        apply_put_in_txn(
            &mut index.write(),
            &index_key_for_name(1, "alice"),
            &[],
            &mut seed_txn,
        )
        .unwrap();
        transaction_manager.commit_txn_state(&mut seed_txn).unwrap();

        let mut cursor = TableCursor::new(
            table,
            primary.clone(),
            vec![index.clone()],
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        cursor.update(&key, &new_value).unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, TXN_NONE).unwrap(),
            Some(new_value)
        );
        assert_eq!(
            get_version(
                &mut index.write(),
                &index_key_for_name(1, "alice"),
                TXN_NONE
            )
            .unwrap(),
            None
        );
        assert_eq!(
            get_version(&mut index.write(), &index_key_for_name(1, "bob"), TXN_NONE).unwrap(),
            Some(Vec::new())
        );
    }

    #[test]
    fn delete_removes_secondary_index() {
        let (_tmp, primary, index, transaction_manager, table) = open_table_with_secondary_index();
        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");

        let mut seed_txn = transaction_manager.begin_snapshot_txn();
        apply_put_in_txn(&mut primary.write(), &key, &value, &mut seed_txn).unwrap();
        apply_put_in_txn(
            &mut index.write(),
            &index_key_for_name(1, "alice"),
            &[],
            &mut seed_txn,
        )
        .unwrap();
        transaction_manager.commit_txn_state(&mut seed_txn).unwrap();

        let mut cursor = TableCursor::new(
            table,
            primary.clone(),
            vec![index.clone()],
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        cursor.delete(&key).unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, TXN_NONE).unwrap(),
            None
        );
        assert_eq!(
            get_version(
                &mut index.write(),
                &index_key_for_name(1, "alice"),
                TXN_NONE
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn read_only_cursor_rejects_writes() {
        let (_tmp, btree, transaction_manager, table) = open_indexless_table();
        let mut cursor = TableCursor::new(
            table,
            btree,
            Vec::new(),
            transaction_manager,
            None,
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadOnly,
        )
        .unwrap();

        let err = cursor.insert(b"k1", b"v1").unwrap_err();
        assert!(err.to_string().contains("cursor is read-only"));
    }

    #[test]
    fn transaction_bound_cursor_commit_persists_primary_and_secondary() {
        let (_tmp, primary, index, transaction_manager, table) = open_table_with_secondary_index();
        let txn_handle = Arc::new(Mutex::new(transaction_manager.begin_snapshot_txn()));
        let txn_id = txn_handle.lock().id();
        let mut cursor = TableCursor::new(
            table,
            primary.clone(),
            vec![index.clone()],
            transaction_manager.clone(),
            Some(Arc::downgrade(&txn_handle)),
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");
        cursor.insert(&key, &value).unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, txn_id).unwrap(),
            Some(value.clone())
        );
        assert_eq!(
            get_version(&mut index.write(), &index_key_for_name(1, "alice"), txn_id).unwrap(),
            Some(Vec::new())
        );

        transaction_manager
            .commit_txn_state(&mut txn_handle.lock())
            .unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, TXN_NONE).unwrap(),
            Some(value)
        );
        assert_eq!(
            get_version(
                &mut index.write(),
                &index_key_for_name(1, "alice"),
                TXN_NONE
            )
            .unwrap(),
            Some(Vec::new())
        );
    }

    #[test]
    fn transaction_bound_cursor_abort_discards_primary_and_secondary() {
        let (_tmp, primary, index, transaction_manager, table) = open_table_with_secondary_index();
        let txn_handle = Arc::new(Mutex::new(transaction_manager.begin_snapshot_txn()));
        let txn_id = txn_handle.lock().id();
        let mut cursor = TableCursor::new(
            table,
            primary.clone(),
            vec![index.clone()],
            transaction_manager.clone(),
            Some(Arc::downgrade(&txn_handle)),
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");
        cursor.insert(&key, &value).unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, txn_id).unwrap(),
            Some(value)
        );

        transaction_manager
            .abort_txn_state(&mut txn_handle.lock())
            .unwrap();

        assert_eq!(
            get_version(&mut primary.write(), &key, TXN_NONE).unwrap(),
            None
        );
        assert_eq!(
            get_version(
                &mut index.write(),
                &index_key_for_name(1, "alice"),
                TXN_NONE
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn transaction_bound_cursor_falls_back_to_autocommit_after_transaction_handle_drops() {
        let (_tmp, btree, transaction_manager, table) = open_indexless_table();
        let txn_handle = Arc::new(Mutex::new(transaction_manager.begin_snapshot_txn()));
        let mut cursor = TableCursor::new(
            table,
            btree,
            Vec::new(),
            transaction_manager,
            Some(Arc::downgrade(&txn_handle)),
            Arc::new(NoopRecoveryUnit),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        drop(txn_handle);

        cursor.insert(b"k1", b"v1").unwrap();
        assert_eq!(cursor.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }
}
