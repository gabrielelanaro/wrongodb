use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::bson::{decode_document, decode_id_value};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::index::encode_index_key;
use crate::storage::api::session::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::table::{contains_key, get_version, scan_range, IndexMetadata, TableMetadata};
use crate::txn::{Transaction, TxnId};
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

#[derive(Debug, Default)]
pub(crate) struct TableCursorState {
    buffered_entries: Vec<CursorEntry>,
    buffer_pos: usize,
    exhausted: bool,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
}

impl TableCursorState {
    pub(crate) fn reset_runtime(&mut self) {
        self.buffered_entries.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
    }
}

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
    ) -> Result<Self, WrongoDBError> {
        if table.indexes().len() != indexes.len() {
            return Err(StorageError(format!(
                "table cursor index handle mismatch: metadata has {}, runtime has {}",
                table.indexes().len(),
                indexes.len()
            ))
            .into());
        }

        let state = Arc::new(Mutex::new(TableCursorState::default()));
        session.track_open_cursor(&state);

        Ok(Self {
            session,
            table,
            primary,
            indexes,
            state,
            write_access,
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
                primary.put(self.table.uri(), key, value, txn)?;
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
                primary.put(self.table.uri(), key, value, txn)?;
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
            primary.delete(self.table.uri(), key, txn).map(|_| ())
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
        _txn_id: TxnId,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        for (index_pos, key) in entries {
            let metadata = &self.table.indexes()[*index_pos];
            let mut btree = self.indexes[*index_pos].write();
            btree.put(metadata.uri(), key, &[], txn)?;
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
            btree.delete(metadata.uri(), key, txn)?;
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::{tempdir, TempDir};

    use super::*;
    use crate::core::bson::{encode_document, encode_id_value};
    use crate::storage::api::Session;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::LogManager;
    use crate::storage::metadata_store::MetadataStore;
    use crate::storage::table::open_or_create_btree;
    use crate::txn::{GlobalTxnState, TXN_NONE};

    fn build_test_session(
        base_path: &Path,
        global_txn: Arc<GlobalTxnState>,
        log_manager: Arc<LogManager>,
    ) -> Session {
        let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
        let metadata_store = Arc::new(MetadataStore::new(
            base_path.to_path_buf(),
            store_handles.clone(),
        ));
        Session::new(
            base_path.to_path_buf(),
            store_handles,
            metadata_store,
            global_txn,
            log_manager,
        )
    }

    fn open_indexless_table() -> (
        TempDir,
        Arc<RwLock<BTreeCursor>>,
        Session,
        Arc<GlobalTxnState>,
        TableMetadata,
    ) {
        let tmp = tempdir().unwrap();
        let uri = "table:test".to_string();
        let store_name = "cursor.main.wt".to_string();
        let path = tmp.path().join(&store_name);
        let global_txn = Arc::new(GlobalTxnState::new());
        let btree = Arc::new(RwLock::new(open_or_create_btree(path).unwrap()));
        let session = build_test_session(tmp.path(), global_txn.clone(), disabled_log_manager());
        (tmp, btree, session, global_txn, TableMetadata::new(uri))
    }

    fn open_table_with_secondary_index() -> (
        TempDir,
        Arc<RwLock<BTreeCursor>>,
        Arc<RwLock<BTreeCursor>>,
        Session,
        Arc<GlobalTxnState>,
        TableMetadata,
    ) {
        let tmp = tempdir().unwrap();
        let primary_path = tmp.path().join("users.main.wt");
        let index_path = tmp.path().join("users.name.idx.wt");
        let global_txn = Arc::new(GlobalTxnState::new());
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
        let session = build_test_session(tmp.path(), global_txn.clone(), disabled_log_manager());
        (tmp, primary, index, session, global_txn, table)
    }

    fn make_document_bytes(id: i64, name: &str) -> Vec<u8> {
        encode_document(&serde_json::from_value(json!({"_id": id, "name": name})).unwrap()).unwrap()
    }

    fn index_key_for_name(id: i64, name: &str) -> Vec<u8> {
        encode_index_key(&json!(name), &json!(id)).unwrap().unwrap()
    }

    fn disabled_log_manager() -> Arc<LogManager> {
        Arc::new(LogManager::disabled())
    }

    #[test]
    fn insert_applies_locally() {
        let (_tmp, btree, session, _global_txn, table) = open_indexless_table();
        let mut cursor = TableCursor::new(
            &session,
            table,
            btree.clone(),
            Vec::new(),
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
        let (_tmp, btree, session, global_txn, table) = open_indexless_table();
        let mut seed_txn = global_txn.begin_snapshot_txn();
        btree
            .write()
            .put(table.uri(), b"k1", b"v1", &mut seed_txn)
            .unwrap();
        seed_txn.commit(global_txn.as_ref()).unwrap();

        let mut cursor = TableCursor::new(
            &session,
            table,
            btree.clone(),
            Vec::new(),
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
        let (_tmp, primary, index, session, _global_txn, table) = open_table_with_secondary_index();
        let mut cursor = TableCursor::new(
            &session,
            table,
            primary.clone(),
            vec![index.clone()],
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
        let (_tmp, primary, index, session, global_txn, table) = open_table_with_secondary_index();
        let key = encode_id_value(&json!(1)).unwrap();
        let old_value = make_document_bytes(1, "alice");
        let new_value = make_document_bytes(1, "bob");

        let mut seed_txn = global_txn.begin_snapshot_txn();
        primary
            .write()
            .put(table.uri(), &key, &old_value, &mut seed_txn)
            .unwrap();
        index
            .write()
            .put(
                table.indexes()[0].uri(),
                &index_key_for_name(1, "alice"),
                &[],
                &mut seed_txn,
            )
            .unwrap();
        seed_txn.commit(global_txn.as_ref()).unwrap();

        let mut cursor = TableCursor::new(
            &session,
            table,
            primary.clone(),
            vec![index.clone()],
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
        let (_tmp, primary, index, session, global_txn, table) = open_table_with_secondary_index();
        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");

        let mut seed_txn = global_txn.begin_snapshot_txn();
        primary
            .write()
            .put(table.uri(), &key, &value, &mut seed_txn)
            .unwrap();
        index
            .write()
            .put(
                table.indexes()[0].uri(),
                &index_key_for_name(1, "alice"),
                &[],
                &mut seed_txn,
            )
            .unwrap();
        seed_txn.commit(global_txn.as_ref()).unwrap();

        let mut cursor = TableCursor::new(
            &session,
            table,
            primary.clone(),
            vec![index.clone()],
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
        let (_tmp, btree, session, _global_txn, table) = open_indexless_table();
        let mut cursor = TableCursor::new(
            &session,
            table,
            btree,
            Vec::new(),
            TableCursorWriteAccess::ReadOnly,
        )
        .unwrap();

        let err = cursor.insert(b"k1", b"v1").unwrap_err();
        assert!(err.to_string().contains("cursor is read-only"));
    }

    #[test]
    fn transaction_bound_cursor_commit_persists_primary_and_secondary() {
        let (_tmp, primary, index, mut session, _global_txn, table) =
            open_table_with_secondary_index();
        session.begin_transaction().unwrap();
        let txn_id = session.current_txn_id();
        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");
        {
            let mut cursor = TableCursor::new(
                &session,
                table,
                primary.clone(),
                vec![index.clone()],
                TableCursorWriteAccess::ReadWrite,
            )
            .unwrap();

            cursor.insert(&key, &value).unwrap();
        }

        assert_eq!(
            get_version(&mut primary.write(), &key, txn_id).unwrap(),
            Some(value.clone())
        );
        assert_eq!(
            get_version(&mut index.write(), &index_key_for_name(1, "alice"), txn_id).unwrap(),
            Some(Vec::new())
        );

        session.commit_transaction().unwrap();

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
        let (_tmp, primary, index, mut session, _global_txn, table) =
            open_table_with_secondary_index();
        session.begin_transaction().unwrap();
        let txn_id = session.current_txn_id();
        let key = encode_id_value(&json!(1)).unwrap();
        let value = make_document_bytes(1, "alice");
        {
            let mut cursor = TableCursor::new(
                &session,
                table,
                primary.clone(),
                vec![index.clone()],
                TableCursorWriteAccess::ReadWrite,
            )
            .unwrap();

            cursor.insert(&key, &value).unwrap();
        }

        assert_eq!(
            get_version(&mut primary.write(), &key, txn_id).unwrap(),
            Some(value)
        );

        session.rollback_transaction().unwrap();

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
    fn cursor_uses_autocommit_without_active_transaction() {
        let (_tmp, btree, session, _global_txn, table) = open_indexless_table();
        let mut cursor = TableCursor::new(
            &session,
            table,
            btree,
            Vec::new(),
            TableCursorWriteAccess::ReadWrite,
        )
        .unwrap();

        cursor.insert(b"k1", b"v1").unwrap();
        assert_eq!(cursor.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }
}
