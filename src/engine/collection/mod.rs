use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;

use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::cursor::Cursor;
use crate::txn::GlobalTxnState;

pub mod update;
mod txn;

pub use self::txn::{CollectionTxn, IndexOp};
use self::update::apply_update;
use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::index::SecondaryIndexManager;
use crate::storage::table::Table;
use crate::txn::{NonTransactional, Transaction};
use crate::{Document, WrongoDBError};

/// Represents a single collection within the database
#[derive(Debug)]
pub struct Collection {
    main_table: Arc<RwLock<Table>>,
    secondary_indexes: SecondaryIndexManager,
    doc_count: usize,
    checkpoint_after_updates: Option<usize>,
    updates_since_checkpoint: usize,
    global_txn: Arc<GlobalTxnState>,
}

impl Collection {
    pub(crate) fn new(
        path: &Path,
        wal_enabled: bool,
        checkpoint_after_updates: Option<usize>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let main_table_path = PathBuf::from(format!("{}.main.wt", path.display()));
        let main_table = Arc::new(RwLock::new(Table::open_or_create(&main_table_path, wal_enabled, global_txn.clone())?));

        // Start with empty manager - indexes can be created via create_index()
        let secondary_indexes = SecondaryIndexManager::empty(path, wal_enabled, global_txn.clone());

        let mut coll = Self {
            main_table,
            secondary_indexes,
            doc_count: 0,
            checkpoint_after_updates,
            updates_since_checkpoint: 0,
            global_txn,
        };
        coll.load_existing(path)?;
        Ok(coll)
    }

    fn begin_snapshot_txn(&self) -> Transaction {
        self.global_txn.begin_snapshot_txn()
    }

    fn load_existing(&mut self, _path: &Path) -> Result<(), WrongoDBError> {
        let entries = self.main_table.write().scan(&NonTransactional)?;
        self.doc_count = entries.len();

        // Indexes are not created automatically - use create_index() after getting collection
        Ok(())
    }

    pub fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let id = obj
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;

        self.main_table.write().insert(&key, &value)?;
        self.secondary_indexes.add(&obj)?;
        self.doc_count = self.doc_count.saturating_add(1);
        self.maybe_checkpoint_after_updates(1)?;
        Ok(obj)
    }

    pub fn find(&mut self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        let txn = self.begin_snapshot_txn();
        self.find_with_txn(filter, &txn)
    }

    /// Shared query implementation used by both Collection and CollectionTxn.
    pub(crate) fn find_with_txn(
        &mut self,
        filter: Option<Value>,
        txn: &Transaction,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(v) => {
                validate_is_object(&v)?;
                v.as_object().expect("validated object").clone()
            }
        };

        if filter_doc.is_empty() {
            let entries = self.main_table.write().scan(txn)?;
            return entries
                .into_iter()
                .map(|(_, v)| decode_document(&v))
                .collect();
        }

        let matches_filter = |doc: &Document| {
            filter_doc.iter().all(|(k, v)| {
                if k == "_id" {
                    serde_json::to_string(doc.get(k).unwrap()).unwrap()
                        == serde_json::to_string(v).unwrap()
                } else {
                    doc.get(k) == Some(v)
                }
            })
        };

        if let Some(id_value) = filter_doc.get("_id") {
            let key = encode_id_value(id_value)?;
            let doc_bytes = self.main_table.write().get(&key, txn)?;
            return Ok(match doc_bytes {
                Some(bytes) => {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        vec![doc]
                    } else {
                        Vec::new()
                    }
                }
                _ => Vec::new(),
            });
        }

        let indexed_field = filter_doc
            .keys()
            .find(|k| self.secondary_indexes.fields.contains(*k))
            .cloned();

        if let Some(field) = indexed_field {
            let value = filter_doc.get(&field).unwrap();
            let ids = self.secondary_indexes.lookup(&field, value)?;
            let mut results = Vec::new();
            for id in ids {
                let key = encode_id_value(&id)?;
                if let Some(bytes) = self.main_table.write().get(&key, txn)? {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        let entries = self.main_table.write().scan(txn)?;
        let mut results = Vec::new();
        for (_, bytes) in entries {
            let doc = decode_document(&bytes)?;
            if matches_filter(&doc) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    pub fn find_one(&mut self, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(filter)?.into_iter().next())
    }

    pub fn count(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        Ok(self.find(filter)?.len())
    }

    pub fn distinct(&mut self, key: &str, filter: Option<Value>) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(filter)?;
        let mut seen = HashSet::new();
        let mut values = Vec::new();

        for doc in docs {
            if let Some(val) = doc.get(key) {
                let key_str = serde_json::to_string(val).unwrap_or_default();
                if seen.insert(key_str) {
                    values.push(val.clone());
                }
            }
        }

        Ok(values)
    }

    pub fn update_one(
        &mut self,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let doc = &docs[0];
        let updated_doc = apply_update(doc, &update)?;

        let id = doc
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&updated_doc)?;

        self.secondary_indexes.remove(doc)?;
        self.main_table.write().update(&key, &value)?;
        self.secondary_indexes.add(&updated_doc)?;
        self.maybe_checkpoint_after_updates(1)?;

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    pub fn update_many(
        &mut self,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let mut modified = 0;
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;

            let id = doc
                .get("_id")
                .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;

            self.secondary_indexes.remove(&doc)?;
            self.main_table.write().update(&key, &value)?;
            self.secondary_indexes.add(&updated_doc)?;
            modified += 1;
        }

        self.maybe_checkpoint_after_updates(modified)?;

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    pub fn delete_one(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;

        self.secondary_indexes.remove(doc)?;
        if self.main_table.write().delete(&key)? {
            self.doc_count = self.doc_count.saturating_sub(1);
            self.maybe_checkpoint_after_updates(1)?;
            return Ok(1);
        }
        Ok(0)
    }

    pub fn delete_many(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;

            self.secondary_indexes.remove(&doc)?;
            if self.main_table.write().delete(&key)? {
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.doc_count = self.doc_count.saturating_sub(deleted);
            self.maybe_checkpoint_after_updates(deleted)?;
        }

        Ok(deleted)
    }

    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        self.secondary_indexes
            .fields
            .iter()
            .map(|f| IndexInfo { field: f.clone() })
            .collect()
    }

    pub fn create_index(&mut self, field: &str) -> Result<(), WrongoDBError> {
        let entries = self.main_table.write().scan(&NonTransactional)?;
        let existing_docs: Vec<Document> = entries
            .into_iter()
            .map(|(_, v)| decode_document(&v))
            .collect::<Result<_, _>>()?;

        let was_indexed = self.secondary_indexes.fields.contains(field);
        self.secondary_indexes.add_field(field, &existing_docs)?;
        if !was_indexed {
            self.maybe_checkpoint_after_updates(existing_docs.len())?;
        }
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.main_table.write().checkpoint()?;
        self.secondary_indexes.checkpoint()?;

        // Run GC on the main table's MVCC update chains
        let (main_chains, main_updates, main_dropped) = self.main_table.write().run_gc();

        // Run GC on secondary indexes' MVCC update chains
        let (idx_chains, idx_updates, idx_dropped) = self.secondary_indexes.run_gc();

        let total_chains = main_chains + idx_chains;
        let total_updates = main_updates + idx_updates;
        let total_dropped = main_dropped + idx_dropped;

        if total_chains > 0 || total_dropped > 0 {
            eprintln!(
                "GC complete: {} chains cleaned, {} updates removed, {} chains dropped",
                total_chains, total_updates, total_dropped
            );
        }

        self.updates_since_checkpoint = 0;
        Ok(())
    }

    fn maybe_checkpoint_after_updates(&mut self, updates: usize) -> Result<(), WrongoDBError> {
        if updates == 0 {
            return Ok(());
        }
        self.updates_since_checkpoint = self.updates_since_checkpoint.saturating_add(updates);
        if let Some(threshold) = self.checkpoint_after_updates {
            if self.updates_since_checkpoint >= threshold {
                self.checkpoint()?;
            }
        }
        Ok(())
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn index_count(&self) -> usize {
        self.secondary_indexes.fields.len()
    }

    // ========================================================================
    // Internal methods for MultiCollectionTxn
    // ========================================================================

    /// Get a cursor for the main table.
    pub(crate) fn main_table_cursor(&self) -> Cursor {
        Cursor::new(self.main_table.clone())
    }

    /// Get mutable access to the main table for internal operations.
    pub(crate) fn main_table(&self) -> parking_lot::RwLockWriteGuard<'_, Table> {
        self.main_table.write()
    }

    /// Get access to the secondary indexes.
    pub(crate) fn secondary_indexes(&mut self) -> &mut SecondaryIndexManager {
        &mut self.secondary_indexes
    }

    /// Increment the document count.
    pub(crate) fn increment_doc_count(&mut self) {
        self.doc_count = self.doc_count.saturating_add(1);
    }

    /// Decrement the document count.
    pub(crate) fn decrement_doc_count(&mut self) {
        self.doc_count = self.doc_count.saturating_sub(1);
    }

    /// Decrement the document count by a specific amount.
    pub(crate) fn decrement_doc_count_by(&mut self, count: usize) {
        self.doc_count = self.doc_count.saturating_sub(count);
    }

    /// Begin a new transaction on this collection.
    ///
    /// # Example
    /// ```
    /// # use wrongodb::WrongoDB;
    /// # use serde_json::json;
    /// # let tmp = tempfile::tempdir().unwrap();
    /// # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
    /// # let coll = db.collection("test").unwrap();
    /// let mut txn = coll.begin_txn().unwrap();
    /// let doc = txn.insert_one(json!({"name": "alice"})).unwrap();
    /// txn.commit().unwrap();
    /// ```
    pub fn begin_txn(&mut self) -> Result<CollectionTxn<'_>, WrongoDBError> {
        Ok(CollectionTxn::new(self))
    }

    /// Run a function in a transaction, automatically committing or aborting.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is aborted.
    ///
    /// # Example
    /// ```
    /// # use wrongodb::WrongoDB;
    /// # use serde_json::json;
    /// # let tmp = tempfile::tempdir().unwrap();
    /// # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
    /// # let coll = db.collection("test").unwrap();
    /// let result = coll.with_txn(|txn| {
    ///     let doc = txn.insert_one(json!({"name": "alice"}))?;
    ///     Ok(doc)
    /// }).unwrap();
    /// ```
    pub fn with_txn<F, R>(&mut self, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&mut CollectionTxn<'_>) -> Result<R, WrongoDBError>,
    {
        let mut txn = self.begin_txn()?;
        match f(&mut txn) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(e) => {
                let _ = txn.abort();
                Err(e)
            }
        }
    }
}

/// Result of an update operation
#[derive(Debug, Clone, Copy)]
pub struct UpdateResult {
    pub matched: usize,
    pub modified: usize,
}

/// Index metadata for listIndexes
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub field: String,
}
