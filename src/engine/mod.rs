use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::index::SecondaryIndexManager;
use crate::storage::main_table::MainTable;
use crate::{Document, WrongoDBError};

/// Represents a single collection within the database
#[derive(Debug)]
struct Collection {
    _name: String,
    main_table: MainTable,
    secondary_indexes: SecondaryIndexManager,
    doc_count: usize,
    wal_enabled: bool,
}

impl Collection {
    fn new(
        name: &str,
        path: &Path,
        index_fields: &HashSet<String>,
        _sync_every_write: bool,
    ) -> Result<Self, WrongoDBError> {
        let wal_enabled = true; // Enable WAL for durability
        let main_table_path = PathBuf::from(format!("{}.main.wt", path.display()));
        let main_table = MainTable::open_or_create(&main_table_path, wal_enabled)?;

        // Start with empty manager - indexes will be created in load_existing after reading docs
        let secondary_indexes = SecondaryIndexManager::empty(path, wal_enabled);

        let mut coll = Self {
            _name: name.to_string(),
            main_table,
            secondary_indexes,
            doc_count: 0,
            wal_enabled,
        };
        coll.load_existing(path, index_fields)?;
        Ok(coll)
    }

    fn load_existing(
        &mut self,
        path: &Path,
        index_fields: &HashSet<String>,
    ) -> Result<(), WrongoDBError> {
        let docs = self.main_table.scan()?;
        self.doc_count = docs.len();

        // Now rebuild secondary indexes with all documents
        // This handles the case where index files don't exist yet
        self.secondary_indexes = SecondaryIndexManager::open_or_rebuild(
            path,
            index_fields.iter().cloned(),
            self.wal_enabled,
            &docs,
        )?;

        Ok(())
    }

    fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let obj = doc.as_object().expect("validated object").clone();
        self.insert_one_doc(obj)
    }

    fn insert_one_doc(&mut self, mut doc: Document) -> Result<Document, WrongoDBError> {
        normalize_document_in_place(&mut doc)?;
        self.main_table.insert(&doc)?;
        self.secondary_indexes.add(&doc)?;
        self.doc_count = self.doc_count.saturating_add(1);
        Ok(doc)
    }

    fn find(&mut self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(v) => {
                validate_is_object(&v)?;
                v.as_object().expect("validated object").clone()
            }
        };

        if filter_doc.is_empty() {
            return self.main_table.scan();
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
            let doc = self.main_table.get(id_value)?;
            return Ok(match doc {
                Some(doc) if matches_filter(&doc) => vec![doc],
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
                if let Some(doc) = self.main_table.get(&id)? {
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        let docs = self.main_table.scan()?;
        Ok(docs.into_iter().filter(|doc| matches_filter(doc)).collect())
    }

    fn count(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        Ok(self.find(filter)?.len())
    }

    fn distinct(&mut self, key: &str, filter: Option<Value>) -> Result<Vec<Value>, WrongoDBError> {
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

    fn update_one(
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

        self.secondary_indexes.remove(doc)?;
        self.main_table.update(&updated_doc)?;
        self.secondary_indexes.add(&updated_doc)?;

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    fn update_many(
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
            self.secondary_indexes.remove(&doc)?;
            self.main_table.update(&updated_doc)?;
            self.secondary_indexes.add(&updated_doc)?;
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    fn delete_one(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        self.secondary_indexes.remove(doc)?;
        if self.main_table.delete(id)? {
            self.doc_count = self.doc_count.saturating_sub(1);
            return Ok(1);
        }
        Ok(0)
    }

    fn delete_many(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            self.secondary_indexes.remove(&doc)?;
            if self.main_table.delete(id)? {
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.doc_count = self.doc_count.saturating_sub(deleted);
        }

        Ok(deleted)
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

/// Database statistics
#[derive(Debug, Clone)]
pub struct DbStats {
    pub collection_count: usize,
    pub document_count: usize,
    pub index_count: usize,
}

/// Apply MongoDB-style update operators or replacement
fn apply_update(doc: &Document, update: &Value) -> Result<Document, WrongoDBError> {
    let update_obj = match update.as_object() {
        Some(obj) => obj,
        None => return Ok(doc.clone()),
    };

    let is_update_operators = update_obj.keys().any(|k| k.starts_with('$'));

    if !is_update_operators {
        let mut new_doc = update_obj.clone();
        if let Some(id) = doc.get("_id") {
            new_doc.insert("_id".to_string(), id.clone());
        }
        return Ok(new_doc);
    }

    let mut new_doc = doc.clone();

    // $set
    if let Some(Value::Object(set_fields)) = update_obj.get("$set") {
        for (k, v) in set_fields {
            new_doc.insert(k.clone(), v.clone());
        }
    }

    // $unset
    if let Some(Value::Object(unset_fields)) = update_obj.get("$unset") {
        for k in unset_fields.keys() {
            new_doc.remove(k);
        }
    }

    // $inc
    if let Some(Value::Object(inc_fields)) = update_obj.get("$inc") {
        for (k, v) in inc_fields {
            if let Some(inc_val) = v.as_f64() {
                let current = new_doc.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                new_doc.insert(
                    k.clone(),
                    Value::Number(
                        serde_json::Number::from_f64(current + inc_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
            }
        }
    }

    // $push
    if let Some(Value::Object(push_fields)) = update_obj.get("$push") {
        for (k, v) in push_fields {
            let arr = new_doc
                .entry(k.clone())
                .or_insert_with(|| Value::Array(vec![]));
            if let Value::Array(ref mut arr_vec) = arr {
                arr_vec.push(v.clone());
            }
        }
    }

    // $pull
    if let Some(Value::Object(pull_fields)) = update_obj.get("$pull") {
        for (k, v) in pull_fields {
            if let Some(Value::Array(arr)) = new_doc.get_mut(k) {
                arr.retain(|item| item != v);
            }
        }
    }

    Ok(new_doc)
}

#[derive(Debug)]
pub struct WrongoDB {
    base_path: PathBuf,
    collections: HashMap<String, Collection>,
    default_index_fields: HashSet<String>,
    sync_every_write: bool,
}

impl WrongoDB {
    pub fn open<P, I, S>(
        path: P,
        index_fields: I,
        sync_every_write: bool,
    ) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let base_path = path.as_ref().to_path_buf();
        let default_index_fields: HashSet<String> =
            index_fields.into_iter().map(|s| s.into()).collect();

        let mut db = Self {
            base_path,
            collections: HashMap::new(),
            default_index_fields,
            sync_every_write,
        };

        db.get_or_create_collection("test")?;

        Ok(db)
    }

    fn get_or_create_collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
        if !self.collections.contains_key(name) {
            let coll_path = if name == "test" {
                self.base_path.clone()
            } else {
                self.base_path.with_extension(format!("{}.db", name))
            };
            let coll = Collection::new(
                name,
                &coll_path,
                &self.default_index_fields,
                self.sync_every_write,
            )?;
            self.collections.insert(name.to_string(), coll);
        }
        Ok(self.collections.get_mut(name).unwrap())
    }

    // ========================================================================
    // Original API (backwards compatible, operates on default "test" collection)
    // ========================================================================

    pub fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        self.insert_one_into("test", doc)
    }

    pub fn insert_one_doc(&mut self, doc: Document) -> Result<Document, WrongoDBError> {
        self.insert_one_doc_into("test", doc)
    }

    pub fn find(&mut self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        self.find_in("test", filter)
    }

    pub fn find_one(&mut self, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(filter)?.into_iter().next())
    }

    // ========================================================================
    // Collection-aware API
    // ========================================================================

    pub fn insert_one_into(
        &mut self,
        collection: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.insert_one(doc)
    }

    pub fn insert_one_doc_into(
        &mut self,
        collection: &str,
        doc: Document,
    ) -> Result<Document, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.insert_one_doc(doc)
    }

    pub fn find_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        match self.collections.get_mut(collection) {
            Some(coll) => coll.find(filter),
            None => Ok(vec![]),
        }
    }

    pub fn find_one_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find_in(collection, filter)?.into_iter().next())
    }

    pub fn update_one_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.update_one(filter, update)
    }

    pub fn update_many_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.update_many(filter, update)
    }

    pub fn delete_one_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.delete_one(filter)
    }

    pub fn delete_many_in(
        &mut self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;
        coll.delete_many(filter)
    }

    pub fn count(&mut self, collection: &str, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        match self.collections.get_mut(collection) {
            Some(coll) => coll.count(filter),
            None => Ok(0),
        }
    }

    pub fn distinct(
        &mut self,
        collection: &str,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        match self.collections.get_mut(collection) {
            Some(coll) => coll.distinct(key, filter),
            None => Ok(vec![]),
        }
    }

    // ========================================================================
    // Metadata operations
    // ========================================================================

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    pub fn list_indexes(&self, collection: &str) -> Vec<IndexInfo> {
        match self.collections.get(collection) {
            Some(coll) => coll
                .secondary_indexes
                .fields
                .iter()
                .map(|f| IndexInfo { field: f.clone() })
                .collect(),
            None => vec![],
        }
    }

    pub fn create_index(&mut self, collection: &str, field: &str) -> Result<(), WrongoDBError> {
        let coll = self.get_or_create_collection(collection)?;

        // Prepare existing documents for index building
        let existing_docs = coll.main_table.scan()?;

        // Add the field to the index manager (creates BTree and builds from existing docs)
        coll.secondary_indexes.add_field(field, &existing_docs)?;

        Ok(())
    }

    /// Checkpoint all id indexes to durable storage.
    ///
    /// This flushes all dirty pages to disk and atomically swaps the root.
    /// After this returns, all previous mutations are durable.
    ///
    /// # Durability semantics
    /// - `checkpoint()` = durability boundary
    /// - Unflushed pages may be lost on crash
    /// - Call after important writes to ensure they survive crashes
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for coll in self.collections.values_mut() {
            coll.main_table.checkpoint()?;
            coll.secondary_indexes.checkpoint()?;
        }
        Ok(())
    }

    /// Request automatic checkpointing after N updates on the id index.
    ///
    /// Once the threshold is reached, `put()` operations will automatically
    /// call `checkpoint()` after the operation completes.
    pub fn request_checkpoint_after_updates(&mut self, count: usize) {
        for coll in self.collections.values_mut() {
            coll.main_table.request_checkpoint_after_updates(count);
        }
    }

    pub fn stats(&self) -> DbStats {
        let doc_count: usize = self
            .collections
            .values()
            .map(|c| c.doc_count)
            .sum();
        let index_count: usize = self
            .collections
            .values()
            .map(|c| c.secondary_indexes.fields.len())
            .sum();

        DbStats {
            collection_count: self.collections.len(),
            document_count: doc_count,
            index_count,
        }
    }
}
