use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::document::{normalize_document, validate_is_object};
use crate::index::InMemoryIndex;
use crate::storage::AppendOnlyStorage;
use crate::{BTree, Document, WrongoDBError};

#[derive(Debug, Clone)]
struct Record {
    offset: u64,
    doc: Document,
    deleted: bool,
}

/// Represents a single collection within the database
#[derive(Debug)]
struct Collection {
    _name: String,
    storage: AppendOnlyStorage,
    index: InMemoryIndex,
    primary_index: Option<BTree>,
    docs: Vec<Record>,
    doc_by_offset: HashMap<u64, usize>,
}

impl Collection {
    fn new(
        name: &str,
        path: &Path,
        index_fields: &HashSet<String>,
        sync_every_write: bool,
    ) -> Result<Self, WrongoDBError> {
        let storage = AppendOnlyStorage::new(path, sync_every_write);
        let primary_path = PathBuf::from(format!("{}.primary.wt", path.display()));
        let primary_index = if name == "test" {
            if primary_path.exists() {
                Some(BTree::open(&primary_path)?)
            } else {
                Some(BTree::create(&primary_path, 4096)?)
            }
        } else {
            None
        };
        let mut coll = Self {
            _name: name.to_string(),
            storage,
            index: InMemoryIndex::new(index_fields.iter().cloned()),
            primary_index,
            docs: Vec::new(),
            doc_by_offset: HashMap::new(),
        };
        coll.load_existing()?;
        Ok(coll)
    }

    fn load_existing(&mut self) -> Result<(), WrongoDBError> {
        let existing = self.storage.read_all()?;
        for (offset, doc) in existing {
            let idx = self.docs.len();
            self.docs.push(Record {
                offset,
                doc: doc.clone(),
                deleted: false,
            });
            self.doc_by_offset.insert(offset, idx);
            self.index.add(&doc, offset);
            if let Some(primary) = &mut self.primary_index {
                if let Some(id) = doc.get("_id") {
                    let key = serde_json::to_string(id).unwrap().into_bytes();
                    let value = offset.to_le_bytes().to_vec();
                    primary.put(&key, &value)?;
                }
            }
        }
        Ok(())
    }

    fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let obj = doc.as_object().expect("validated object").clone();
        let normalized = normalize_document(&obj)?;
        let offset = self.storage.append(&normalized)?;
        let idx = self.docs.len();
        self.docs.push(Record {
            offset,
            doc: normalized.clone(),
            deleted: false,
        });
        self.doc_by_offset.insert(offset, idx);
        self.index.add(&normalized, offset);
        if let Some(primary) = &mut self.primary_index {
            if let Some(id) = normalized.get("_id") {
                let key = serde_json::to_string(id).unwrap().into_bytes();
                if primary.get(&key)?.is_some() {
                    return Err(crate::errors::DocumentValidationError(
                        "duplicate key error".into(),
                    )
                    .into());
                }
                let value = offset.to_le_bytes().to_vec();
                primary.put(&key, &value)?;
            }
        }
        Ok(normalized)
    }

    fn find(&self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(v) => {
                validate_is_object(&v)?;
                v.as_object().expect("validated object").clone()
            }
        };

        if filter_doc.is_empty() {
            return Ok(self
                .docs
                .iter()
                .filter(|r| !r.deleted)
                .map(|r| r.doc.clone())
                .collect());
        }

        let indexed_field = filter_doc
            .keys()
            .find(|k| self.index.fields.contains(*k))
            .cloned();

        let candidates: Box<dyn Iterator<Item = &Record> + '_> = if let Some(field) = indexed_field
        {
            let value = filter_doc.get(&field).unwrap();
            let offsets = self.index.lookup(&field, value);
            Box::new(
                offsets
                    .into_iter()
                    .filter_map(|o| self.doc_by_offset.get(&o))
                    .filter_map(|&idx| self.docs.get(idx))
                    .filter(|r| !r.deleted),
            )
        } else {
            Box::new(self.docs.iter().filter(|r| !r.deleted))
        };

        Ok(candidates
            .filter(|rec| {
                filter_doc.iter().all(|(k, v)| {
                    if k == "_id" {
                        serde_json::to_string(rec.doc.get(k).unwrap()).unwrap()
                            == serde_json::to_string(v).unwrap()
                    } else {
                        rec.doc.get(k) == Some(v)
                    }
                })
            })
            .map(|rec| rec.doc.clone())
            .collect())
    }

    fn count(&self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        Ok(self.find(filter)?.len())
    }

    fn distinct(&self, key: &str, filter: Option<Value>) -> Result<Vec<Value>, WrongoDBError> {
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
        let id = doc.get("_id").cloned();
        let updated_doc = apply_update(doc, &update)?;

        for rec in &mut self.docs {
            if !rec.deleted && rec.doc.get("_id") == id.as_ref() {
                self.index.remove(&rec.doc, rec.offset);
                rec.doc = updated_doc.clone();
                self.index.add(&rec.doc, rec.offset);
                return Ok(UpdateResult {
                    matched: 1,
                    modified: 1,
                });
            }
        }

        Ok(UpdateResult {
            matched: 1,
            modified: 0,
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

        let ids: Vec<_> = docs.iter().filter_map(|d| d.get("_id").cloned()).collect();
        let mut modified = 0;

        for rec in &mut self.docs {
            if rec.deleted {
                continue;
            }
            if let Some(rec_id) = rec.doc.get("_id") {
                if ids.contains(rec_id) {
                    self.index.remove(&rec.doc, rec.offset);
                    rec.doc = apply_update(&rec.doc, &update)?;
                    self.index.add(&rec.doc, rec.offset);
                    modified += 1;
                }
            }
        }

        Ok(UpdateResult {
            matched: ids.len(),
            modified,
        })
    }

    fn delete_one(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let id = docs[0].get("_id").cloned();

        for rec in &mut self.docs {
            if !rec.deleted && rec.doc.get("_id") == id.as_ref() {
                rec.deleted = true;
                self.index.remove(&rec.doc, rec.offset);
                return Ok(1);
            }
        }

        Ok(0)
    }

    fn delete_many(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        let docs = self.find(filter)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let ids: HashSet<_> = docs
            .iter()
            .filter_map(|d| {
                d.get("_id")
                    .map(|v| serde_json::to_string(v).unwrap_or_default())
            })
            .collect();

        let mut deleted = 0;
        for rec in &mut self.docs {
            if rec.deleted {
                continue;
            }
            if let Some(rec_id) = rec.doc.get("_id") {
                let id_str = serde_json::to_string(rec_id).unwrap_or_default();
                if ids.contains(&id_str) {
                    rec.deleted = true;
                    self.index.remove(&rec.doc, rec.offset);
                    deleted += 1;
                }
            }
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

    pub fn find(&self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        self.find_in("test", filter)
    }

    pub fn find_one(&self, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
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

    pub fn find_in(
        &self,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        match self.collections.get(collection) {
            Some(coll) => coll.find(filter),
            None => Ok(vec![]),
        }
    }

    pub fn find_one_in(
        &self,
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

    pub fn count(&self, collection: &str, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        match self.collections.get(collection) {
            Some(coll) => coll.count(filter),
            None => Ok(0),
        }
    }

    pub fn distinct(
        &self,
        collection: &str,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        match self.collections.get(collection) {
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
                .index
                .fields
                .iter()
                .map(|f| IndexInfo { field: f.clone() })
                .collect(),
            None => vec![],
        }
    }

    pub fn create_index(&mut self, collection: &str, field: &str) {
        if let Ok(coll) = self.get_or_create_collection(collection) {
            if coll.index.fields.insert(field.to_string()) {
                for rec in &coll.docs {
                    if !rec.deleted {
                        coll.index.add(&rec.doc, rec.offset);
                    }
                }
            }
        }
    }

    pub fn stats(&self) -> DbStats {
        let doc_count: usize = self
            .collections
            .values()
            .map(|c| c.docs.iter().filter(|r| !r.deleted).count())
            .sum();
        let index_count: usize = self
            .collections
            .values()
            .map(|c| c.index.fields.len())
            .sum();

        DbStats {
            collection_count: self.collections.len(),
            document_count: doc_count,
            index_count,
        }
    }
}
