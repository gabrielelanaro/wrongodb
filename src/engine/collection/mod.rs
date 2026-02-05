use std::collections::HashSet;

use serde_json::Value;

use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::cursor::Cursor;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::session::Session;
use crate::txn::Transaction;
use crate::{Document, WrongoDBError};

pub mod update;

use self::update::apply_update;

/// Represents a single collection within the database
#[derive(Debug, Clone)]
pub struct Collection {
    name: String,
}

impl Collection {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn with_txn<R, F>(&self, session: &mut Session, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&mut Session) -> Result<R, WrongoDBError>,
    {
        if session.current_txn().is_some() {
            return f(session);
        }

        let mut txn = session.transaction()?;
        let result = f(txn.session_mut());
        match result {
            Ok(value) => {
                txn.commit()?;
                Ok(value)
            }
            Err(e) => {
                let _ = txn.abort();
                Err(e)
            }
        }
    }

    fn require_txn<'a>(&self, session: &'a Session) -> Result<&'a Transaction, WrongoDBError> {
        session
            .current_txn()
            .map(|txn| txn)
            .ok_or(WrongoDBError::NoActiveTransaction)
    }

    fn apply_index_add(&self, session: &mut Session, doc: &Document) -> Result<(), WrongoDBError> {
        let table = session.table_handle(&self.name, false)?;
        let mut table_guard = table.write();
        let catalog = table_guard
            .index_catalog_mut()
            .ok_or_else(|| crate::core::errors::StorageError("missing index catalog".into()))?;
        let ops = catalog.add_doc(doc)?;
        session.record_index_ops(ops)?;
        Ok(())
    }

    fn apply_index_remove(&self, session: &mut Session, doc: &Document) -> Result<(), WrongoDBError> {
        let table = session.table_handle(&self.name, false)?;
        let mut table_guard = table.write();
        let catalog = table_guard
            .index_catalog_mut()
            .ok_or_else(|| crate::core::errors::StorageError("missing index catalog".into()))?;
        let ops = catalog.remove_doc(doc)?;
        session.record_index_ops(ops)?;
        Ok(())
    }

    pub fn insert_one(&self, session: &mut Session, doc: Value) -> Result<Document, WrongoDBError> {
        self.with_txn(session, |session| self.insert_one_in_txn(session, doc))
    }

    fn insert_one_in_txn(&self, session: &mut Session, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let id = obj
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;

        let mut cursor = session.open_cursor(&format!("table:{}", self.name))?;
        let txn = self.require_txn(session)?;
        cursor.insert(&key, &value, txn)?;

        self.apply_index_add(session, &obj)?;
        Ok(obj)
    }

    pub fn find(&self, session: &mut Session, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        self.with_txn(session, |session| self.find_with_txn(session, filter))
    }

    fn find_with_txn(
        &self,
        session: &mut Session,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(v) => {
                validate_is_object(&v)?;
                v.as_object().expect("validated object").clone()
            }
        };

        let mut table_cursor = session.open_cursor(&format!("table:{}", self.name))?;

        if filter_doc.is_empty() {
            let txn = self.require_txn(session)?;
            let mut results = Vec::new();
            while let Some((_, bytes)) = table_cursor.next(Some(txn))? {
                results.push(decode_document(&bytes)?);
            }
            return Ok(results);
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
            let txn = self.require_txn(session)?;
            let key = encode_id_value(id_value)?;
            let doc_bytes = table_cursor.get(&key, Some(txn))?;
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

        let indexed_field = {
            let table_handle = session.table_handle(&self.name, false)?;
            let table_guard = table_handle.read();
            let catalog = match table_guard.index_catalog() {
                Some(c) => c,
                None => {
                    let txn = self.require_txn(session)?;
                    return self.scan_with_cursor(table_cursor, txn, &matches_filter);
                }
            };
            filter_doc
                .keys()
                .find(|k| catalog.has_index(*k))
                .cloned()
        };

        if let Some(field) = indexed_field {
            let value = filter_doc.get(&field).unwrap();
            let Some((start_key, end_key)) = encode_range_bounds(value) else {
                return Ok(Vec::new());
            };
            let mut index_cursor = session.open_cursor(&format!("index:{}:{}", self.name, field))?;
            index_cursor.set_range(Some(start_key), Some(end_key));
            let txn = self.require_txn(session)?;

            let mut results = Vec::new();
            while let Some((key, _)) = index_cursor.next(Some(txn))? {
                let Some(id) = decode_index_id(&key)? else {
                    continue;
                };
                let key = encode_id_value(&id)?;
                if let Some(bytes) = table_cursor.get(&key, Some(txn))? {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        let txn = self.require_txn(session)?;
        self.scan_with_cursor(table_cursor, txn, &matches_filter)
    }

    fn scan_with_cursor<F>(
        &self,
        mut cursor: Cursor,
        txn: &Transaction,
        matches_filter: &F,
    ) -> Result<Vec<Document>, WrongoDBError>
    where
        F: Fn(&Document) -> bool,
    {
        let mut results = Vec::new();
        while let Some((_, bytes)) = cursor.next(Some(txn))? {
            let doc = decode_document(&bytes)?;
            if matches_filter(&doc) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    pub fn find_one(&self, session: &mut Session, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(session, filter)?.into_iter().next())
    }

    pub fn count(&self, session: &mut Session, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        Ok(self.find(session, filter)?.len())
    }

    pub fn distinct(&self, session: &mut Session, key: &str, filter: Option<Value>) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(session, filter)?;
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
        &self,
        session: &mut Session,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.with_txn(session, |session| {
            let docs = self.find_with_txn(session, filter)?;
            if docs.is_empty() {
                return Ok(UpdateResult { matched: 0, modified: 0 });
            }

            let doc = &docs[0];
            let updated_doc = apply_update(doc, &update)?;

            let id = doc
                .get("_id")
                .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;

            let mut cursor = session.open_cursor(&format!("table:{}", self.name))?;
            let txn = self.require_txn(session)?;
            cursor.update(&key, &value, txn)?;

            self.apply_index_remove(session, doc)?;
            self.apply_index_add(session, &updated_doc)?;

            Ok(UpdateResult { matched: 1, modified: 1 })
        })
    }

    pub fn update_many(
        &self,
        session: &mut Session,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.with_txn(session, |session| {
            let docs = self.find_with_txn(session, filter)?;
            if docs.is_empty() {
                return Ok(UpdateResult { matched: 0, modified: 0 });
            }

            let mut modified = 0;
            for doc in docs {
                let updated_doc = apply_update(&doc, &update)?;

                let id = doc
                    .get("_id")
                    .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
                let key = encode_id_value(id)?;
                let value = encode_document(&updated_doc)?;

                let mut cursor = session.open_cursor(&format!("table:{}", self.name))?;
                let txn = self.require_txn(session)?;
                cursor.update(&key, &value, txn)?;

                self.apply_index_remove(session, &doc)?;
                self.apply_index_add(session, &updated_doc)?;
                modified += 1;
            }

            Ok(UpdateResult { matched: modified, modified })
        })
    }

    pub fn delete_one(&self, session: &mut Session, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        self.with_txn(session, |session| {
            let docs = self.find_with_txn(session, filter)?;
            if docs.is_empty() {
                return Ok(0);
            }

            let doc = &docs[0];
            let Some(id) = doc.get("_id") else {
                return Ok(0);
            };
            let key = encode_id_value(id)?;

            let mut cursor = session.open_cursor(&format!("table:{}", self.name))?;
            let txn = self.require_txn(session)?;
            cursor.delete(&key, txn)?;

            self.apply_index_remove(session, doc)?;
            Ok(1)
        })
    }

    pub fn delete_many(&self, session: &mut Session, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        self.with_txn(session, |session| {
            let docs = self.find_with_txn(session, filter)?;
            if docs.is_empty() {
                return Ok(0);
            }

            let mut deleted = 0;
            for doc in docs {
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                let key = encode_id_value(id)?;

                let mut cursor = session.open_cursor(&format!("table:{}", self.name))?;
                let txn = self.require_txn(session)?;
                cursor.delete(&key, txn)?;

                self.apply_index_remove(session, &doc)?;
                deleted += 1;
            }

            Ok(deleted)
        })
    }

    pub fn list_indexes(&self, session: &mut Session) -> Result<Vec<IndexInfo>, WrongoDBError> {
        let table = session.table_handle(&self.name, false)?;
        let table_guard = table.read();
        let catalog = table_guard
            .index_catalog()
            .ok_or_else(|| crate::core::errors::StorageError("missing index catalog".into()))?;
        Ok(catalog
            .index_names()
            .into_iter()
            .map(|f| IndexInfo { field: f })
            .collect())
    }

    pub fn create_index(&self, session: &mut Session, field: &str) -> Result<(), WrongoDBError> {
        self.with_txn(session, |session| {
            let docs = self.find_with_txn(session, None)?;
            let table = session.table_handle(&self.name, false)?;
            let mut table_guard = table.write();
            let catalog = table_guard
                .index_catalog_mut()
                .ok_or_else(|| crate::core::errors::StorageError("missing index catalog".into()))?;
            catalog.add_index(field, vec![field.to_string()], &docs)?;
            Ok(())
        })
    }

    pub fn checkpoint(&self, session: &mut Session) -> Result<(), WrongoDBError> {
        let table = session.table_handle(&self.name, false)?;
        let mut guard = table.write();
        guard.checkpoint()
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
