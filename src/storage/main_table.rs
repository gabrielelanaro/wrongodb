use std::path::Path;

use serde_json::Value;

use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::{BTree, Document, WrongoDBError};

#[derive(Debug)]
pub struct MainTable {
    btree: BTree,
}

impl MainTable {
    pub fn open_or_create<P: AsRef<Path>>(path: P, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let btree = if path.as_ref().exists() {
            BTree::open(path, wal_enabled)?
        } else {
            BTree::create(path, 4096, wal_enabled)?
        };
        Ok(Self { btree })
    }

    pub fn insert(&mut self, doc: &Document) -> Result<(), WrongoDBError> {
        let id = doc
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(doc)?;
        if !self.btree.insert_unique(&key, &value)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }
        Ok(())
    }

    pub fn get(&mut self, id: &Value) -> Result<Option<Document>, WrongoDBError> {
        let key = encode_id_value(id)?;
        let Some(bytes) = self.btree.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(decode_document(&bytes)?))
    }

    pub fn update(&mut self, doc: &Document) -> Result<bool, WrongoDBError> {
        let id = doc
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        if self.btree.get(&key)?.is_none() {
            return Ok(false);
        }
        let value = encode_document(doc)?;
        self.btree.put(&key, &value)?;
        Ok(true)
    }

    pub fn delete(&mut self, id: &Value) -> Result<bool, WrongoDBError> {
        let key = encode_id_value(id)?;
        self.btree.delete(&key)
    }

    pub fn scan(&mut self) -> Result<Vec<Document>, WrongoDBError> {
        let mut out = Vec::new();
        let iter = self
            .btree
            .range(None, None)
            .map_err(|e| StorageError(format!("main table scan failed: {e}")))?;
        for entry in iter {
            let (_, value) = entry?;
            out.push(decode_document(&value)?);
        }
        Ok(out)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()
    }

    pub fn request_checkpoint_after_updates(&mut self, count: usize) {
        self.btree.request_checkpoint_after_updates(count);
    }
}
