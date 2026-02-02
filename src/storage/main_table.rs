use std::path::Path;
use std::sync::Arc;

use serde_json::Value;

use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::txn::{GlobalTxnState, NonTransactional, ReadContext, Transaction};
use crate::{BTree, Document, WrongoDBError};

#[derive(Debug)]
pub struct MainTable {
    btree: BTree,
}

impl MainTable {
    pub fn open_or_create<P: AsRef<Path>>(
        path: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        let btree = if path.as_ref().exists() {
            BTree::open(path, wal_enabled, global_txn)?
        } else {
            BTree::create(path, 4096, wal_enabled, global_txn)?
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

    /// Get a document (transactional or non-transactional)
    pub fn get(&mut self, id: &Value, ctx: impl ReadContext) -> Result<Option<Document>, WrongoDBError> {
        let key = encode_id_value(id)?;
        match self.btree.get_ctx(&key, &ctx)? {
            Some(bytes) => Ok(Some(decode_document(&bytes)?)),
            None => Ok(None),
        }
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

    /// Scan all documents visible to the context
    pub fn scan(&mut self, ctx: impl ReadContext) -> Result<Vec<Document>, WrongoDBError> {
        let mut out = Vec::new();

        // Collect all keys first to avoid borrow issues
        let keys: Vec<Vec<u8>> = self
            .btree
            .range(None, None)
            .map_err(|e| StorageError(format!("main table scan failed: {e}")))?
            .map(|entry| entry.map(|(k, _v)| k.to_vec()))
            .collect::<Result<_, _>>()?;

        for key in keys {
            // Use get_ctx to respect the read context (MVCC or non-transactional)
            if let Some(bytes) = self.btree.get_ctx(&key, &ctx)? {
                out.push(decode_document(&bytes)?);
            }
        }
        Ok(out)
    }

    /// Scan all documents without a transaction (non-transactional)
    pub fn scan_non_txn(&mut self) -> Result<Vec<Document>, WrongoDBError> {
        self.scan(NonTransactional)
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()
    }

    /// Insert a document within a transaction (MVCC)
    pub fn insert_mvcc(
        &mut self,
        doc: &Document,
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        let id = doc
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(doc)?;

        // Check if key already exists in this transaction's view
        if self.btree.get_mvcc(&key, txn)?.is_some() {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }

        self.btree.put_mvcc(&key, &value, txn)?;
        Ok(())
    }

    /// Update a document within a transaction (MVCC)
    pub fn update_mvcc(
        &mut self,
        doc: &Document,
        txn: &mut Transaction,
    ) -> Result<bool, WrongoDBError> {
        let id = doc
            .get("_id")
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;

        // Check if document exists in this transaction's view
        if self.btree.get_mvcc(&key, txn)?.is_none() {
            return Ok(false);
        }

        let value = encode_document(doc)?;
        self.btree.put_mvcc(&key, &value, txn)?;
        Ok(true)
    }

    /// Delete a document within a transaction (MVCC)
    pub fn delete_mvcc(
        &mut self,
        id: &Value,
        txn: &mut Transaction,
    ) -> Result<bool, WrongoDBError> {
        let key = encode_id_value(id)?;

        // Check if document exists in this transaction's view
        if self.btree.get_mvcc(&key, txn)?.is_none() {
            return Ok(false);
        }

        self.btree.delete_mvcc(&key, txn)?;
        Ok(true)
    }

    /// Get the underlying BTree's MVCC state for transaction management
    pub fn begin_txn(&self) -> Transaction {
        self.btree.begin_txn(crate::txn::IsolationLevel::Snapshot)
    }

    /// Commit a transaction on the underlying BTree
    pub fn commit_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        self.btree.commit_txn(txn)
    }

    /// Abort a transaction on the underlying BTree
    pub fn abort_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        self.btree.abort_txn(txn)
    }

    /// Run garbage collection on MVCC update chains.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        self.btree.run_gc()
    }

    /// Sync the WAL to ensure durability.
    /// This is a no-op if WAL is disabled.
    pub fn sync_wal(&mut self) -> Result<(), WrongoDBError> {
        self.btree.sync_wal()
    }

    // ==========================================================================
    // Raw byte-oriented API (for Cursor)
    // ==========================================================================

    /// Insert a raw key/value pair.
    pub fn insert_raw(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        if !self.btree.insert_unique(key, value)? {
            return Err(DocumentValidationError("duplicate key error".into()).into());
        }
        Ok(())
    }

    /// Get a raw value by key within a transaction context.
    pub fn get_raw(&mut self, key: &[u8], ctx: impl ReadContext) -> Result<Option<Vec<u8>>, WrongoDBError> {
        self.btree.get_ctx(key, &ctx)
    }

    /// Delete a raw key.
    pub fn delete_raw(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        self.btree.delete(key)
    }

    /// Scan all raw key/value pairs visible to the given context.
    #[allow(dead_code)]
    pub fn scan_raw(&mut self, ctx: impl ReadContext) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
        let mut out = Vec::new();

        let entries = self
            .btree
            .range(None, None)
            .map_err(|e| StorageError(format!("main table scan failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()?;

        for (key, _value) in entries {
            if let Some(bytes) = self.btree.get_ctx(&key, &ctx)? {
                out.push((key.to_vec(), bytes));
            }
        }

        Ok(out)
    }
}
