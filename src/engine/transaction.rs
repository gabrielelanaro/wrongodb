//! Multi-collection transaction support for WrongoDB.
//!
//! This module provides `MultiCollectionTxn`, which allows atomic operations
//! across multiple collections using the same transaction context. This enables
//! multi-document ACID transactions with snapshot isolation.
//!
//! # Example
//!
//! ```
//! # use wrongodb::WrongoDB;
//! # use serde_json::json;
//! # let tmp = tempfile::tempdir().unwrap();
//! # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
//! // Using with_txn for automatic commit/abort
//! db.with_txn(|txn| {
//!     // Insert into first collection
//!     let doc1 = txn.insert_one("collection_a", json!({"name": "alice"}))?;
//!
//!     // Insert into second collection (cross-collection transaction)
//!     let _doc2 = txn.insert_one("collection_b", json!({"ref": doc1.get("_id").unwrap()}))?;
//!
//!     Ok(())
//! }).unwrap();
//! ```

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::core::bson::{encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::engine::collection::update::apply_update;
use crate::engine::collection::{Collection, IndexOp, UpdateResult};
use crate::txn::Transaction;
use crate::{Document, WrongoDB, WrongoDBError};

/// A transaction that spans multiple collections.
///
/// All operations within this transaction share the same transaction ID,
/// ensuring atomic visibility - either all changes are visible to other
/// transactions, or none are.
///
/// Index operations are applied immediately during operations for visibility,
/// and tracked for rollback on abort.
// TODO: Refactor to eliminate the Option<Transaction> take/replace pattern.
// The current design stores both &mut WrongoDB and Transaction, causing borrow
// checker conflicts. Options:
//   A) Pass &mut WrongoDB to each method instead of storing it
//   B) Restructure so txn methods don't need simultaneous db access
//   C) Use RefCell<Transaction> for interior mutability
// See code review discussion for trade-offs.
pub struct MultiCollectionTxn<'a> {
    db: &'a mut WrongoDB,
    // We use Option here so we can temporarily take the transaction
    // to work around borrow checker issues
    txn: Option<Transaction>,
    /// Tracks which collections have been modified (for commit coordination)
    touched_collections: HashSet<String>,
    /// Pending index operations per collection
    pending_index_ops: HashMap<String, Vec<IndexOp>>,
}

impl<'a> MultiCollectionTxn<'a> {
    /// Create a new multi-collection transaction.
    ///
    /// This is called by `WrongoDB::begin_txn()`. The transaction starts with
    /// snapshot isolation, capturing the current global state at the time of
    /// first read/write.
    pub(crate) fn new(db: &'a mut WrongoDB) -> Result<Self, WrongoDBError> {
        let txn = db.global_txn().begin_snapshot_txn();
        Ok(Self {
            db,
            txn: Some(txn),
            touched_collections: HashSet::new(),
            pending_index_ops: HashMap::new(),
        })
    }

    /// Get or create a collection for this transaction.
    fn get_collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
        if !self.db.has_collection(name) {
            self.db.create_collection(name)?;
        }
        self.db.get_collection_mut(name)
    }

    /// Insert a document into a collection in this transaction.
    ///
    /// The document is inserted into the main table using MVCC, and the
    /// index is updated immediately for visibility, tracked for rollback on abort.
    ///
    /// # Example
    ///
    /// ```
    /// # use wrongodb::WrongoDB;
    /// # use serde_json::json;
    /// # let tmp = tempfile::tempdir().unwrap();
    /// # let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
    /// db.with_txn(|txn| {
    ///     let doc = txn.insert_one("users", json!({"name": "alice"}))?;
    ///     Ok(())
    /// }).unwrap();
    /// ```
    pub fn insert_one(
        &mut self,
        collection_name: &str,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let id = obj
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;

        // Mark collection as touched
        self.touched_collections.insert(collection_name.to_string());

        // Temporarily take the transaction to avoid borrow issues
        let mut txn = self.txn.take().expect("transaction present");

        // Do the insert - this is self-contained
        let result = (|| {
            let collection = self.db.get_collection_mut(collection_name)?;
            collection.main_table().insert_mvcc(&key, &value, &mut txn)?;
            Ok::<(), WrongoDBError>(())
        })();

        // Restore the transaction FIRST (before any other operations)
        self.txn = Some(txn);

        // Check if insert failed
        result?;

        // Insert succeeded - now update doc count and index
        let collection = self.db.get_collection_mut(collection_name)?;
        collection.increment_doc_count();

        // Apply index update immediately (not deferred) for visibility
        collection.secondary_indexes().add(&obj)?;

        // Track for potential rollback on abort
        let pending_ops = self
            .pending_index_ops
            .entry(collection_name.to_string())
            .or_default();
        pending_ops.push(IndexOp::Add { doc: obj.clone() });

        Ok(obj)
    }

    /// Find documents in a collection visible to this transaction.
    ///
    /// This uses the transaction's snapshot to ensure consistent reads
    /// of committed data plus this transaction's own uncommitted writes.
    pub fn find(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        // Temporarily take the transaction to avoid borrow issues
        let txn = self.txn.take().expect("transaction present");

        // Get collection and do the find
        let collection = self.get_collection(collection_name)?;
        let result = collection.find_with_txn(filter, &txn);

        // Restore the transaction
        self.txn = Some(txn);

        result
    }

    /// Find one document in a collection visible to this transaction.
    pub fn find_one(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
    ) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(collection_name, filter)?.into_iter().next())
    }

    /// Count documents in a collection visible to this transaction.
    pub fn count(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(collection_name, filter)?.len())
    }

    /// Update one document in a collection in this transaction.
    ///
    /// The main table update uses MVCC, and index operations are applied
    /// immediately for visibility, tracked for rollback on abort.
    pub fn update_one(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        // First find the documents
        // Temporarily take the transaction to avoid borrow issues
        let txn = self.txn.take().expect("transaction present");
        let collection = self.get_collection(collection_name)?;
        let docs = collection.find_with_txn(filter, &txn)?;
        // Restore the transaction (we'll need it again)
        self.txn = Some(txn);

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

        // Mark collection as touched
        self.touched_collections.insert(collection_name.to_string());

        // Temporarily take the transaction to avoid borrow issues
        let mut txn = self.txn.take().expect("transaction present");

        // Get collection and do the update
        let collection = self.get_collection(collection_name)?;
        let result = collection
            .main_table()
            .update_mvcc(&key, &value, &mut txn);

        // Restore the transaction
        self.txn = Some(txn);

        result?;

        // Apply index updates immediately (not deferred) for visibility
        let collection = self.get_collection(collection_name)?;
        collection.secondary_indexes().remove(doc)?;
        collection.secondary_indexes().add(&updated_doc)?;

        // Track for potential rollback on abort (inverse operations)
        let pending_ops = self
            .pending_index_ops
            .entry(collection_name.to_string())
            .or_default();
        pending_ops.push(IndexOp::Remove {
            doc: doc.clone(),
        });
        pending_ops.push(IndexOp::Add {
            doc: updated_doc,
        });

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    /// Update many documents in a collection in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
    pub fn update_many(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        // First find the documents
        // Temporarily take the transaction to avoid borrow issues
        let txn = self.txn.take().expect("transaction present");
        let collection = self.get_collection(collection_name)?;
        let docs = collection.find_with_txn(filter, &txn)?;
        // Restore the transaction (we'll need it again)
        self.txn = Some(txn);

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

            // Mark collection as touched
            self.touched_collections.insert(collection_name.to_string());

            // Temporarily take the transaction to avoid borrow issues
            let mut txn = self.txn.take().expect("transaction present");

            // Get collection and do the update
            let collection = self.get_collection(collection_name)?;
            let result = collection
                .main_table()
                .update_mvcc(&key, &value, &mut txn);

            // Restore the transaction
            self.txn = Some(txn);

            result?;

            // Apply index updates immediately (not deferred) for visibility
            let collection = self.get_collection(collection_name)?;
            collection.secondary_indexes().remove(&doc)?;
            collection.secondary_indexes().add(&updated_doc)?;

            // Track for potential rollback on abort (inverse operations)
            let pending_ops = self
                .pending_index_ops
                .entry(collection_name.to_string())
                .or_default();
            pending_ops.push(IndexOp::Remove {
                doc: doc.clone(),
            });
            pending_ops.push(IndexOp::Add {
                doc: updated_doc,
            });
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    /// Delete one document in a collection in this transaction.
    ///
    /// The main table delete uses MVCC (tombstone), and the index operation
    /// is applied immediately for visibility, tracked for rollback on abort.
    pub fn delete_one(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        // First find the documents
        // Temporarily take the transaction to avoid borrow issues
        let txn = self.txn.take().expect("transaction present");
        let collection = self.get_collection(collection_name)?;
        let docs = collection.find_with_txn(filter, &txn)?;
        // Restore the transaction (we'll need it again)
        self.txn = Some(txn);

        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;

        // Mark collection as touched
        self.touched_collections.insert(collection_name.to_string());

        // Temporarily take the transaction to avoid borrow issues
        let mut txn = self.txn.take().expect("transaction present");

        // Get collection and do the delete
        let collection = self.get_collection(collection_name)?;
        let result = collection.main_table().delete_mvcc(&key, &mut txn);

        // Restore the transaction
        self.txn = Some(txn);

        match result {
            Ok(true) => {
                // Apply index removal immediately (not deferred) for visibility
                // Use a scope to ensure collection borrow is dropped before pending_index_ops access
                {
                    let collection = self.get_collection(collection_name)?;
                    collection.secondary_indexes().remove(doc)?;
                    collection.decrement_doc_count();
                }

                // Track for potential rollback on abort
                let pending_ops = self
                    .pending_index_ops
                    .entry(collection_name.to_string())
                    .or_default();
                pending_ops.push(IndexOp::Remove {
                    doc: doc.clone(),
                });

                Ok(1)
            }
            Ok(false) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Delete many documents in a collection in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
    pub fn delete_many(
        &mut self,
        collection_name: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        // First find the documents
        // Temporarily take the transaction to avoid borrow issues
        let txn = self.txn.take().expect("transaction present");
        let collection = self.get_collection(collection_name)?;
        let docs = collection.find_with_txn(filter, &txn)?;
        // Restore the transaction (we'll need it again)
        self.txn = Some(txn);

        if docs.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;

            // Mark collection as touched
            self.touched_collections.insert(collection_name.to_string());

            // Temporarily take the transaction to avoid borrow issues
            let mut txn = self.txn.take().expect("transaction present");

            // Get collection and do the delete
            let collection = self.get_collection(collection_name)?;
            let result = collection.main_table().delete_mvcc(&key, &mut txn);

            // Restore the transaction
            self.txn = Some(txn);

            match result {
                Ok(true) => {
                    // Apply index removal immediately (not deferred) for visibility
                    let collection = self.get_collection(collection_name)?;
                    collection.secondary_indexes().remove(&doc)?;

                    // Track for potential rollback on abort
                    let pending_ops = self
                        .pending_index_ops
                        .entry(collection_name.to_string())
                        .or_default();
                    pending_ops.push(IndexOp::Remove {
                        doc: doc.clone(),
                    });
                    deleted += 1;
                }
                Ok(false) => {}
                Err(e) => return Err(e),
            }
        }

        if deleted > 0 {
            let collection = self.get_collection(collection_name)?;
            collection.decrement_doc_count_by(deleted);
        }

        Ok(deleted)
    }

    /// Commit all changes across all touched collections.
    ///
    /// This makes all changes visible atomically. The commit process:
    /// 1. Commit main table changes for all touched collections (durability boundary)
    ///
    /// Index changes are already applied immediately during operations.
    ///
    /// If the commit fails partway through, some collections may have committed
    /// main table changes while others have not. This is safe because:
    /// - All changes share the same transaction ID
    /// - The transaction is only marked as committed when commit_txn() succeeds
    /// - Recovery will properly handle partially-committed transactions via WAL
    // TODO: Ensure secondary indexes are checkpointed during multi-collection commit.
    // Currently only main_table().commit_txn() is called, but secondary indexes may
    // have uncommitted changes that could be lost on crash. Consider:
    //   A) Call secondary_indexes().checkpoint() for each touched collection
    //   B) Or ensure index WAL entries are covered by the main table's commit
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        // Take ownership of the transaction
        let mut txn = self.txn.take().expect("transaction present");

        // Step 1: Commit the transaction (once, globally)
        // We commit on the first touched collection's BTree (if any)
        // This writes the commit record to WAL and updates global state
        let first_coll = self.touched_collections.iter().next().cloned();
        if let Some(first_coll_name) = first_coll {
            let coll = self.db.get_collection_mut(&first_coll_name)?;
            coll.main_table().commit_txn(&mut txn)?;
        }

        // Step 2: For other collections, just sync the WAL (if needed)
        // The transaction is already committed in global state, so other BTrees
        // will see the committed updates when they read
        for coll_name in self.touched_collections.iter().skip(1) {
            // No need to call commit_txn again - the global transaction state
            // is what determines visibility. Each collection's MVCC chains
            // will be updated when readers access them.
            // We just need to ensure WAL is synced for durability.
            let coll = self.db.get_collection_mut(coll_name)?;
            coll.main_table().sync_wal()?;
        }

        // Index changes are already applied immediately during operations
        // Just clear the tracking
        self.pending_index_ops.clear();

        Ok(())
    }

    /// Abort all changes across all collections.
    ///
    /// This rolls back all index changes (in reverse order), then discards
    /// all main table changes. The transaction is marked as aborted, and
    /// all update chains are cleaned up.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        // Step 1: Rollback index changes in reverse order (LIFO)
        // This ensures consistency if the same key was touched multiple times
        for (coll_name, ops) in &self.pending_index_ops {
            let coll = self.db.get_collection_mut(coll_name)?;
            for op in ops.iter().rev() {
                let inverse = op.inverse();
                if let Err(e) = Self::apply_index_op(coll, inverse) {
                    // Log warning but continue - partial rollback is better than none
                    // Index inconsistency is recoverable via index rebuild
                    eprintln!(
                        "Warning: failed to rollback index operation on collection '{}': {}",
                        coll_name, e
                    );
                }
            }
        }

        // Clear tracking after rollback
        self.pending_index_ops.clear();

        // Step 2: Mark all main table updates as aborted
        let mut txn = self.txn.take().expect("transaction present");
        for coll_name in &self.touched_collections {
            let coll = self.db.get_collection_mut(coll_name)?;
            coll.main_table().abort_txn(&mut txn)?;
        }

        Ok(())
    }

    /// Apply a pending index operation to a collection.
    fn apply_index_op(collection: &mut Collection, op: IndexOp) -> Result<(), WrongoDBError> {
        match op {
            IndexOp::Add { doc } => collection.secondary_indexes().add(&doc),
            IndexOp::Remove { doc } => collection.secondary_indexes().remove(&doc),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn test_multi_collection_insert() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();

        let mut txn = db.begin_txn().unwrap();
        let doc1 = txn.insert_one("coll_a", json!({"name": "alice"})).unwrap();
        let _doc2 = txn.insert_one("coll_b", json!({"ref": doc1.get("_id").unwrap()})).unwrap();
        txn.commit().unwrap();
    }
}
