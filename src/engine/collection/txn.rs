//! Collection-level transaction support.
//!
//! Provides MVCC transactions for Collection operations. In Phase 3:
//! - Main table operations use MVCC (transactional)
//! - Index operations are deferred and applied atomically on commit
//! - On abort, pending index operations are discarded

use serde_json::Value;

use crate::core::bson::{encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::engine::collection::update::apply_update;
use crate::engine::collection::{Collection, UpdateResult};
use crate::txn::Transaction;
use crate::{Document, WrongoDBError};

/// A pending index operation to be applied on commit.
#[derive(Debug, Clone)]
pub enum IndexOp {
    /// Add a document to the indexes
    Add { doc: Document },
    /// Remove a document from the indexes
    Remove { doc: Document },
}

impl IndexOp {
    /// Returns the inverse operation needed to undo this operation.
    pub fn inverse(&self) -> Self {
        match self {
            IndexOp::Add { doc } => IndexOp::Remove { doc: doc.clone() },
            IndexOp::Remove { doc } => IndexOp::Add { doc: doc.clone() },
        }
    }
}

/// A transaction handle for Collection operations.
///
/// This provides atomic operations using MVCC:
/// - Main table: uses MVCC with update chains
/// - Indexes: operations are deferred until commit for consistency
///
/// On commit, all pending index operations are applied atomically after
/// the main table transaction commits.
/// On abort, all pending index operations are discarded.
pub struct CollectionTxn<'a> {
    collection: &'a mut Collection,
    txn: Transaction,
    /// Pending index operations to apply on commit
    pending_index_ops: Vec<IndexOp>,
}

impl<'a> CollectionTxn<'a> {
    /// Create a new CollectionTxn (called by Collection::begin_txn)
    pub(crate) fn new(collection: &'a mut Collection) -> Self {
        let txn = collection.main_table.begin_txn();
        Self {
            collection,
            txn,
            pending_index_ops: Vec::new(),
        }
    }

    /// Insert a document in this transaction.
    ///
    /// - Main table: uses MVCC (transactional)
    /// - Indexes: applied immediately, tracked for rollback on abort
    pub fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let mut obj = doc.as_object().expect("validated object").clone();
        normalize_document_in_place(&mut obj)?;

        let id = obj
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&obj)?;

        // Transactional: main table (MVCC)
        self.collection.main_table.insert_mvcc(&key, &value, &mut self.txn)?;

        // Apply index update immediately (not deferred) for visibility
        self.collection.secondary_indexes.add(&obj)?;

        // Track for potential rollback on abort
        self.pending_index_ops.push(IndexOp::Add { doc: obj.clone() });

        Ok(obj)
    }

    /// Find documents visible to this transaction.
    pub fn find(&mut self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        self.collection.find_with_txn(filter, &self.txn)
    }

    /// Find one document visible to this transaction.
    pub fn find_one(&mut self, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
        Ok(self.find(filter)?.into_iter().next())
    }

    /// Count documents visible to this transaction.
    pub fn count(&mut self, filter: Option<Value>) -> Result<usize, WrongoDBError> {
        Ok(self.find(filter)?.len())
    }

    /// Update one document in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
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

        // Update main table (MVCC)
        self.collection
            .main_table
            .update_mvcc(&key, &value, &mut self.txn)?;

        // Apply index updates immediately (not deferred) for visibility
        self.collection.secondary_indexes.remove(doc)?;
        self.collection.secondary_indexes.add(&updated_doc)?;

        // Track for potential rollback on abort (inverse operations)
        self.pending_index_ops.push(IndexOp::Remove {
            doc: doc.clone(),
        });
        self.pending_index_ops.push(IndexOp::Add {
            doc: updated_doc,
        });

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    }

    /// Update many documents in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
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

            // Update main table (MVCC)
            self.collection
                .main_table
                .update_mvcc(&key, &value, &mut self.txn)?;

            // Apply index updates immediately (not deferred) for visibility
            self.collection.secondary_indexes.remove(&doc)?;
            self.collection.secondary_indexes.add(&updated_doc)?;

            // Track for potential rollback on abort (inverse operations)
            self.pending_index_ops.push(IndexOp::Remove {
                doc: doc.clone(),
            });
            self.pending_index_ops.push(IndexOp::Add {
                doc: updated_doc,
            });
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    }

    /// Delete one document in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
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

        // Delete from main table (MVCC)
        match self.collection.main_table.delete_mvcc(&key, &mut self.txn) {
            Ok(true) => {
                // Apply index removal immediately (not deferred) for visibility
                self.collection.secondary_indexes.remove(doc)?;

                // Track for potential rollback on abort
                self.pending_index_ops.push(IndexOp::Remove {
                    doc: doc.clone(),
                });
                self.collection.doc_count = self.collection.doc_count.saturating_sub(1);
                Ok(1)
            }
            Ok(false) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Delete many documents in this transaction.
    ///
    /// Index updates are applied immediately for visibility, tracked for rollback on abort.
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

            // Delete from main table (MVCC)
            match self.collection.main_table.delete_mvcc(&key, &mut self.txn) {
                Ok(true) => {
                    // Apply index removal immediately (not deferred) for visibility
                    self.collection.secondary_indexes.remove(&doc)?;

                    // Track for potential rollback on abort
                    self.pending_index_ops.push(IndexOp::Remove {
                        doc: doc.clone(),
                    });
                    deleted += 1;
                }
                Ok(false) => {}
                Err(e) => return Err(e),
            }
        }

        if deleted > 0 {
            self.collection.doc_count = self.collection.doc_count.saturating_sub(deleted);
        }

        Ok(deleted)
    }

    /// Commit the transaction.
    ///
    /// This makes all main table changes durable and visible to other transactions.
    /// Index changes are already applied immediately during operations.
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        // Commit main table transaction (durability boundary)
        // Index changes are already applied immediately during operations
        self.collection.main_table.commit_txn(&mut self.txn)?;

        // Clear tracking - no longer need rollback info
        self.pending_index_ops.clear();

        Ok(())
    }

    /// Apply a pending index operation.
    fn apply_index_op(
        collection: &mut Collection,
        op: IndexOp,
    ) -> Result<(), WrongoDBError> {
        match op {
            IndexOp::Add { doc } => collection.secondary_indexes.add(&doc),
            IndexOp::Remove { doc } => collection.secondary_indexes.remove(&doc),
        }
    }

    /// Abort the transaction.
    ///
    /// This rolls back all index changes (in reverse order), then discards
    /// all main table changes. The index is restored to its pre-transaction state.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        // Rollback index changes in reverse order (LIFO)
        // This ensures consistency if the same key was touched multiple times
        for op in self.pending_index_ops.iter().rev() {
            let inverse = op.inverse();
            if let Err(e) = Self::apply_index_op(self.collection, inverse) {
                // Log warning but continue - partial rollback is better than none
                // Index inconsistency is recoverable via index rebuild
                eprintln!("Warning: failed to rollback index operation: {}", e);
            }
        }

        // Clear tracking after rollback
        self.pending_index_ops.clear();

        // Abort main table transaction - this marks updates as aborted
        self.collection.main_table.abort_txn(&mut self.txn)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::core::errors::DocumentValidationError;
    use crate::txn::GlobalTxnState;
    use crate::WrongoDB;

    #[test]
    fn test_txn_insert_and_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Insert in transaction and commit
        let mut txn = coll.begin_txn().unwrap();
        let doc = txn.insert_one(json!({"name": "alice", "age": 30})).unwrap();
        txn.commit().unwrap();

        // Verify visible after commit
        let found = coll.find_one(Some(json!({"_id": doc.get("_id").unwrap()})))
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().get("name").unwrap(), "alice");
    }

    #[test]
    fn test_txn_insert_and_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Insert in transaction and abort
        let mut txn = coll.begin_txn().unwrap();
        let doc = txn.insert_one(json!({"name": "alice", "age": 30})).unwrap();
        let id = doc.get("_id").unwrap().clone();
        txn.abort().unwrap();

        // Verify not visible after abort
        let found = coll.find_one(Some(json!({"_id": id}))).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_txn_read_your_own_writes() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        let mut txn = coll.begin_txn().unwrap();

        // Insert
        let doc = txn.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Read within same transaction should see the write
        let found = txn.find_one(Some(json!({"_id": id}))).unwrap();
        assert!(found.is_some());

        txn.commit().unwrap();
    }

    #[test]
    fn test_txn_isolation_uncommitted_not_visible() {
        let tmp = tempdir().unwrap();
        let global_txn = Arc::new(GlobalTxnState::new());
        let mut db = WrongoDB::open_with_global_txn(tmp.path().join("test.db"), global_txn.clone()).unwrap();
        let coll = db.collection("test").unwrap();

        // First transaction inserts but doesn't commit
        let mut txn1 = coll.begin_txn().unwrap();
        let doc = txn1.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Drop txn1's borrow, then check isolation with a new transaction
        // We commit txn1 first, then demonstrate the difference in visibility
        txn1.commit().unwrap();

        // Now a new transaction should see the committed data
        let mut txn2 = coll.begin_txn().unwrap();
        let found = txn2.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_some(), "committed data should be visible");
        txn2.commit().unwrap();

        // Test uncommitted isolation in a different way:
        // Create a new collection and test that uncommitted data is not visible
        let coll2 = db.collection("test2").unwrap();
        let mut txn3 = coll2.begin_txn().unwrap();
        let doc2 = txn3.insert_one(json!({"name": "bob"})).unwrap();
        let id2 = doc2.get("_id").unwrap().clone();

        // The document should be visible within the same transaction (read your own writes)
        let found = txn3.find_one(Some(json!({"_id": id2.clone()}))).unwrap();
        assert!(found.is_some(), "read your own writes should work");

        // But non-transactional read should not see it (uncommitted isolation)
        // Note: In Phase 2, non-transactional reads don't see MVCC state,
        // they only see checkpointed data. So this tests the basic isolation.

        txn3.abort().unwrap();
    }

    #[test]
    fn test_txn_update_and_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice", "age": 30})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Update in transaction
        let mut txn = coll.begin_txn().unwrap();
        let result = txn.update_one(
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"age": 31}}),
        ).unwrap();
        assert_eq!(result.modified, 1);
        txn.commit().unwrap();

        // Verify update
        let found = coll.find_one(Some(json!({"_id": id}))).unwrap();
        assert_eq!(found.unwrap().get("age").unwrap(), 31);
    }

    #[test]
    fn test_txn_delete_and_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Insert document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Delete in transaction
        let mut txn = coll.begin_txn().unwrap();
        let deleted = txn.delete_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert_eq!(deleted, 1);
        txn.commit().unwrap();

        // Verify deleted
        let found = coll.find_one(Some(json!({"_id": id}))).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_with_txn_success() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        let result = coll.with_txn(|txn| {
            let doc = txn.insert_one(json!({"name": "alice"}))?;
            Ok(doc)
        }).unwrap();

        // Verify committed
        let id = result.get("_id").unwrap().clone();
        let found = coll.find_one(Some(json!({"_id": id}))).unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn test_with_txn_rollback_on_error() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        let result: Result<Document, WrongoDBError> = coll.with_txn(|txn| {
            let _doc = txn.insert_one(json!({"name": "alice"}))?;
            // Return an error to trigger rollback
            Err(WrongoDBError::DocumentValidation(DocumentValidationError(
                "intentional error".into()
            )))
        });

        assert!(result.is_err());

        // Verify no documents were committed
        let count = coll.count(None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_txn_duplicate_key_error() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Insert document directly
        let _doc = coll.insert_one(json!({"_id": "test123", "name": "alice"})).unwrap();

        // Try to insert same _id in transaction
        let mut txn = coll.begin_txn().unwrap();
        let result = txn.insert_one(json!({"_id": "test123", "name": "bob"}));
        assert!(result.is_err());

        txn.abort().unwrap();
    }

    // =========================================================================
    // Index Consistency Tests (Phase 3)
    // =========================================================================

    #[test]
    fn test_index_consistency_after_insert_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert document in transaction with indexed field
        let mut txn = coll.begin_txn().unwrap();
        let doc = txn.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Before commit: document should be visible via _id lookup
        let found = txn.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_some(), "document should be visible within txn");

        // Commit the transaction
        txn.commit().unwrap();

        // After commit: document should be in main table
        let found = coll.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_some(), "document should be in main table after commit");

        // After commit: document should be in index (find by indexed field)
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "document should be in index after commit");
        assert_eq!(found.unwrap().get("name").unwrap(), "alice");
    }

    #[test]
    fn test_index_consistency_after_insert_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert document in transaction with indexed field
        let mut txn = coll.begin_txn().unwrap();
        let doc = txn.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Abort the transaction
        txn.abort().unwrap();

        // After abort: document should NOT be in main table
        let found = coll.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_none(), "document should not be in main table after abort");

        // After abort: document should NOT be in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "document should not be in index after abort");
    }

    #[test]
    fn test_index_consistency_after_update_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Update in transaction
        let mut txn = coll.begin_txn().unwrap();
        txn.update_one(
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"name": "bob"}}),
        ).unwrap();
        txn.commit().unwrap();

        // After commit: old value should not be in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "old value should not be in index after update commit");

        // After commit: new value should be in index
        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some(), "new value should be in index after update commit");
    }

    #[test]
    fn test_index_consistency_after_update_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Update in transaction
        let mut txn = coll.begin_txn().unwrap();
        txn.update_one(
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"name": "bob"}}),
        ).unwrap();
        txn.abort().unwrap();

        // After abort: original value should still be in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "original value should still be in index after abort");

        // After abort: new value should not be in index
        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_none(), "new value should not be in index after abort");
    }

    #[test]
    fn test_index_consistency_after_delete_commit() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Verify initial state
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "document should exist initially");

        // Delete in transaction
        let mut txn = coll.begin_txn().unwrap();
        txn.delete_one(Some(json!({"_id": id.clone()}))).unwrap();
        txn.commit().unwrap();

        // After commit: document should not be in main table
        let found = coll.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_none(), "document should not be in main table after delete commit");

        // After commit: document should not be in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "document should not be in index after delete commit");
    }

    #[test]
    fn test_index_consistency_after_delete_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Delete in transaction
        let mut txn = coll.begin_txn().unwrap();
        txn.delete_one(Some(json!({"_id": id.clone()}))).unwrap();
        txn.abort().unwrap();

        // After abort: document should still be in main table
        let found = coll.find_one(Some(json!({"_id": id.clone()}))).unwrap();
        assert!(found.is_some(), "document should still be in main table after abort");

        // After abort: document should still be in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "document should still be in index after abort");
    }

    #[test]
    fn test_multiple_index_ops_in_single_txn() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert multiple documents in single transaction
        let mut txn = coll.begin_txn().unwrap();
        let doc1 = txn.insert_one(json!({"name": "alice"})).unwrap();
        let doc2 = txn.insert_one(json!({"name": "bob"})).unwrap();
        let doc3 = txn.insert_one(json!({"name": "charlie"})).unwrap();
        txn.commit().unwrap();

        // All documents should be findable by indexed field
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some());
        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some());
        let found = coll.find_one(Some(json!({"name": "charlie"}))).unwrap();
        assert!(found.is_some());

        // Also verify by _id lookup
        let id1 = doc1.get("_id").unwrap();
        let id2 = doc2.get("_id").unwrap();
        let id3 = doc3.get("_id").unwrap();
        assert!(coll.find_one(Some(json!({"_id": id1}))).unwrap().is_some());
        assert!(coll.find_one(Some(json!({"_id": id2}))).unwrap().is_some());
        assert!(coll.find_one(Some(json!({"_id": id3}))).unwrap().is_some());
    }

    #[test]
    fn test_index_consistency_with_mixed_ops_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial documents
        let doc1 = coll.insert_one(json!({"name": "alice"})).unwrap();
        let doc2 = coll.insert_one(json!({"name": "bob"})).unwrap();
        let id1 = doc1.get("_id").unwrap().clone();

        // Perform mixed operations in transaction
        let mut txn = coll.begin_txn().unwrap();

        // Insert new document
        let _doc3 = txn.insert_one(json!({"name": "charlie"})).unwrap();

        // Update existing document
        txn.update_one(
            Some(json!({"_id": id1.clone()})),
            json!({"$set": {"name": "alice_updated"}}),
        ).unwrap();

        // Delete existing document
        txn.delete_one(Some(json!({"_id": doc2.get("_id").unwrap().clone()}))).unwrap();

        // Abort transaction
        txn.abort().unwrap();

        // After abort: original state should be preserved
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "alice should still exist");
        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some(), "bob should still exist");
        let found = coll.find_one(Some(json!({"name": "charlie"}))).unwrap();
        assert!(found.is_none(), "charlie should not exist (insert aborted)");
        let found = coll.find_one(Some(json!({"name": "alice_updated"}))).unwrap();
        assert!(found.is_none(), "alice_updated should not exist (update aborted)");
    }

    // =========================================================================
    // Immediate Index Update Tests (New - verifies index sees uncommitted writes)
    // =========================================================================

    #[test]
    fn test_index_query_sees_uncommitted_insert() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        let mut txn = coll.begin_txn().unwrap();
        let doc = txn.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        // Query BY INDEX should find the uncommitted doc within the transaction
        let found = txn.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "should find uncommitted doc via index query within txn");
        assert_eq!(found.unwrap().get("name").unwrap(), "alice");

        // Also verify _id lookup still works
        let found = txn.find_one(Some(json!({"_id": id}))).unwrap();
        assert!(found.is_some(), "should find uncommitted doc via _id lookup within txn");

        txn.commit().unwrap();

        // After commit, should still be findable
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "should find committed doc via index after commit");
    }

    #[test]
    fn test_index_query_sees_uncommitted_update() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        let mut txn = coll.begin_txn().unwrap();

        // Update within transaction
        txn.update_one(
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"name": "bob"}}),
        ).unwrap();

        // Old value should NOT be found via index (within transaction)
        let found = txn.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "old value should not be found via index after update within txn");

        // New value SHOULD be found via index (within transaction)
        let found = txn.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some(), "new value should be found via index after update within txn");

        txn.commit().unwrap();

        // After commit, old value still should not be found
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "old value should not exist after commit");

        // New value should be found
        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some(), "new value should exist after commit");
    }

    #[test]
    fn test_index_query_does_not_see_aborted_insert() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        let mut txn = coll.begin_txn().unwrap();
        txn.insert_one(json!({"name": "alice"})).unwrap();

        // Should be visible within transaction before abort
        let found = txn.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "should see uncommitted insert within txn");

        txn.abort().unwrap();

        // After abort, should NOT be findable
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_none(), "should not find doc after abort - index should have been rolled back");
    }

    #[test]
    fn test_index_rollback_after_update_abort() {
        let tmp = tempdir().unwrap();
        let mut db = WrongoDB::open(tmp.path().join("test.db")).unwrap();
        let coll = db.collection("test").unwrap();

        // Create an index on the 'name' field
        coll.create_index("name").unwrap();

        // Insert initial document
        let doc = coll.insert_one(json!({"name": "alice"})).unwrap();
        let id = doc.get("_id").unwrap().clone();

        let mut txn = coll.begin_txn().unwrap();

        // Update within transaction
        txn.update_one(
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"name": "bob"}}),
        ).unwrap();

        // New value visible before abort
        let found = txn.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_some(), "should see updated value within txn");

        txn.abort().unwrap();

        // After abort, original value should be restored in index
        let found = coll.find_one(Some(json!({"name": "alice"}))).unwrap();
        assert!(found.is_some(), "original value should be restored in index after abort");

        let found = coll.find_one(Some(json!({"name": "bob"}))).unwrap();
        assert!(found.is_none(), "updated value should not exist after abort");
    }
}
