use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;

use crate::catalog::DurableCatalog;
use crate::core::bson::encode_id_value;
use crate::core::document::validate_is_object;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::storage::api::{Session, TableCursor};
use crate::storage::row::decode_row_value;
use crate::{Document, WrongoDBError};

/// Read path for collection queries over the WT-like storage API.
///
/// `DocumentQuery` resolves collection definitions from the durable catalog,
/// opens the storage-layer table cursor, and applies simple filter planning on
/// top of the available primary and secondary indexes.
#[derive(Clone)]
pub(crate) struct DocumentQuery {
    durable_catalog: Arc<DurableCatalog>,
}

impl DocumentQuery {
    /// Creates the document query service.
    pub(crate) fn new(durable_catalog: Arc<DurableCatalog>) -> Self {
        Self { durable_catalog }
    }

    pub(crate) fn find(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        session.with_transaction(|session| self.find_in_transaction(session, collection, filter))
    }

    /// Counts the documents matching `filter`.
    pub(crate) fn count(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(session, collection, filter)?.len())
    }

    /// Returns the distinct values of `key` among the matching documents.
    pub(crate) fn distinct(
        &self,
        session: &mut Session,
        collection: &str,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(session, collection, filter)?;
        let mut seen = HashSet::new();
        let mut values = Vec::new();

        for doc in docs {
            if let Some(value) = doc.get(key) {
                let encoded = serde_json::to_string(value).unwrap_or_default();
                if seen.insert(encoded) {
                    values.push(value.clone());
                }
            }
        }

        Ok(values)
    }

    /// Executes a read inside the caller's active transaction.
    pub(crate) fn find_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let Some(collection_definition) = self.durable_catalog.collection_for_txn(
            session,
            collection,
            session.current_txn_id(),
        )?
        else {
            return Ok(Vec::new());
        };

        let filter_doc = match filter {
            None => Document::new(),
            Some(value) => {
                validate_is_object(&value)?;
                value.as_object().expect("validated object").clone()
            }
        };

        let mut table_cursor = session.open_table_cursor(collection_definition.table_uri())?;

        if filter_doc.is_empty() {
            return scan_with_cursor(&mut table_cursor, |doc| {
                let _ = doc;
                true
            });
        }

        let matches_filter = |doc: &Document| {
            filter_doc.iter().all(|(key, value)| {
                if key == "_id" {
                    serde_json::to_string(doc.get(key).unwrap()).unwrap()
                        == serde_json::to_string(value).unwrap()
                } else {
                    doc.get(key) == Some(value)
                }
            })
        };

        if let Some(id_value) = filter_doc.get("_id") {
            let key = encode_id_value(id_value)?;
            let doc_bytes = table_cursor.get(&key)?;
            return Ok(match doc_bytes {
                Some(bytes) => {
                    let doc = decode_row_value(table_cursor.table(), &key, &bytes)?;
                    if matches_filter(&doc) {
                        vec![doc]
                    } else {
                        Vec::new()
                    }
                }
                None => Vec::new(),
            });
        }

        if let Some(index) = collection_definition.indexes().values().find(|index| {
            index.ready()
                && index
                    .indexed_field()
                    .map(|field| filter_doc.contains_key(&field))
                    .unwrap_or(false)
        }) {
            let field = index.indexed_field()?;
            let value = filter_doc.get(&field).expect("field selected from filter");
            let Some((start_key, end_key)) = encode_range_bounds(value) else {
                return Ok(Vec::new());
            };
            let index_entries =
                session.scan_store_range(index.uri(), Some(&start_key), Some(&end_key))?;

            let mut results = Vec::new();
            for (key, _) in index_entries {
                let Some(id) = decode_index_id(&key)? else {
                    continue;
                };
                let primary_key = encode_id_value(&id)?;
                if let Some(bytes) = table_cursor.get(&primary_key)? {
                    let doc = decode_row_value(table_cursor.table(), &primary_key, &bytes)?;
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        scan_with_cursor(&mut table_cursor, matches_filter)
    }
}

fn scan_with_cursor<F>(
    cursor: &mut TableCursor<'_>,
    matches_filter: F,
) -> Result<Vec<Document>, WrongoDBError>
where
    F: Fn(&Document) -> bool,
{
    let mut results = Vec::new();
    while let Some((key, bytes)) = cursor.next()? {
        let doc = decode_row_value(cursor.table(), &key, &bytes)?;
        if matches_filter(&doc) {
            results.push(doc);
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest};
    use crate::collection_write_path::CollectionWritePath;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::LogManager;
    use crate::storage::metadata_store::MetadataStore;
    use crate::txn::GlobalTxnState;

    struct QueryTestFixture {
        query: DocumentQuery,
        write_path: CollectionWritePath,
        session: Session,
    }

    impl QueryTestFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);

            let global_txn = Arc::new(GlobalTxnState::new());
            let store_handles =
                Arc::new(HandleCache::<String, parking_lot::RwLock<BTreeCursor>>::new());
            let log_manager = Arc::new(LogManager::disabled());
            let metadata_store = Arc::new(
                MetadataStore::new(
                    base_path.clone(),
                    store_handles.clone(),
                    global_txn.clone(),
                    log_manager.clone(),
                )
                .unwrap(),
            );
            let durable_catalog = Arc::new(DurableCatalog::new(CatalogStore::new()));
            let collection_catalog = Arc::new(CollectionCatalog::new());
            let session = Session::new(
                base_path,
                store_handles,
                metadata_store.clone(),
                global_txn,
                log_manager,
            );
            durable_catalog.ensure_store_exists(&session).unwrap();
            collection_catalog
                .load_from_durable(&session, durable_catalog.as_ref())
                .unwrap();
            let query = DocumentQuery::new(durable_catalog.clone());
            let write_path = CollectionWritePath::new(
                metadata_store,
                durable_catalog,
                collection_catalog,
                query.clone(),
            );

            Self {
                query,
                write_path,
                session,
            }
        }

        fn into_parts(self) -> (DocumentQuery, CollectionWritePath, Session) {
            (self.query, self.write_path, self.session)
        }
    }

    #[test]
    fn indexed_lookup_survives_checkpoint_reconciliation() {
        let (query, write_path, mut session) = QueryTestFixture::new().into_parts();

        write_path
            .create_collection(&mut session, "test", vec!["name".to_string()])
            .unwrap();
        write_path
            .create_index(
                &mut session,
                "test",
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();
        write_path
            .insert_one(&mut session, "test", json!({"_id": 1, "name": "alice"}))
            .unwrap();

        session.checkpoint().unwrap();

        let docs = query
            .find(&mut session, "test", Some(json!({"name": "alice"})))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name"), Some(&json!("alice")));
    }
}
