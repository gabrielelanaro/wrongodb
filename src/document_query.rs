use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;

use crate::core::bson::{decode_document, encode_id_value};
use crate::core::document::validate_is_object;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::schema::SchemaCatalog;
use crate::storage::api::{Cursor, Session, WriteUnitOfWork};
use crate::{Document, WrongoDBError};

#[derive(Clone)]
pub(crate) struct DocumentQuery {
    schema_catalog: Arc<SchemaCatalog>,
}

impl DocumentQuery {
    pub(crate) fn new(schema_catalog: Arc<SchemaCatalog>) -> Self {
        Self { schema_catalog }
    }

    pub(crate) fn find(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        self.run_in_write_unit(session, |this, write_unit| {
            this.find_in_write_unit(write_unit, collection, filter)
        })
    }

    pub(crate) fn count(
        &self,
        session: &mut Session,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(session, collection, filter)?.len())
    }

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

    pub(crate) fn list_indexes(&self, collection: &str) -> Result<Vec<String>, WrongoDBError> {
        self.schema_catalog.list_indexes(collection)
    }

    pub(crate) fn find_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        if !self
            .schema_catalog
            .collection_exists_in_txn(collection, write_unit.txn_id())?
        {
            return Ok(Vec::new());
        }

        let filter_doc = match filter {
            None => Document::new(),
            Some(value) => {
                validate_is_object(&value)?;
                value.as_object().expect("validated object").clone()
            }
        };

        let mut table_cursor = write_unit.open_cursor(&format!("table:{collection}"))?;

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
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        vec![doc]
                    } else {
                        Vec::new()
                    }
                }
                None => Vec::new(),
            });
        }

        let schema = self
            .schema_catalog
            .collection_schema_for_txn(collection, write_unit.txn_id())?;
        let indexed_field = filter_doc.keys().find(|key| schema.has_index(key)).cloned();
        if let Some(field) = indexed_field {
            let value = filter_doc.get(&field).expect("field selected from filter");
            let Some((start_key, end_key)) = encode_range_bounds(value) else {
                return Ok(Vec::new());
            };
            let mut index_cursor =
                write_unit.open_cursor(&format!("index:{collection}:{field}"))?;
            index_cursor.set_range(Some(start_key), Some(end_key));

            let mut results = Vec::new();
            while let Some((key, _)) = index_cursor.next()? {
                let Some(id) = decode_index_id(&key)? else {
                    continue;
                };
                let primary_key = encode_id_value(&id)?;
                if let Some(bytes) = table_cursor.get(&primary_key)? {
                    let doc = decode_document(&bytes)?;
                    if matches_filter(&doc) {
                        results.push(doc);
                    }
                }
            }
            return Ok(results);
        }

        scan_with_cursor(&mut table_cursor, matches_filter)
    }

    fn run_in_write_unit<R, F>(&self, session: &mut Session, f: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&Self, &mut WriteUnitOfWork<'_>) -> Result<R, WrongoDBError>,
    {
        let mut write_unit = session.transaction()?;
        let result = f(self, &mut write_unit);
        match result {
            Ok(value) => {
                write_unit.commit()?;
                Ok(value)
            }
            Err(err) => {
                let _ = write_unit.abort();
                Err(err)
            }
        }
    }
}

fn scan_with_cursor<F>(
    cursor: &mut Cursor,
    matches_filter: F,
) -> Result<Vec<Document>, WrongoDBError>
where
    F: Fn(&Document) -> bool,
{
    let mut results = Vec::new();
    while let Some((_, bytes)) = cursor.next()? {
        let doc = decode_document(&bytes)?;
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
    use crate::collection_write_path::CollectionWritePath;
    use crate::durability::DurabilityBackend;
    use crate::replication::ReplicationCoordinator;
    use crate::schema::SchemaCatalog;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::storage::table::Table;
    use crate::store_write_path::StoreWritePath;
    use crate::txn::{GlobalTxnState, TransactionManager};

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

            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let table_handles = Arc::new(HandleCache::<String, parking_lot::RwLock<Table>>::new());
            let metadata_catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                table_handles.clone(),
                transaction_manager.clone(),
            ));
            let schema_catalog = Arc::new(SchemaCatalog::new(
                base_path.clone(),
                metadata_catalog.clone(),
            ));
            let backend = Arc::new(DurabilityBackend::Disabled);
            let query = DocumentQuery::new(schema_catalog.clone());
            let write_path = CollectionWritePath::new(
                metadata_catalog.clone(),
                schema_catalog.clone(),
                query.clone(),
                StoreWritePath::new(
                    base_path.clone(),
                    table_handles.clone(),
                    transaction_manager.clone(),
                    backend.clone(),
                    Arc::new(ReplicationCoordinator::standalone()),
                ),
            );
            let session = Session::new(
                base_path,
                table_handles,
                metadata_catalog,
                transaction_manager,
                backend,
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
            .create_index(&mut session, "test", "name")
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
