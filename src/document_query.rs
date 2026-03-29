use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;

use crate::catalog::CollectionCatalog;
use crate::core::bson::encode_id_value;
use crate::core::document::validate_is_object;
use crate::core::Namespace;
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
    catalog: Arc<CollectionCatalog>,
}

impl DocumentQuery {
    /// Creates the document query service.
    pub(crate) fn new(catalog: Arc<CollectionCatalog>) -> Self {
        Self { catalog }
    }

    pub(crate) fn find(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        session.with_transaction(|session| self.find_in_transaction(session, namespace, filter))
    }

    /// Counts the documents matching `filter`.
    pub(crate) fn count(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(session, namespace, filter)?.len())
    }

    /// Returns the distinct values of `key` among the matching documents.
    pub(crate) fn distinct(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(session, namespace, filter)?;
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
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let Some(collection_definition) = self.catalog.get_collection(session, namespace)? else {
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

            let mut index_cursor = session.open_index_cursor(index.uri())?;
            index_cursor.set_range(Some(start_key), Some(end_key));

            let mut results = Vec::new();
            while let Some((key, _)) = index_cursor.next()? {
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
    use crate::api::DdlPath;
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest};
    use crate::collection_write_path::CollectionWritePath;
    use crate::core::{DatabaseName, Namespace};
    use crate::replication::{
        OplogMode, OplogStore, ReplicationConfig, ReplicationCoordinator, ReplicationObserver,
    };
    use crate::storage::api::{Connection, ConnectionConfig};

    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    struct QueryTestFixture {
        connection: Arc<Connection>,
        ddl_path: DdlPath,
        query: DocumentQuery,
        write_path: CollectionWritePath,
    }

    impl QueryTestFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);

            let config = ConnectionConfig::new().logging_enabled(false);
            let connection = Arc::new(Connection::open(&base_path, config).unwrap());
            let metadata_store = connection.metadata_store();
            let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
            let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
            let replication = ReplicationCoordinator::new(ReplicationConfig::default());
            {
                let mut session = connection.open_session();
                oplog_store.ensure_table_exists(&mut session).unwrap();
                let next_op_index = oplog_store
                    .load_last_op_time(&mut session)
                    .unwrap()
                    .map(|op_time| op_time.index + 1)
                    .unwrap_or(1);
                replication.seed_next_op_index(next_op_index);
                catalog.ensure_store_exists(&mut session).unwrap();
                catalog.load_cache(&session).unwrap();
            }
            let query = DocumentQuery::new(catalog.clone());
            let write_path = CollectionWritePath::new(
                metadata_store.clone(),
                catalog.clone(),
                query.clone(),
                ReplicationObserver::new(replication.clone(), oplog_store),
            );
            let ddl_path = DdlPath::new(connection.clone(), metadata_store, catalog, replication);

            Self {
                connection,
                ddl_path,
                query,
                write_path,
            }
        }
    }

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new("test").unwrap(), collection).unwrap()
    }

    // EARS: When an indexed lookup runs after checkpoint reconciliation, the
    // query layer shall still find the matching document through the durable
    // index definition.
    #[test]
    fn indexed_lookup_survives_checkpoint_reconciliation() {
        let fixture = QueryTestFixture::new();
        let mut session = fixture.connection.open_session();

        fixture
            .ddl_path
            .create_collection(&namespace("test"), vec!["name".to_string()])
            .unwrap();
        fixture
            .ddl_path
            .create_index(
                &namespace("test"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();
        session
            .with_transaction(|session| {
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("test"),
                    json!({"_id": 1, "name": "alice"}),
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();

        session.checkpoint().unwrap();

        let docs = fixture
            .query
            .find(
                &mut session,
                &namespace("test"),
                Some(json!({"name": "alice"})),
            )
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name"), Some(&json!("alice")));
    }
}
