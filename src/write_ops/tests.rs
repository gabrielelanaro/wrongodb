use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use super::WriteOps;
use crate::api::DdlPath;
use crate::catalog::{CatalogStore, CollectionCatalog};
use crate::collection_write_path::CollectionWritePath;
use crate::core::{DatabaseName, Namespace};
use crate::document_query::DocumentQuery;
use crate::replication::{
    OplogAwaitService, OplogOperation, OplogStore, ReplicationConfig, ReplicationCoordinator,
    ReplicationObserver, ReplicationRole,
};
use crate::storage::api::{Connection, ConnectionConfig};
use crate::WrongoDBError;

const TEST_DB: &str = "test";
const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

struct TestServices {
    connection: Arc<Connection>,
    ddl_path: DdlPath,
    write_ops: WriteOps,
    query: DocumentQuery,
    oplog_store: OplogStore,
}

fn open_services(
    base_path: &Path,
    connection_config: ConnectionConfig,
    ddl_replication_config: ReplicationConfig,
    write_replication_config: ReplicationConfig,
) -> TestServices {
    let connection = Arc::new(Connection::open(base_path, connection_config).unwrap());
    let metadata_store = connection.metadata_store();
    let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
    let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
    let shared_replication = (ddl_replication_config == write_replication_config)
        .then(|| ReplicationCoordinator::new(ddl_replication_config.clone()));
    let ddl_replication = shared_replication
        .clone()
        .unwrap_or_else(|| ReplicationCoordinator::new(ddl_replication_config));
    let write_replication =
        shared_replication.unwrap_or_else(|| ReplicationCoordinator::new(write_replication_config));

    let mut session = connection.open_session();
    oplog_store.ensure_table_exists(&mut session).unwrap();
    let next_op_index = oplog_store
        .load_last_op_time(&mut session)
        .unwrap()
        .map(|op_time| op_time.index + 1)
        .unwrap_or(1);
    write_replication.seed_next_op_index(next_op_index);
    catalog.ensure_store_exists(&mut session).unwrap();
    catalog.load_cache(&session).unwrap();

    let query = DocumentQuery::new(catalog.clone());
    let collection_write_path =
        CollectionWritePath::new(metadata_store.clone(), catalog.clone(), query.clone());
    let ddl_path = DdlPath::new(
        connection.clone(),
        metadata_store,
        catalog,
        ddl_replication.clone(),
        ReplicationObserver::new(ddl_replication, oplog_store.clone()),
    );
    let write_ops = WriteOps::new(
        connection.clone(),
        collection_write_path,
        ReplicationObserver::new(write_replication.clone(), oplog_store.clone()),
        write_replication,
        OplogAwaitService::new(next_op_index.saturating_sub(1)),
    );

    TestServices {
        connection,
        ddl_path,
        write_ops,
        query,
        oplog_store,
    }
}

fn namespace(collection: &str) -> Namespace {
    Namespace::new(DatabaseName::new(TEST_DB).unwrap(), collection).unwrap()
}

// EARS: When the node is not writable primary, the top-level write executor
// shall reject the write before changing user data or appending oplog rows.
#[test]
fn non_primary_write_is_rejected_before_mutation_and_oplog_append() {
    let dir = tempdir().unwrap();
    let services = open_services(
        dir.path(),
        ConnectionConfig::new().logging_enabled(false),
        ReplicationConfig::default(),
        ReplicationConfig {
            role: ReplicationRole::Secondary,
            node_name: "node-2".to_string(),
            primary_hint: Some("node-2".to_string()),
            sync_source_uri: Some("mongodb://node-2".to_string()),
            term: 8,
        },
    );
    services
        .ddl_path
        .create_collection(&namespace("users"), vec!["name".to_string()])
        .unwrap();

    let err = services
        .write_ops
        .insert_one(&namespace("users"), json!({"_id": 1, "name": "alice"}))
        .unwrap_err();

    assert!(matches!(
        err,
        WrongoDBError::NotLeader {
            leader_hint: Some(ref leader_hint)
        } if leader_hint == "node-2"
    ));

    let mut session = services.connection.open_session();
    assert!(services
        .query
        .find(&mut session, &namespace("users"), Some(json!({"_id": 1})))
        .unwrap()
        .is_empty());
    assert_eq!(
        services
            .oplog_store
            .list_entries(&mut session)
            .unwrap()
            .len(),
        1
    );
}

// EARS: When `updateMany` matches multiple documents, the write executor shall
// append one oplog entry per updated document in commit order.
#[test]
fn update_many_emits_one_oplog_row_per_affected_document_in_order() {
    let dir = tempdir().unwrap();
    let services = open_services(
        dir.path(),
        ConnectionConfig::new().logging_enabled(false),
        ReplicationConfig::default(),
        ReplicationConfig::default(),
    );
    services
        .ddl_path
        .create_collection(
            &namespace("users"),
            vec!["name".to_string(), "age".to_string()],
        )
        .unwrap();

    services
        .write_ops
        .insert_one(
            &namespace("users"),
            json!({"_id": 1, "name": "alice", "age": 30}),
        )
        .unwrap();
    services
        .write_ops
        .insert_one(
            &namespace("users"),
            json!({"_id": 2, "name": "bob", "age": 40}),
        )
        .unwrap();

    let result = services
        .write_ops
        .update_many(&namespace("users"), None, json!({"$set": {"age": 50}}))
        .unwrap();
    assert_eq!(result.matched, 2);
    assert_eq!(result.modified, 2);

    let mut session = services.connection.open_session();
    let entries = services.oplog_store.list_entries(&mut session).unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].op_time.index, 1);
    assert_eq!(entries[1].op_time.index, 2);
    assert_eq!(entries[2].op_time.index, 3);
    assert_eq!(entries[3].op_time.index, 4);
    assert_eq!(entries[4].op_time.index, 5);

    let update_ids: Vec<i64> = entries[3..]
        .iter()
        .map(|entry| match &entry.operation {
            OplogOperation::Update { document_key, .. } => document_key["_id"].as_i64().unwrap(),
            other => panic!("expected update oplog entry, found {other:?}"),
        })
        .collect();
    assert_eq!(update_ids, vec![1, 2]);
}

// EARS: When `deleteMany` matches multiple documents, the write executor shall
// delete the documents and append one oplog entry per deleted document.
#[test]
fn delete_many_emits_one_oplog_row_per_affected_document() {
    let dir = tempdir().unwrap();
    let services = open_services(
        dir.path(),
        ConnectionConfig::new().logging_enabled(false),
        ReplicationConfig::default(),
        ReplicationConfig::default(),
    );
    services
        .ddl_path
        .create_collection(&namespace("users"), vec!["name".to_string()])
        .unwrap();

    services
        .write_ops
        .insert_one(&namespace("users"), json!({"_id": 1, "name": "alice"}))
        .unwrap();
    services
        .write_ops
        .insert_one(&namespace("users"), json!({"_id": 2, "name": "bob"}))
        .unwrap();

    let deleted = services
        .write_ops
        .delete_many(&namespace("users"), None)
        .unwrap();
    assert_eq!(deleted, 2);

    let mut session = services.connection.open_session();
    let entries = services.oplog_store.list_entries(&mut session).unwrap();
    let delete_ids: Vec<i64> = entries[3..]
        .iter()
        .map(|entry| match &entry.operation {
            OplogOperation::Delete { document_key, .. } => document_key["_id"].as_i64().unwrap(),
            other => panic!("expected delete oplog entry, found {other:?}"),
        })
        .collect();
    assert_eq!(delete_ids, vec![1, 2]);
    assert!(services
        .query
        .find(&mut session, &namespace("users"), None)
        .unwrap()
        .is_empty());
}

// EARS: When a committed write is recovered after restart, the oplog store
// shall preserve the logical entry through storage WAL recovery.
#[test]
fn oplog_survives_restart_through_storage_wal_recovery() {
    let dir = tempdir().unwrap();
    {
        let services = open_services(
            dir.path(),
            ConnectionConfig::default(),
            ReplicationConfig::default(),
            ReplicationConfig::default(),
        );
        services
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();
        services
            .write_ops
            .insert_one(&namespace("users"), json!({"_id": 1, "name": "alice"}))
            .unwrap();
    }

    let reopened = Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
    let oplog_store = OplogStore::new(reopened.metadata_store(), TEST_OPLOG_TABLE_URI);
    let mut session = reopened.open_session();
    let entries = oplog_store.list_entries(&mut session).unwrap();

    assert_eq!(entries.len(), 2);
    match &entries[1].operation {
        OplogOperation::Insert { ns, document } => {
            assert_eq!(ns, &namespace("users").full_name());
            assert_eq!(document["name"], json!("alice"));
        }
        other => panic!("expected insert oplog entry, found {other:?}"),
    }
}
