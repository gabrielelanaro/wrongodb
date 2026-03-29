use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use super::WriteOps;
use crate::api::DdlPath;
use crate::catalog::{CatalogStore, CollectionCatalog};
use crate::collection_write_path::CollectionWritePath;
use crate::document_query::DocumentQuery;
use crate::replication::{
    OplogOperation, OplogStore, ReplicationConfig, ReplicationCoordinator, ReplicationObserver,
};
use crate::storage::api::{Connection, ConnectionConfig};
use crate::WrongoDBError;

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
    let oplog_store = OplogStore::new(metadata_store.clone());
    let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
    let ddl_replication = ReplicationCoordinator::new(ddl_replication_config);
    let write_replication = ReplicationCoordinator::new(write_replication_config);

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
    let collection_write_path = CollectionWritePath::new(
        metadata_store.clone(),
        catalog.clone(),
        query.clone(),
        ReplicationObserver::new(write_replication.clone(), oplog_store.clone()),
    );
    let ddl_path = DdlPath::new(connection.clone(), metadata_store, catalog, ddl_replication);
    let write_ops = WriteOps::new(connection.clone(), collection_write_path, write_replication);

    TestServices {
        connection,
        ddl_path,
        write_ops,
        query,
        oplog_store,
    }
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
            is_writable_primary: false,
            primary_hint: Some("node-2".to_string()),
            term: 8,
        },
    );
    services
        .ddl_path
        .create_collection("users", vec!["name".to_string()])
        .unwrap();

    let err = services
        .write_ops
        .insert_one("users", json!({"_id": 1, "name": "alice"}))
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
        .find(&mut session, "users", Some(json!({"_id": 1})))
        .unwrap()
        .is_empty());
    assert!(services
        .oplog_store
        .list_entries(&mut session)
        .unwrap()
        .is_empty());
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
        .create_collection("users", vec!["name".to_string(), "age".to_string()])
        .unwrap();

    services
        .write_ops
        .insert_one("users", json!({"_id": 1, "name": "alice", "age": 30}))
        .unwrap();
    services
        .write_ops
        .insert_one("users", json!({"_id": 2, "name": "bob", "age": 40}))
        .unwrap();

    let result = services
        .write_ops
        .update_many("users", None, json!({"$set": {"age": 50}}))
        .unwrap();
    assert_eq!(result.matched, 2);
    assert_eq!(result.modified, 2);

    let mut session = services.connection.open_session();
    let entries = services.oplog_store.list_entries(&mut session).unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].op_time.index, 1);
    assert_eq!(entries[1].op_time.index, 2);
    assert_eq!(entries[2].op_time.index, 3);
    assert_eq!(entries[3].op_time.index, 4);

    let update_ids: Vec<i64> = entries[2..]
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
        .create_collection("users", vec!["name".to_string()])
        .unwrap();

    services
        .write_ops
        .insert_one("users", json!({"_id": 1, "name": "alice"}))
        .unwrap();
    services
        .write_ops
        .insert_one("users", json!({"_id": 2, "name": "bob"}))
        .unwrap();

    let deleted = services.write_ops.delete_many("users", None).unwrap();
    assert_eq!(deleted, 2);

    let mut session = services.connection.open_session();
    let entries = services.oplog_store.list_entries(&mut session).unwrap();
    let delete_ids: Vec<i64> = entries[2..]
        .iter()
        .map(|entry| match &entry.operation {
            OplogOperation::Delete { document_key, .. } => document_key["_id"].as_i64().unwrap(),
            other => panic!("expected delete oplog entry, found {other:?}"),
        })
        .collect();
    assert_eq!(delete_ids, vec![1, 2]);
    assert!(services
        .query
        .find(&mut session, "users", None)
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
            .create_collection("users", vec!["name".to_string()])
            .unwrap();
        services
            .write_ops
            .insert_one("users", json!({"_id": 1, "name": "alice"}))
            .unwrap();
    }

    let reopened = Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
    let oplog_store = OplogStore::new(reopened.metadata_store());
    let mut session = reopened.open_session();
    let entries = oplog_store.list_entries(&mut session).unwrap();

    assert_eq!(entries.len(), 1);
    match &entries[0].operation {
        OplogOperation::Insert { ns, document } => {
            assert_eq!(ns, "test.users");
            assert_eq!(document["name"], json!("alice"));
        }
        other => panic!("expected insert oplog entry, found {other:?}"),
    }
}
