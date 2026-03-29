use serde_json::json;
use tempfile::tempdir;

use super::*;
use crate::storage::api::{Connection, ConnectionConfig};
use crate::WrongoDBError;

fn open_store() -> (Connection, OplogStore) {
    let dir = tempdir().unwrap();
    let base_path = dir.path().to_path_buf();
    std::mem::forget(dir);

    let connection = Connection::open(&base_path, ConnectionConfig::new()).unwrap();
    let oplog_store = OplogStore::new(connection.metadata_store());
    let mut session = connection.open_session();
    oplog_store.ensure_table_exists(&mut session).unwrap();

    (connection, oplog_store)
}

// TODO: the unit test should be close to their module like in rust best practice (unless they are integratino tests)

// EARS: When the coordinator uses default replication state, it shall report
// the node as writable primary and start at term `1`.
#[test]
fn default_replication_state_is_writable() {
    let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());

    assert_eq!(coordinator.hello_state(), (true, None));
    assert_eq!(coordinator.current_term(), 1);
}

// EARS: When the coordinator marks the node as non-primary, it shall reject
// writes and surface the configured leader hint.
#[test]
fn non_primary_rejects_writes_with_leader_hint() {
    let coordinator = ReplicationCoordinator::new(ReplicationConfig {
        is_writable_primary: false,
        primary_hint: Some("node-2".to_string()),
        term: 9,
    });

    let err = coordinator.require_writable_primary().unwrap_err();
    assert!(matches!(
        err,
        WrongoDBError::NotLeader {
            leader_hint: Some(ref leader_hint)
        } if leader_hint == "node-2"
    ));
}

// EARS: When an oplog entry is appended and later reloaded, the oplog store
// shall preserve both the logical operation and its replication position.
#[test]
fn oplog_store_roundtrips_entries() {
    let (connection, oplog_store) = open_store();
    let entry = OplogEntry {
        op_time: OpTime { term: 3, index: 42 },
        operation: OplogOperation::Update {
            ns: "test.users".to_string(),
            document: json!({"_id": 7, "name": "alice"})
                .as_object()
                .unwrap()
                .clone(),
            document_key: json!({"_id": 7}).as_object().unwrap().clone(),
        },
    };

    let mut session = connection.open_session();
    session
        .with_transaction(|session| oplog_store.append(session, &entry))
        .unwrap();

    let entries = oplog_store.list_entries(&mut session).unwrap();
    assert_eq!(entries, vec![entry]);
    assert_eq!(
        oplog_store.load_last_op_time(&mut session).unwrap(),
        Some(OpTime { term: 3, index: 42 })
    );
}

// EARS: When the observer runs in suppress-oplog mode, it shall not append any
// oplog rows.
#[test]
fn observer_skips_writes_in_suppress_mode() {
    let (connection, oplog_store) = open_store();
    let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());
    let observer = ReplicationObserver::new(coordinator, oplog_store.clone());
    let document = json!({"_id": 1, "name": "alice"})
        .as_object()
        .unwrap()
        .clone();

    let mut session = connection.open_session();
    session
        .with_transaction(|session| {
            observer.on_insert(session, "users", &document, OplogMode::SuppressOplog)
        })
        .unwrap();

    assert!(oplog_store.list_entries(&mut session).unwrap().is_empty());
}
