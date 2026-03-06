use std::net::TcpListener;

use serde_json::json;
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig, RaftMode, RaftPeerConfig, WrongoDBError};

fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn insert_kv(conn: &Connection, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
    let mut session = conn.open_session();
    session.insert_one(
        "test",
        json!({
            "_id": String::from_utf8_lossy(key).to_string(),
            "value": String::from_utf8_lossy(value).to_string(),
        }),
    )?;
    Ok(())
}

#[test]
fn standalone_mode_commits_writes_with_majority_one() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let conn = Connection::open(
        &path,
        ConnectionConfig::new().raft_mode(RaftMode::Standalone),
    )
    .unwrap();

    insert_kv(&conn, b"alice", b"value").unwrap();

    let mut session = conn.open_session();
    let doc = session
        .find_one("test", Some(json!({"_id": "alice"})))
        .unwrap()
        .unwrap();
    assert_eq!(doc.get("value"), Some(&json!("value")));
}

#[test]
fn cluster_mode_rejects_write_when_node_is_not_leader() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let local_raft_addr = free_local_addr();
    let peer_raft_addr = free_local_addr();
    let conn = Connection::open(
        &path,
        ConnectionConfig::new().raft_mode(RaftMode::Cluster {
            local_node_id: "n1".to_string(),
            local_raft_addr,
            peers: vec![RaftPeerConfig {
                node_id: "n2".to_string(),
                raft_addr: peer_raft_addr,
            }],
        }),
    )
    .unwrap();

    let err = insert_kv(&conn, b"alice", b"value").unwrap_err();
    match err {
        WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
        other => panic!("unexpected error: {other}"),
    }
}
