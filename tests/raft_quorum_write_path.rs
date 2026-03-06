use std::net::TcpListener;

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
    session.create("table:test")?;
    let mut cursor = session.open_cursor("table:test")?;
    cursor.insert(key, value, 0)?;
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

    let session = conn.open_session();
    let mut cursor = session.open_cursor("table:test").unwrap();
    let value = cursor.get(b"alice", 0).unwrap().unwrap();
    assert_eq!(value, b"value".to_vec());
}

#[test]
fn cluster_mode_rejects_public_cursor_writes() {
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
    assert!(err
        .to_string()
        .contains("cursor writes are not available when durability defers apply"));
}
