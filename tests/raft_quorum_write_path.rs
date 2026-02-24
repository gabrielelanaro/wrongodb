use std::net::TcpListener;

use serde_json::json;
use tempfile::tempdir;

use wrongodb::{RaftMode, RaftPeerConfig, WrongoDB, WrongoDBConfig, WrongoDBError};

fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

#[test]
fn standalone_mode_commits_writes_with_majority_one() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let db =
        WrongoDB::open_with_config(&path, WrongoDBConfig::new().raft_mode(RaftMode::Standalone))
            .unwrap();

    let coll = db.collection("test");
    let mut session = db.open_session();
    let inserted = coll
        .insert_one(&mut session, json!({"name": "alice"}))
        .unwrap();
    assert_eq!(inserted.get("name").unwrap().as_str().unwrap(), "alice");
}

#[test]
fn cluster_mode_rejects_write_when_node_is_not_leader() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let local_raft_addr = free_local_addr();
    let peer_raft_addr = free_local_addr();
    let db = WrongoDB::open_with_config(
        &path,
        WrongoDBConfig::new().raft_mode(RaftMode::Cluster {
            local_node_id: "n1".to_string(),
            local_raft_addr,
            peers: vec![RaftPeerConfig {
                node_id: "n2".to_string(),
                raft_addr: peer_raft_addr,
            }],
        }),
    )
    .unwrap();

    let coll = db.collection("test");
    let mut session = db.open_session();
    let err = coll
        .insert_one(&mut session, json!({"name": "alice"}))
        .unwrap_err();
    match err {
        WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
        other => panic!("unexpected error: {other}"),
    }
}
