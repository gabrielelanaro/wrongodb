use serde_json::json;
use tempfile::tempdir;

use wrongodb::{RaftMode, WrongoDB, WrongoDBConfig, WrongoDBError};

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
    let db = WrongoDB::open_with_config(
        &path,
        WrongoDBConfig::new().raft_mode(RaftMode::Cluster {
            local_node_id: "n1".to_string(),
            peer_ids: vec!["n2".to_string()],
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
