mod common;

use std::thread;
use std::time::Duration;

use serde_json::json;
use tempfile::tempdir;

use common::raft_cluster::{build_cluster, find_docs, find_single_leader, insert_doc};
use wrongodb::{Connection, ConnectionConfig, RaftMode};

#[test]
fn raft_cluster_restart_recovers_replicated_wal_tail() {
    let tmp = tempdir().unwrap();
    let mut nodes = build_cluster(&tmp, 3);

    let leader = find_single_leader(&mut nodes, "raft_restart_probe", Duration::from_secs(6));
    let follower = (0..nodes.len()).find(|idx| *idx != leader).unwrap();

    insert_doc(
        nodes[leader].db(),
        "raft_restart_data",
        json!({"_id": "restart-visible", "value": 42}),
    )
    .unwrap();

    thread::sleep(Duration::from_millis(800));
    nodes[follower].drop_db();
    thread::sleep(Duration::from_millis(200));
    let recovered = Connection::open(
        &nodes[follower].db_path,
        ConnectionConfig::new().raft_mode(RaftMode::Standalone),
    )
    .unwrap();

    let docs = find_docs(
        &recovered,
        "raft_restart_data",
        json!({"_id": "restart-visible"}),
    )
    .unwrap();
    assert!(
        !docs.is_empty(),
        "expected follower to recover replicated write after restart"
    );
}
