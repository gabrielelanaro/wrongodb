mod common;

use std::time::Duration;

use serde_json::json;
use tempfile::tempdir;

use common::raft_cluster::{build_cluster, find_single_leader, insert_doc};
use wrongodb::WrongoDBError;

#[test]
fn raft_cluster_follower_rejects_writes_with_not_leader() {
    let tmp = tempdir().unwrap();
    let mut nodes = build_cluster(&tmp, 3);

    let leader = find_single_leader(&mut nodes, "raft_follower_probe", Duration::from_secs(6));
    let follower = (0..nodes.len()).find(|idx| *idx != leader).unwrap();

    let err = insert_doc(
        nodes[follower].db(),
        "raft_follower_data",
        json!({"_id": "follower-write-reject", "value": "x"}),
    )
    .unwrap_err();

    match err {
        WrongoDBError::NotLeader { .. } => {}
        other => panic!("expected NotLeader from follower write, got: {other}"),
    }
}
