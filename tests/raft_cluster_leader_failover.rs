mod common;

use std::thread;
use std::time::Duration;

use serde_json::json;
use tempfile::tempdir;

use common::raft_cluster::{
    build_cluster, find_single_leader, find_single_leader_among, insert_doc,
};

#[test]
fn raft_cluster_elects_new_leader_after_leader_failure() {
    let tmp = tempdir().unwrap();
    let mut nodes = build_cluster(&tmp, 3);

    let initial_leader =
        find_single_leader(&mut nodes, "raft_failover_probe", Duration::from_secs(6));
    insert_doc(
        nodes[initial_leader].db(),
        "raft_failover_data",
        json!({"_id": "before-failover", "value": 1}),
    )
    .unwrap();

    nodes[initial_leader].drop_db();
    thread::sleep(Duration::from_millis(700));

    let remaining: Vec<usize> = (0..nodes.len())
        .filter(|idx| nodes[*idx].db.is_some())
        .collect();
    let new_leader = find_single_leader_among(
        &mut nodes,
        &remaining,
        "raft_failover_probe",
        Duration::from_secs(20),
    );
    assert_ne!(new_leader, initial_leader);

    insert_doc(
        nodes[new_leader].db(),
        "raft_failover_data",
        json!({"_id": "after-failover", "value": 2}),
    )
    .unwrap();
}
