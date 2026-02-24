mod common;

use std::thread;
use std::time::Duration;

use serde_json::json;
use tempfile::tempdir;

use common::raft_cluster::{
    build_cluster, find_single_leader, find_single_leader_among, insert_doc,
};
use wrongodb::WrongoDBError;

#[test]
fn raft_quorum_write_behavior_with_one_and_two_followers_down() {
    let tmp = tempdir().unwrap();
    let mut nodes = build_cluster(&tmp, 3);

    let leader = find_single_leader(&mut nodes, "raft_quorum_probe", Duration::from_secs(6));
    insert_doc(
        nodes[leader].db(),
        "raft_quorum_data",
        json!({"_id": "initial-write", "value": 1}),
    )
    .unwrap();

    let down_one = (0..nodes.len()).find(|idx| *idx != leader).unwrap();
    nodes[down_one].drop_db();
    thread::sleep(Duration::from_millis(300));

    let active: Vec<usize> = (0..nodes.len())
        .filter(|idx| nodes[*idx].db.is_some())
        .collect();
    let leader_after_one_down = find_single_leader_among(
        &mut nodes,
        &active,
        "raft_quorum_probe",
        Duration::from_secs(6),
    );
    insert_doc(
        nodes[leader_after_one_down].db(),
        "raft_quorum_data",
        json!({"_id": "one-follower-down", "value": 2}),
    )
    .unwrap();

    let down_two = active
        .into_iter()
        .find(|idx| *idx != leader_after_one_down)
        .unwrap();
    nodes[down_two].drop_db();
    thread::sleep(Duration::from_millis(300));

    let err = insert_doc(
        nodes[leader_after_one_down].db(),
        "raft_quorum_data",
        json!({"_id": "two-followers-down", "value": 3}),
    )
    .unwrap_err();
    match err {
        WrongoDBError::NotLeader { .. } => {}
        other if other.to_string().contains("timed out before quorum commit") => {}
        other => panic!("expected quorum failure, got: {other}"),
    }
}
