mod common;

use std::time::Duration;

use tempfile::tempdir;

use common::raft_cluster::{build_cluster, find_single_leader};

#[test]
fn raft_cluster_elects_single_leader() {
    let tmp = tempdir().unwrap();
    let mut nodes = build_cluster(&tmp, 3);

    let leader = find_single_leader(&mut nodes, "raft_election_probe", Duration::from_secs(6));
    assert!(leader < nodes.len());

    let leader_second_probe =
        find_single_leader(&mut nodes, "raft_election_probe", Duration::from_secs(6));
    assert!(leader_second_probe < nodes.len());
}
