#![allow(dead_code)]

pub mod raft_cluster;

pub async fn wait_for_server() {
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}
