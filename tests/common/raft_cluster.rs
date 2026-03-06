#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;

use wrongodb::{Connection, ConnectionConfig, RaftMode, RaftPeerConfig, WrongoDBError};

#[derive(Debug)]
pub struct ClusterNode {
    pub id: String,
    pub raft_addr: String,
    pub db_path: PathBuf,
    pub mode: RaftMode,
    pub db: Option<Connection>,
}

impl ClusterNode {
    pub fn db(&self) -> &Connection {
        self.db.as_ref().expect("cluster node is not running")
    }

    pub fn drop_db(&mut self) {
        self.db.take();
    }

    pub fn restart(&mut self) {
        let db = Connection::open(
            &self.db_path,
            ConnectionConfig::new().raft_mode(self.mode.clone()),
        )
        .unwrap();
        self.db = Some(db);
    }
}

pub fn build_cluster(tmp: &TempDir, node_count: usize) -> Vec<ClusterNode> {
    let ids: Vec<String> = (1..=node_count).map(|n| format!("n{n}")).collect();
    let addrs: Vec<String> = (0..node_count).map(|_| free_local_addr()).collect();
    let mut nodes = Vec::with_capacity(node_count);

    for i in 0..node_count {
        let peers = ids
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != i)
            .map(|(idx, id)| RaftPeerConfig {
                node_id: id.clone(),
                raft_addr: addrs[idx].clone(),
            })
            .collect::<Vec<_>>();
        let mode = RaftMode::Cluster {
            local_node_id: ids[i].clone(),
            local_raft_addr: addrs[i].clone(),
            peers,
        };
        let db_path = tmp.path().join(format!("{}.db", ids[i]));
        let db =
            Connection::open(&db_path, ConnectionConfig::new().raft_mode(mode.clone())).unwrap();
        nodes.push(ClusterNode {
            id: ids[i].clone(),
            raft_addr: addrs[i].clone(),
            db_path,
            mode,
            db: Some(db),
        });
    }

    nodes
}

pub fn find_single_leader(
    nodes: &mut [ClusterNode],
    probe_collection: &str,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    let mut round = 0u64;

    while Instant::now() < deadline {
        round += 1;
        let mut leaders = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            let Some(db) = node.db.as_ref() else {
                continue;
            };
            let probe = json!({"_id": format!("leader-probe-{round}-{idx}"), "round": round});
            match insert_doc(db, probe_collection, probe) {
                Ok(()) => leaders.push(idx),
                Err(WrongoDBError::NotLeader { .. }) => {}
                Err(err) if err.to_string().contains("timed out before quorum commit") => {}
                Err(err) => panic!("unexpected raft write error while probing leader: {err}"),
            }
        }

        if leaders.len() == 1 {
            return leaders[0];
        }

        thread::sleep(Duration::from_millis(50));
    }

    panic!("failed to identify a single raft leader within {timeout:?}");
}

pub fn find_single_leader_among(
    nodes: &mut [ClusterNode],
    candidate_indexes: &[usize],
    probe_collection: &str,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    let mut round = 0u64;

    while Instant::now() < deadline {
        round += 1;
        let mut leaders = Vec::new();
        for idx in candidate_indexes {
            let Some(db) = nodes[*idx].db.as_ref() else {
                continue;
            };
            let probe = json!({"_id": format!("leader-probe-subset-{round}-{idx}")});
            match insert_doc(db, probe_collection, probe) {
                Ok(()) => leaders.push(*idx),
                Err(WrongoDBError::NotLeader { .. }) => {}
                Err(err) if err.to_string().contains("timed out before quorum commit") => {}
                Err(err) => {
                    panic!("unexpected raft write error while probing leader subset: {err}")
                }
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        thread::sleep(Duration::from_millis(50));
    }

    panic!("failed to identify a single raft leader in subset within {timeout:?}");
}

pub fn insert_doc(db: &Connection, collection: &str, value: Value) -> Result<(), WrongoDBError> {
    let mut session = db.open_session();
    session.insert_one(collection, value)?;
    Ok(())
}

pub fn find_docs(
    db: &Connection,
    collection: &str,
    filter: Value,
) -> Result<Vec<wrongodb::Document>, WrongoDBError> {
    let filter_obj = match filter {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    let mut session = db.open_session();
    session.find(collection, Some(Value::Object(filter_obj)))
}

fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}
