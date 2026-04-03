use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client,
};
use tokio::task::JoinHandle;
use wrongodb::{
    start_server_with_replication, Connection, ConnectionConfig, ReplicationConfig, ReplicationRole,
};

struct RunningNode {
    addr: String,
    client: Client,
    server: JoinHandle<()>,
}

impl RunningNode {
    fn database(&self, name: &str) -> mongodb::Database {
        self.client.database(name)
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_node(db_path: &Path, config: ReplicationConfig) -> RunningNode {
    let conn = Connection::open(db_path, ConnectionConfig::default()).unwrap();
    let conn = Arc::new(conn);
    let (addr, reserved) = crate::common::reserve_local_addr();
    drop(reserved);

    let conn_clone = Arc::clone(&conn);
    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        start_server_with_replication(&server_addr, conn_clone, config)
            .await
            .unwrap();
    });

    crate::common::wait_for_server(&addr).await;

    let client_options = ClientOptions::parse(format!("mongodb://{addr}"))
        .await
        .unwrap();
    let client = Client::with_options(client_options).unwrap();

    RunningNode {
        addr,
        client,
        server,
    }
}

async fn wait_until<F, Fut>(description: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if check().await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn list_index_names(db: &mongodb::Database, collection: &str) -> Option<Vec<String>> {
    let indexes = db
        .run_command(doc! { "listIndexes": collection })
        .await
        .ok()?;
    indexes
        .get_document("cursor")
        .ok()
        .and_then(|cursor| cursor.get_array("firstBatch").ok())
        .map(|batch| {
            batch
                .iter()
                .filter_map(|value| value.as_document())
                .filter_map(|document| document.get_str("name").ok())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
}

async fn secondary_has_replicated_users_ddl(db: &mongodb::Database) -> bool {
    let Some(index_names) = list_index_names(db, "users").await else {
        return false;
    };
    index_names.iter().any(|name| name == "name_1")
}

async fn secondary_has_replicated_users_crud(db: &mongodb::Database) -> bool {
    let users = db.collection::<Document>("users");
    match users.find_one(doc! { "_id": 1 }).await {
        Ok(Some(document)) => {
            let missing_second = users
                .find_one(doc! { "_id": 2 })
                .await
                .ok()
                .flatten()
                .is_none();
            document.get("age").and_then(|value| value.as_i64()) == Some(31) && missing_second
        }
        _ => false,
    }
}

async fn create_users_schema(primary_db: &mongodb::Database) {
    primary_db
        .run_command(doc! {
            "createCollection": "users",
            "storageColumns": ["name", "age"]
        })
        .await
        .unwrap();
    primary_db
        .run_command(doc! {
            "createIndexes": "users",
            "indexes": [{ "key": { "name": 1 }, "name": "name_1" }]
        })
        .await
        .unwrap();
}

async fn apply_users_crud(primary_db: &mongodb::Database) {
    let primary_users = primary_db.collection::<Document>("users");
    primary_users
        .insert_many(vec![
            doc! { "_id": 1, "name": "alice", "age": 30 },
            doc! { "_id": 2, "name": "bob", "age": 40 },
        ])
        .await
        .unwrap();
    primary_db
        .run_command(doc! {
            "update": "users",
            "updates": [{ "q": { "_id": 1 }, "u": { "$set": { "age": 31 } } }]
        })
        .await
        .unwrap();
    primary_db
        .run_command(doc! {
            "delete": "users",
            "deletes": [{ "q": { "_id": 2 }, "limit": 1 }]
        })
        .await
        .unwrap();
}

fn is_not_writable_primary(error: &mongodb::error::Error) -> bool {
    error.to_string().contains("NotWritablePrimary")
}

fn primary_config(node_name: &str) -> ReplicationConfig {
    ReplicationConfig {
        role: ReplicationRole::Primary,
        node_name: node_name.to_string(),
        primary_hint: None,
        sync_source_uri: None,
        term: 1,
    }
}

fn secondary_config(node_name: &str, primary_addr: &str) -> ReplicationConfig {
    let sync_source_uri = format!("mongodb://{primary_addr}");
    ReplicationConfig {
        role: ReplicationRole::Secondary,
        node_name: node_name.to_string(),
        primary_hint: Some(primary_addr.to_string()),
        sync_source_uri: Some(sync_source_uri),
        term: 1,
    }
}

// EARS: When a primary creates a collection and secondary index, the secondary
// shall later expose the same catalog state.
#[tokio::test]
async fn test_secondary_replicates_ddl() {
    let primary_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let primary = start_node(
        &primary_dir.path().join("primary.db"),
        primary_config("primary-a"),
    )
    .await;
    let secondary = start_node(
        &secondary_dir.path().join("secondary.db"),
        secondary_config("secondary-a", &primary.addr),
    )
    .await;

    let primary_db = primary.database("app");
    create_users_schema(&primary_db).await;

    let secondary_db = secondary.database("app");
    wait_until("secondary to apply replicated DDL", || {
        let secondary_db = secondary_db.clone();
        async move { secondary_has_replicated_users_ddl(&secondary_db).await }
    })
    .await;
}

// EARS: When a primary mutates replicated documents, the secondary shall later
// expose the same document state.
#[tokio::test]
async fn test_secondary_replicates_crud() {
    let primary_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let primary = start_node(
        &primary_dir.path().join("primary.db"),
        primary_config("primary-a"),
    )
    .await;
    let secondary = start_node(
        &secondary_dir.path().join("secondary.db"),
        secondary_config("secondary-a", &primary.addr),
    )
    .await;

    let primary_db = primary.database("app");
    create_users_schema(&primary_db).await;
    apply_users_crud(&primary_db).await;

    let secondary_db = secondary.database("app");
    wait_until("secondary to apply replicated CRUD", || {
        let secondary_db = secondary_db.clone();
        async move { secondary_has_replicated_users_crud(&secondary_db).await }
    })
    .await;
}

// EARS: When a node is configured as a secondary, it shall reject direct user
// writes even after replication has initialized its state.
#[tokio::test]
async fn test_secondary_rejects_direct_writes() {
    let primary_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let primary = start_node(
        &primary_dir.path().join("primary.db"),
        primary_config("primary-a"),
    )
    .await;
    let secondary = start_node(
        &secondary_dir.path().join("secondary.db"),
        secondary_config("secondary-a", &primary.addr),
    )
    .await;

    let primary_db = primary.database("app");
    create_users_schema(&primary_db).await;

    let secondary_db = secondary.database("app");
    wait_until("secondary to apply replicated DDL before write checks", || {
        let secondary_db = secondary_db.clone();
        async move { secondary_has_replicated_users_ddl(&secondary_db).await }
    })
    .await;

    let hello = secondary_db.run_command(doc! { "hello": 1 }).await.unwrap();
    assert_eq!(hello.get_bool("isWritablePrimary").unwrap(), false);
    assert_eq!(hello.get_str("primary").unwrap(), primary.addr);

    let err = secondary_db
        .collection::<Document>("users")
        .insert_one(doc! { "_id": 99, "name": "mallory" })
        .await
        .unwrap_err();
    assert!(is_not_writable_primary(&err));

    let err = secondary_db
        .run_command(doc! {
            "createCollection": "attack",
            "storageColumns": ["value"]
        })
        .await
        .unwrap_err();
    assert!(is_not_writable_primary(&err));
}

// EARS: When a secondary restarts from the same data directory, it shall keep
// catching up from its persisted replication state instead of becoming stuck.
#[tokio::test]
async fn test_restarted_secondary_catches_up_again() {
    let primary_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let primary = start_node(
        &primary_dir.path().join("primary.db"),
        primary_config("primary-b"),
    )
    .await;
    let primary_db = primary.database("app");

    primary_db
        .run_command(doc! {
            "createCollection": "items",
            "storageColumns": ["value"]
        })
        .await
        .unwrap();
    primary_db
        .collection::<Document>("items")
        .insert_one(doc! { "_id": 1, "value": "v1" })
        .await
        .unwrap();

    let secondary_db_path = secondary_dir.path().join("secondary.db");
    let secondary_config = secondary_config("secondary-b", &primary.addr);
    let secondary = start_node(&secondary_db_path, secondary_config.clone()).await;
    let secondary_db = secondary.database("app");

    wait_until("secondary to receive first replicated document", || {
        let secondary_db = secondary_db.clone();
        async move {
            secondary_db
                .collection::<Document>("items")
                .find_one(doc! { "_id": 1 })
                .await
                .ok()
                .flatten()
                .is_some()
        }
    })
    .await;

    drop(secondary);

    primary_db
        .collection::<Document>("items")
        .insert_one(doc! { "_id": 2, "value": "v2" })
        .await
        .unwrap();

    let restarted_secondary = start_node(&secondary_db_path, secondary_config).await;
    let restarted_db = restarted_secondary.database("app");
    wait_until("restarted secondary to catch up", || {
        let restarted_db = restarted_db.clone();
        async move {
            let items = restarted_db.collection::<Document>("items");
            items
                .find_one(doc! { "_id": 1 })
                .await
                .ok()
                .flatten()
                .is_some()
                && items
                    .find_one(doc! { "_id": 2 })
                    .await
                    .ok()
                    .flatten()
                    .is_some()
        }
    })
    .await;
}
