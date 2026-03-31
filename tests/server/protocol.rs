use futures_util::stream::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Bson, Document},
    options::ClientOptions,
    Client,
};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use wrongodb::{start_server, Connection, ConnectionConfig};

struct TestServer {
    _dir: TempDir,
    client: Client,
    server: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("server.db");
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let conn = Arc::new(conn);
        let (addr, reserved) = crate::common::reserve_local_addr();
        drop(reserved);

        let conn_clone = Arc::clone(&conn);
        let server_addr = addr.clone();
        let server = tokio::spawn(async move {
            start_server(&server_addr, conn_clone).await.unwrap();
        });

        crate::common::wait_for_server(&addr).await;

        let client_options = ClientOptions::parse(format!("mongodb://{addr}"))
            .await
            .unwrap();
        let client = Client::with_options(client_options).unwrap();

        Self {
            _dir: dir,
            client,
            server,
        }
    }

    fn database(&self, name: &str) -> mongodb::Database {
        self.client.database(name)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn cursor_namespace(response: &Document) -> &str {
    response
        .get_document("cursor")
        .unwrap()
        .get_str("ns")
        .unwrap()
}

fn database_names(response: &Document) -> Vec<String> {
    response
        .get_array("databases")
        .unwrap()
        .iter()
        .filter_map(|value| match value {
            Bson::Document(document) => document.get_str("name").ok().map(str::to_string),
            _ => None,
        })
        .collect()
}

fn cursor_id(response: &Document) -> i64 {
    response
        .get_document("cursor")
        .unwrap()
        .get_i64("id")
        .unwrap()
}

// EARS: When a MongoDB client exercises the supported CRUD and cursor commands,
// the server shall accept the end-to-end flow over the wire protocol.
#[tokio::test]
async fn test_mongo_client_connection() {
    let server = TestServer::start().await;
    let db_client = server.database("test");
    db_client.run_command(doc! { "ping": 1 }).await.unwrap();

    let coll_cursor = db_client.list_collections().await.unwrap();
    let collections: Vec<_> = coll_cursor.try_collect().await.unwrap();
    assert!(collections.is_empty());

    db_client
        .run_command(doc! { "createCollection": "test", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db_client.collection("test");
    coll.insert_one(doc! { "name": "test" }).await.unwrap();

    let update_cmd = doc! {
        "update": "test",
        "updates": [
            { "q": { "name": "test" }, "u": { "$set": { "name": "test" } } }
        ]
    };
    db_client.run_command(update_cmd).await.unwrap();

    let delete_cmd = doc! {
        "delete": "test",
        "deletes": [
            { "q": { "name": "does-not-exist" }, "limit": 1 }
        ]
    };
    db_client.run_command(delete_cmd).await.unwrap();

    let list_indexes = db_client
        .run_command(doc! { "listIndexes": "test" })
        .await
        .unwrap();
    assert!(list_indexes.get_document("cursor").is_ok());

    db_client
        .run_command(doc! { "getMore": 0_i32, "collection": "test" })
        .await
        .unwrap();

    db_client
        .run_command(doc! { "killCursors": "test", "cursors": [0_i32] })
        .await
        .unwrap();

    // Find
    let cursor = coll.find(doc! { "name": "test" }).await.unwrap();
    let docs: Vec<_> = cursor.collect().await;
    assert_eq!(docs.len(), 1);
}

// EARS: When mongosh-compatible commands run against the server, the command
// handlers shall return successful replies for the supported command set.
#[tokio::test]
async fn test_supported_mongosh_commands() {
    let server = TestServer::start().await;
    let admin_db = server.database("admin");
    let db_client = server.database("test");
    let coll = db_client.collection("test");

    let assert_ok = |d: &mongodb::bson::Document| {
        let ok = d.get_f64("ok").ok().unwrap_or(0.0);
        assert_eq!(ok, 1.0);
    };

    assert_ok(&db_client.run_command(doc! { "hello": 1 }).await.unwrap());
    assert_ok(
        &db_client
            .run_command(doc! { "buildInfo": 1 })
            .await
            .unwrap(),
    );
    assert_ok(
        &db_client
            .run_command(doc! { "serverStatus": 1 })
            .await
            .unwrap(),
    );
    assert_ok(
        &admin_db
            .run_command(doc! { "listDatabases": 1 })
            .await
            .unwrap(),
    );

    let coll_cursor = db_client.list_collections().await.unwrap();
    let _collections: Vec<_> = coll_cursor.try_collect().await.unwrap();

    assert_ok(
        &db_client
            .run_command(doc! { "createCollection": "test", "storageColumns": ["name"] })
            .await
            .unwrap(),
    );

    assert_ok(
        &db_client
            .run_command(doc! { "listIndexes": "test" })
            .await
            .unwrap(),
    );

    assert_ok(
        &db_client
            .run_command(doc! { "createIndexes": "test", "indexes": [ { "key": { "name": 1 }, "name": "name_1" } ] })
            .await
            .unwrap(),
    );

    assert_ok(
        &admin_db
            .run_command(doc! { "connectionStatus": 1 })
            .await
            .unwrap(),
    );

    assert_ok(&db_client.run_command(doc! { "dbStats": 1 }).await.unwrap());
    assert_ok(
        &db_client
            .run_command(doc! { "collStats": "test" })
            .await
            .unwrap(),
    );

    coll.insert_many(vec![doc! { "name": "a" }, doc! { "name": "b" }])
        .await
        .unwrap();

    let find_cmd = doc! { "find": "test", "limit": 0 };
    let find_res = db_client.run_command(find_cmd).await.unwrap();
    let cursor_doc = find_res.get_document("cursor").unwrap();
    let first_batch = cursor_doc.get_array("firstBatch").unwrap();
    assert!(first_batch.len() >= 2);

    assert_ok(
        &db_client
            .run_command(doc! { "getMore": 0_i32, "collection": "test" })
            .await
            .unwrap(),
    );

    assert_ok(
        &db_client
            .run_command(doc! { "killCursors": "test", "cursors": [0_i32] })
            .await
            .unwrap(),
    );
}

// EARS: When two databases each define a collection with the same name, the
// server shall keep their data and command cursor namespaces isolated by
// database name.
#[tokio::test]
async fn test_multi_db_isolation_and_cursor_namespaces() {
    let server = TestServer::start().await;
    let admin_db = server.database("admin");
    let foo_db = server.database("foo");
    let bar_db = server.database("bar");

    foo_db
        .run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();
    bar_db
        .run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    foo_db
        .collection::<Document>("users")
        .insert_one(doc! { "name": "alice" })
        .await
        .unwrap();
    bar_db
        .collection::<Document>("users")
        .insert_one(doc! { "name": "bob" })
        .await
        .unwrap();

    let foo_user = foo_db
        .collection::<Document>("users")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    let bar_user = bar_db
        .collection::<Document>("users")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(foo_user.get_str("name").unwrap(), "alice");
    assert_eq!(bar_user.get_str("name").unwrap(), "bob");

    let foo_collections = foo_db
        .run_command(doc! { "listCollections": 1 })
        .await
        .unwrap();
    assert_eq!(
        cursor_namespace(&foo_collections),
        "foo.$cmd.listCollections"
    );

    let foo_indexes = foo_db
        .run_command(doc! { "listIndexes": "users" })
        .await
        .unwrap();
    assert_eq!(cursor_namespace(&foo_indexes), "foo.users");

    let foo_find = foo_db.run_command(doc! { "find": "users" }).await.unwrap();
    assert_eq!(cursor_namespace(&foo_find), "foo.users");

    let database_names = database_names(
        &admin_db
            .run_command(doc! { "listDatabases": 1 })
            .await
            .unwrap(),
    );
    assert!(database_names.contains(&"local".to_string()));
    assert!(database_names.contains(&"foo".to_string()));
    assert!(database_names.contains(&"bar".to_string()));
}

// EARS: When `listDatabases` runs outside the `admin` database, the server
// shall reject the command instead of silently treating another database as
// admin.
#[tokio::test]
async fn test_list_databases_requires_admin_database() {
    let server = TestServer::start().await;
    let err = server
        .database("foo")
        .run_command(doc! { "listDatabases": 1 })
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("listDatabases must run against the admin database"));
}

// EARS: When a `find` response is shorter than the full result set, the server
// shall return a non-zero cursor id and only the requested first batch.
#[tokio::test]
async fn test_find_returns_non_zero_cursor_when_batch_is_short() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db.collection::<Document>("users");
    coll.insert_many(vec![doc! { "name": "alice" }, doc! { "name": "bob" }])
        .await
        .unwrap();

    let response = db
        .run_command(doc! { "find": "users", "batchSize": 1 })
        .await
        .unwrap();

    let cursor = response.get_document("cursor").unwrap();
    assert!(cursor.get_i64("id").unwrap() > 0);
    assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 1);
}

// EARS: When `singleBatch` is set on `find`, the server shall return only the
// first batch and close the cursor immediately.
#[tokio::test]
async fn test_find_single_batch_closes_the_cursor() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db.collection::<Document>("users");
    coll.insert_many(vec![doc! { "name": "alice" }, doc! { "name": "bob" }])
        .await
        .unwrap();

    let response = db
        .run_command(doc! { "find": "users", "batchSize": 1, "singleBatch": true })
        .await
        .unwrap();

    let cursor = response.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 1);
}

// EARS: When `getMore` continues a live find cursor, it shall return the next
// batch and close the cursor when the results are exhausted.
#[tokio::test]
async fn test_get_more_drains_remaining_batches() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db.collection::<Document>("users");
    coll.insert_many(vec![doc! { "name": "alice" }, doc! { "name": "bob" }])
        .await
        .unwrap();

    let response = db
        .run_command(doc! { "find": "users", "batchSize": 1 })
        .await
        .unwrap();
    let live_cursor_id = cursor_id(&response);

    let get_more = db
        .run_command(doc! { "getMore": live_cursor_id, "collection": "users" })
        .await
        .unwrap();
    let cursor = get_more.get_document("cursor").unwrap();
    let next_batch = cursor.get_array("nextBatch").unwrap();

    assert_eq!(next_batch.len(), 1);
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
}

// EARS: When `find` has both `limit` and `batchSize`, the server shall treat
// `limit` as the total cursor budget across `firstBatch` and `getMore`.
#[tokio::test]
async fn test_find_limit_is_total_across_get_more() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db.collection::<Document>("users");
    coll.insert_many(vec![
        doc! { "name": "alice" },
        doc! { "name": "bob" },
        doc! { "name": "carol" },
    ])
    .await
    .unwrap();

    let response = db
        .run_command(doc! { "find": "users", "batchSize": 1, "limit": 2 })
        .await
        .unwrap();
    let live_cursor_id = cursor_id(&response);
    assert!(live_cursor_id > 0);
    assert_eq!(
        response
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .len(),
        1
    );

    let get_more = db
        .run_command(doc! { "getMore": live_cursor_id, "collection": "users" })
        .await
        .unwrap();
    let cursor = get_more.get_document("cursor").unwrap();

    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 1);
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
}

// EARS: When a materialized command cursor has more rows than fit in the first
// batch, `getMore` shall resume it through the same saved cursor mechanism.
#[tokio::test]
async fn test_list_collections_get_more_uses_saved_materialized_cursor() {
    let server = TestServer::start().await;
    let db = server.database("test");

    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();
    db.run_command(doc! { "createCollection": "posts", "storageColumns": ["title"] })
        .await
        .unwrap();

    let response = db
        .run_command(doc! { "listCollections": 1, "cursor": { "batchSize": 1 } })
        .await
        .unwrap();
    let cursor = response.get_document("cursor").unwrap();
    let live_cursor_id = cursor.get_i64("id").unwrap();

    assert!(live_cursor_id > 0);
    assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 1);

    let get_more = db
        .run_command(doc! { "getMore": live_cursor_id, "collection": "$cmd.listCollections" })
        .await
        .unwrap();
    let next_batch = get_more
        .get_document("cursor")
        .unwrap()
        .get_array("nextBatch")
        .unwrap();

    assert_eq!(next_batch.len(), 1);
}

// EARS: When `killCursors` removes a live cursor, a later `getMore` shall
// fail instead of silently returning an empty batch.
#[tokio::test]
async fn test_kill_cursors_removes_saved_state() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let coll = db.collection::<Document>("users");
    coll.insert_many(vec![doc! { "name": "alice" }, doc! { "name": "bob" }])
        .await
        .unwrap();

    let response = db
        .run_command(doc! { "find": "users", "batchSize": 1 })
        .await
        .unwrap();
    let live_cursor_id = cursor_id(&response);

    db.run_command(doc! { "killCursors": "users", "cursors": [live_cursor_id] })
        .await
        .unwrap();

    let err = db
        .run_command(doc! { "getMore": live_cursor_id, "collection": "users" })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cursor not found"));
}

// EARS: When `awaitData` is set without `tailable`, the server shall reject
// the command instead of pretending the cursor can wait.
#[tokio::test]
async fn test_await_data_requires_tailable() {
    let server = TestServer::start().await;
    let local_db = server.database("local");

    let err = local_db
        .run_command(doc! { "find": "oplog.rs", "awaitData": true })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("awaitData requires tailable=true"));
}

// EARS: When a non-oplog namespace asks for a tailable cursor, the server
// shall reject the command instead of enabling unsupported capped semantics.
#[tokio::test]
async fn test_tailable_is_rejected_for_non_oplog_namespace() {
    let server = TestServer::start().await;
    let db = server.database("test");
    db.run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();

    let err = db
        .run_command(doc! { "find": "users", "tailable": true })
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("tailable and awaitData are only supported on local.oplog.rs"));
}

// EARS: When a tailable awaitData oplog cursor reaches the current end, the
// server shall wake the waiting `getMore` after a later committed write.
#[tokio::test]
async fn test_tailable_oplog_cursor_waits_for_new_write() {
    let server = TestServer::start().await;
    let app_db = server.database("test");
    let local_db = server.database("local");

    app_db
        .run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();
    let coll = app_db.collection::<Document>("users");
    coll.insert_one(doc! { "name": "alice" }).await.unwrap();

    let find_response = local_db
        .run_command(doc! {
            "find": "oplog.rs",
            "filter": { "_id": { "$gt": 2_i64 } },
            "batchSize": 1,
            "tailable": true,
            "awaitData": true,
        })
        .await
        .unwrap();

    assert_eq!(cursor_namespace(&find_response), "local.oplog.rs");
    assert!(cursor_id(&find_response) > 0);
    assert!(find_response
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()
        .is_empty());

    let live_cursor_id = cursor_id(&find_response);
    let local_db_for_get_more = local_db.clone();
    let wait_for_get_more = tokio::spawn(async move {
        local_db_for_get_more
            .run_command(doc! {
                "getMore": live_cursor_id,
                "collection": "oplog.rs",
                "maxTimeMS": 5_000_i64,
            })
            .await
            .unwrap()
    });

    coll.insert_one(doc! { "name": "bob" }).await.unwrap();

    let get_more_response = wait_for_get_more.await.unwrap();
    let cursor = get_more_response.get_document("cursor").unwrap();
    let next_batch = cursor.get_array("nextBatch").unwrap();

    assert_eq!(next_batch.len(), 1);
    assert!(cursor.get_i64("id").unwrap() > 0);
    let oplog_entry = next_batch[0].as_document().unwrap();
    assert_eq!(oplog_entry.get_i64("_id").unwrap(), 3);
    assert_eq!(oplog_entry.get_str("op").unwrap(), "i");
    assert_eq!(oplog_entry.get_str("ns").unwrap(), "test.users");
}

// EARS: When a tailable awaitData oplog cursor times out without a new write,
// `getMore` shall return an empty batch and keep the cursor alive.
#[tokio::test]
async fn test_tailable_oplog_cursor_timeout_keeps_cursor_alive() {
    let server = TestServer::start().await;
    let app_db = server.database("test");
    let local_db = server.database("local");

    app_db
        .run_command(doc! { "createCollection": "users", "storageColumns": ["name"] })
        .await
        .unwrap();
    app_db
        .collection::<Document>("users")
        .insert_one(doc! { "name": "alice" })
        .await
        .unwrap();

    let find_response = local_db
        .run_command(doc! {
            "find": "oplog.rs",
            "filter": { "_id": { "$gt": 1_i64 } },
            "batchSize": 1,
            "tailable": true,
            "awaitData": true,
        })
        .await
        .unwrap();
    let live_cursor_id = cursor_id(&find_response);

    let get_more_response = local_db
        .run_command(doc! {
            "getMore": live_cursor_id,
            "collection": "oplog.rs",
            "maxTimeMS": 25_i64,
        })
        .await
        .unwrap();
    let cursor = get_more_response.get_document("cursor").unwrap();

    assert!(cursor.get_i64("id").unwrap() > 0);
    assert!(cursor.get_array("nextBatch").unwrap().is_empty());
}
