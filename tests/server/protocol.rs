use futures_util::stream::{StreamExt, TryStreamExt};
use mongodb::{bson::doc, options::ClientOptions, Client};
use std::sync::Arc;
use tempfile::tempdir;
use wrongodb::{start_server, Connection, ConnectionConfig};

#[tokio::test]
async fn test_mongo_client_connection() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("test_server.db");
    let config = ConnectionConfig::default();
    let conn = Connection::open(&db_path, config.clone()).unwrap();
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

    let db_client = client.database("test");
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

    server.abort();
}

#[tokio::test]
async fn test_supported_mongosh_commands() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("test_shell.db");
    let config = ConnectionConfig::default();
    let conn = Connection::open(&db_path, config.clone()).unwrap();
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
    let admin_db = client.database("admin");
    let db_client = client.database("test");
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

    server.abort();
}
