//! Integration tests for connection-level global WAL behavior.

use std::fs;

use serde_json::json;
use tempfile::tempdir;

use wrongodb::{WrongoDB, WrongoDBConfig};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

#[test]
fn global_wal_created_when_enabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let db = WrongoDB::open(&db_path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();
    coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
        .unwrap();

    assert!(global_wal_path(&db_path).exists());
}

#[test]
fn global_wal_not_created_when_disabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = WrongoDBConfig::new().wal_enabled(false);

    let db = WrongoDB::open_with_config(&db_path, cfg).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();
    coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
        .unwrap();

    assert!(!global_wal_path(&db_path).exists());
}

#[test]
fn global_wal_grows_after_committed_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let db = WrongoDB::open(&db_path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    for i in 0..20 {
        coll.insert_one(&mut session, json!({"_id": i, "v": i}))
            .unwrap();
    }

    let metadata = fs::metadata(global_wal_path(&db_path)).unwrap();
    assert!(metadata.len() > 512);
}

#[test]
fn collection_checkpoint_truncates_global_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let db = WrongoDB::open(&db_path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    for i in 0..10 {
        coll.insert_one(&mut session, json!({"_id": i, "v": i}))
            .unwrap();
    }

    let wal_path = global_wal_path(&db_path);
    let before = fs::metadata(&wal_path).unwrap().len();
    assert!(before > 512);

    coll.checkpoint(&mut session).unwrap();

    let after = fs::metadata(&wal_path).unwrap().len();
    assert!(after <= 512);
}

#[test]
fn checkpoint_does_not_truncate_wal_while_transaction_is_active() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let db = WrongoDB::open(&db_path).unwrap();
    let coll = db.collection("test");
    let mut writer = db.open_session();
    let mut checkpointer = db.open_session();

    coll.insert_one(&mut checkpointer, json!({"_id": 1, "v": "seed"}))
        .unwrap();

    let wal_path = global_wal_path(&db_path);
    let before = fs::metadata(&wal_path).unwrap().len();
    assert!(before > 512);

    let mut txn = writer.transaction().unwrap();
    coll.insert_one(txn.session_mut(), json!({"_id": 2, "v": "pending"}))
        .unwrap();

    coll.checkpoint(&mut checkpointer).unwrap();
    let after_skip = fs::metadata(&wal_path).unwrap().len();
    assert!(after_skip >= before);

    txn.commit().unwrap();
    coll.checkpoint(&mut checkpointer).unwrap();

    let after_truncate = fs::metadata(&wal_path).unwrap().len();
    assert!(after_truncate <= 512);
}
