//! Integration tests for connection-level global WAL recovery.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use serde_json::json;
use tempfile::tempdir;

use wrongodb::{WrongoDB, WrongoDBConfig};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

#[test]
fn recovery_replays_committed_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        let mut txn = session.transaction().unwrap();
        coll.insert_one(txn.session_mut(), json!({"_id": 1, "v": "a"}))
            .unwrap();
        coll.insert_one(txn.session_mut(), json!({"_id": 2, "v": "b"}))
            .unwrap();
        txn.commit().unwrap();
        // Simulate crash: no explicit checkpoint.
    }

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 1})))
            .unwrap()
            .is_some());
        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 2})))
            .unwrap()
            .is_some());
    }
}

#[test]
fn recovery_replays_records_appended_after_restart() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
            .unwrap();
    }

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        coll.insert_one(&mut session, json!({"_id": 2, "v": "b"}))
            .unwrap();
    }

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 1})))
            .unwrap()
            .is_some());
        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 2})))
            .unwrap()
            .is_some());
    }
}

#[test]
fn recovery_skips_uncommitted_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        let mut txn = session.transaction().unwrap();
        coll.insert_one(txn.session_mut(), json!({"_id": 1, "v": "a"}))
            .unwrap();
        coll.insert_one(txn.session_mut(), json!({"_id": 2, "v": "b"}))
            .unwrap();

        // Simulate crash before commit marker by skipping Drop on SessionTxn.
        std::mem::forget(txn);
    }

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 1})))
            .unwrap()
            .is_none());
        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 2})))
            .unwrap()
            .is_none());
    }
}

#[test]
fn recovery_handles_corrupted_global_wal_tail() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
            .unwrap();
    }

    let wal_path = global_wal_path(&db_path);
    {
        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(600)).unwrap();
        file.write_all(b"CORRUPTED_TAIL").unwrap();
    }

    // Should not panic; either open succeeds with partial recovery or fails gracefully.
    let _ = WrongoDB::open(&db_path);
}

#[test]
fn wal_disabled_still_requires_checkpoint_for_durability() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = WrongoDBConfig::new().wal_enabled(false);

    {
        let db = WrongoDB::open_with_config(&db_path, cfg.clone()).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
            .unwrap();
        coll.checkpoint(&mut session).unwrap();
    }

    {
        let db = WrongoDB::open_with_config(&db_path, cfg).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 1})))
            .unwrap()
            .is_some());
    }
}
