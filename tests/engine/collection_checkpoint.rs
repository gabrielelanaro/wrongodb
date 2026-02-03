use serde_json::json;
use tempfile::tempdir;

use wrongodb::{WrongoDB, WrongoDBConfig};

#[test]
fn explicit_checkpoint_persists_without_wal() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("collection_checkpoint.db");
    let config = WrongoDBConfig::new()
        .wal_enabled(false)
        .disable_auto_checkpoint();

    // First run: write without checkpoint
    {
        let db = WrongoDB::open_with_config(&path, config.clone()).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        coll.insert_one(&mut session, json!({"_id": 1, "v": "a"}))
            .unwrap();
        coll.insert_one(&mut session, json!({"_id": 2, "v": "b"}))
            .unwrap();
    }

    // Reopen: nothing should be durable yet
    {
        let db = WrongoDB::open_with_config(&path, config.clone()).unwrap();
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

        coll.insert_one(&mut session, json!({"_id": 3, "v": "c"}))
            .unwrap();
        coll.insert_one(&mut session, json!({"_id": 4, "v": "d"}))
            .unwrap();
        coll.checkpoint(&mut session).unwrap();
    }

    // Reopen: checkpoint should have made the last batch durable
    {
        let db = WrongoDB::open_with_config(&path, config).unwrap();
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
        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 3})))
            .unwrap()
            .is_some());
        assert!(coll
            .find_one(&mut session, Some(json!({"_id": 4})))
            .unwrap()
            .is_some());
    }
}
