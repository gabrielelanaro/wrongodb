use serde_json::json;
use tempfile::tempdir;

use wrongodb::{WrongoDB, WrongoDBConfig};

#[test]
fn auto_checkpoint_after_n_collection_updates() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("collection_auto_checkpoint.db");
    // Configure auto-checkpoint in the config, not at runtime
    let config = WrongoDBConfig::new()
        .wal_enabled(false)
        .checkpoint_after_updates(3);

    // First run: stop before threshold
    {
        let mut db = WrongoDB::open_with_config(&path, config.clone()).unwrap();
        let coll = db.collection("test").unwrap();

        coll.insert_one(json!({"_id": 1, "v": "a"})).unwrap();
        coll.insert_one(json!({"_id": 2, "v": "b"})).unwrap();
    }

    // Reopen: nothing should be durable yet
    {
        let mut db = WrongoDB::open_with_config(&path, config.clone()).unwrap();
        let coll = db.collection("test").unwrap();
        assert!(coll.find_one(Some(json!({"_id": 1}))).unwrap().is_none());
        assert!(coll.find_one(Some(json!({"_id": 2}))).unwrap().is_none());

        coll.insert_one(json!({"_id": 3, "v": "c"})).unwrap();
        coll.insert_one(json!({"_id": 4, "v": "d"})).unwrap();
        coll.insert_one(json!({"_id": 5, "v": "e"})).unwrap();
    }

    // Reopen: checkpoint should have made the last batch durable
    {
        let mut db = WrongoDB::open_with_config(&path, config).unwrap();
        let coll = db.collection("test").unwrap();
        assert!(coll.find_one(Some(json!({"_id": 1}))).unwrap().is_none());
        assert!(coll.find_one(Some(json!({"_id": 2}))).unwrap().is_none());
        assert!(coll.find_one(Some(json!({"_id": 3}))).unwrap().is_some());
        assert!(coll.find_one(Some(json!({"_id": 4}))).unwrap().is_some());
        assert!(coll.find_one(Some(json!({"_id": 5}))).unwrap().is_some());
    }
}
