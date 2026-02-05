//! Integration tests for persistent secondary indexes.
//!
//! These tests verify that:
//! 1. Secondary indexes are persisted to disk
//! 2. Indexes survive database restarts
//! 3. Index maintenance works correctly for updates/deletes
//! 4. Multiple indexes can coexist

use serde_json::json;
use std::fs;
use tempfile::tempdir;

use wrongodb::{decode_index_id, encode_range_bounds, WrongoDB};

#[test]
fn index_created_on_empty_collection() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test");
        let mut session = db.open_session();

        // Create index
        coll.create_index(&mut session, "username").unwrap();

        // Insert documents
        coll.insert_one(&mut session, json!({"username": "alice", "age": 30}))
            .unwrap();
        coll.insert_one(&mut session, json!({"username": "bob", "age": 25})).unwrap();
        coll.insert_one(&mut session, json!({"username": "alice", "age": 35}))
            .unwrap(); // Duplicate username

        // Query by indexed field
        let alices = coll
            .find(&mut session, Some(json!({"username": "alice"})))
            .unwrap();
        assert_eq!(alices.len(), 2, "Expected 2 alices");

        let bobs = coll
            .find(&mut session, Some(json!({"username": "bob"})))
            .unwrap();
        assert_eq!(bobs.len(), 1, "Expected 1 bob");

        coll.checkpoint(&mut session).unwrap();
    }

    // Verify index file exists
    let index_path = path.join("test.username.idx.wt");
    assert!(index_path.exists(), "Index file should exist after checkpoint");
}

#[test]
fn index_survives_database_restart() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create database and insert documents
    {
        let db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test");
            let mut session = db.open_session();
            coll.create_index(&mut session, "email").unwrap();
            coll.insert_one(&mut session, json!({"email": "alice@example.com", "name": "Alice"}))
                .unwrap();
            coll.insert_one(&mut session, json!({"email": "bob@example.com", "name": "Bob"}))
                .unwrap();
            coll.checkpoint(&mut session).unwrap();
        }
    }

    // Reopen database
    {
        let db = WrongoDB::open(&path).unwrap();

        // Query should work immediately without rebuilding
        let coll = db.collection("test");
        let mut session = db.open_session();
        let results = coll
            .find(&mut session, Some(json!({"email": "alice@example.com"})))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("name").unwrap().as_str().unwrap(),
            "Alice"
        );
    }
}

#[test]
fn index_builds_from_existing_documents() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create data without index
    {
        let db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test");
            let mut session = db.open_session();
            coll.insert_one(&mut session, json!({"city": "nyc", "name": "alice"}))
                .unwrap();
            coll.insert_one(&mut session, json!({"city": "la", "name": "bob"})).unwrap();
            coll.insert_one(&mut session, json!({"city": "nyc", "name": "charlie"}))
                .unwrap();
            coll.checkpoint(&mut session).unwrap();
        }
    }

    // Now create city index and query
    {
        let db = WrongoDB::open(&path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();
        coll.create_index(&mut session, "city").unwrap();
        let nyc_docs = coll
            .find(&mut session, Some(json!({"city": "nyc"})))
            .unwrap();
        assert_eq!(nyc_docs.len(), 2, "Expected 2 NYC docs");
    }
}

#[test]
fn index_maintenance_on_update() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();

    let coll = db.collection("test");
    let mut session = db.open_session();
    coll.create_index(&mut session, "status").unwrap();
    coll.insert_one(&mut session, json!({"name": "alice", "status": "active"}))
        .unwrap();
    coll.insert_one(&mut session, json!({"name": "bob", "status": "inactive"}))
        .unwrap();

    // Verify initial state
    let active = coll
        .find(&mut session, Some(json!({"status": "active"})))
        .unwrap();
    assert_eq!(active.len(), 1);

    // Update bob to active
    coll.update_one(
        &mut session,
        Some(json!({"name": "bob"})),
        json!({"$set": {"status": "active"}}),
    )
    .unwrap();

    let active = coll
        .find(&mut session, Some(json!({"status": "active"})))
        .unwrap();
    assert_eq!(active.len(), 2, "Expected 2 active users after update");

    let inactive = coll
        .find(&mut session, Some(json!({"status": "inactive"})))
        .unwrap();
    assert_eq!(inactive.len(), 0, "Expected 0 inactive users after update");
}

#[test]
fn index_maintenance_on_delete() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();

    let coll = db.collection("test");
    let mut session = db.open_session();
    coll.create_index(&mut session, "category").unwrap();
    coll.insert_one(&mut session, json!({"name": "item1", "category": "electronics"}))
        .unwrap();
    coll.insert_one(&mut session, json!({"name": "item2", "category": "electronics"}))
        .unwrap();
    coll.insert_one(&mut session, json!({"name": "item3", "category": "clothing"}))
        .unwrap();

    // Verify initial state
    let electronics = coll
        .find(&mut session, Some(json!({"category": "electronics"})))
        .unwrap();
    assert_eq!(electronics.len(), 2);

    // Delete one electronics item
    coll.delete_one(&mut session, Some(json!({"name": "item1"})))
        .unwrap();

    let electronics = coll
        .find(&mut session, Some(json!({"category": "electronics"})))
        .unwrap();
    assert_eq!(electronics.len(), 1, "Expected 1 electronics item after delete");
}

#[test]
fn multiple_indexes_on_same_collection() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test");
        let mut session = db.open_session();
        coll.create_index(&mut session, "dept").unwrap();
        coll.create_index(&mut session, "role").unwrap();
        coll.insert_one(&mut session, json!({"name": "alice", "dept": "eng", "role": "dev"}))
            .unwrap();
        coll.insert_one(&mut session, json!({"name": "bob", "dept": "eng", "role": "manager"}))
            .unwrap();
        coll.insert_one(&mut session, json!({"name": "charlie", "dept": "sales", "role": "manager"}))
            .unwrap();

        // Query by dept
        let eng = coll.find(&mut session, Some(json!({"dept": "eng"}))).unwrap();
        assert_eq!(eng.len(), 2);

        // Query by role
        let managers = coll
            .find(&mut session, Some(json!({"role": "manager"})))
            .unwrap();
        assert_eq!(managers.len(), 2);

        coll.checkpoint(&mut session).unwrap();
    }

    let dept_idx = path.join("test.dept.idx.wt");
    let role_idx = path.join("test.role.idx.wt");
    assert!(dept_idx.exists(), "Dept index file should exist");
    assert!(role_idx.exists(), "Role index file should exist");
}

#[test]
fn index_persistence_with_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create and populate database
    {
        let db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test");
            let mut session = db.open_session();
            coll.create_index(&mut session, "priority").unwrap();
            coll.insert_one(&mut session, json!({"task": "task1", "priority": 1}))
                .unwrap();
            coll.insert_one(&mut session, json!({"task": "task2", "priority": 2}))
                .unwrap();
            coll.checkpoint(&mut session).unwrap();
        }
    }

    // Verify index file has content
    let index_path = path.join("test.priority.idx.wt");
    let metadata = fs::metadata(&index_path).unwrap();
    assert!(metadata.len() > 0, "Index file should have content");

    // Reopen and query
    {
        let db = WrongoDB::open(&path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();
        let high_priority = coll
            .find(&mut session, Some(json!({"priority": 2})))
            .unwrap();
        assert_eq!(high_priority.len(), 1);
        assert_eq!(
            high_priority[0].get("task").unwrap().as_str().unwrap(),
            "task2"
        );
    }
}

#[test]
fn create_index_dynamically() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create data without index
    let db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test");
        let mut session = db.open_session();
        coll.insert_one(&mut session, json!({"name": "alice", "country": "usa"}))
            .unwrap();
        coll.insert_one(&mut session, json!({"name": "bob", "country": "uk"}))
            .unwrap();

        // Create index dynamically
        coll.create_index(&mut session, "country").unwrap();

        // Query using new index
        let usa_docs = coll
            .find(&mut session, Some(json!({"country": "usa"})))
            .unwrap();
        assert_eq!(usa_docs.len(), 1);

        coll.checkpoint(&mut session).unwrap();
    }

    // Verify index file was created
    let index_path = path.join("test.country.idx.wt");
    assert!(index_path.exists(), "Dynamically created index file should exist");
}

#[test]
fn index_cursor_range_scan_returns_ids() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    coll.create_index(&mut session, "name").unwrap();
    let doc1 = coll
        .insert_one(&mut session, json!({"name": "alice"}))
        .unwrap();
    let doc2 = coll
        .insert_one(&mut session, json!({"name": "alice"}))
        .unwrap();
    let _doc3 = coll
        .insert_one(&mut session, json!({"name": "bob"}))
        .unwrap();

    let (start, end) = encode_range_bounds(&json!("alice")).unwrap();
    let mut cursor = session.open_cursor("index:test:name").unwrap();
    cursor.set_range(Some(start), Some(end));

    let txn = session.transaction().unwrap();

    let mut ids = Vec::new();
    while let Some((key, _)) = cursor.next(Some(txn.as_ref())).unwrap() {
        if let Some(id) = decode_index_id(&key).unwrap() {
            ids.push(id);
        }
    }

    txn.commit().unwrap();

    assert_eq!(ids.len(), 2);
    assert!(ids.contains(doc1.get("_id").unwrap()));
    assert!(ids.contains(doc2.get("_id").unwrap()));
}

#[test]
fn index_cursor_is_read_only() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    coll.create_index(&mut session, "name").unwrap();
    coll.insert_one(&mut session, json!({"name": "alice"}))
        .unwrap();

    let mut cursor = session.open_cursor("index:test:name").unwrap();
    let txn = session.transaction().unwrap();

    let err = cursor.insert(b"bad", b"write", txn.as_ref()).unwrap_err();
    assert!(err.to_string().contains("read-only"));

    txn.abort().unwrap();
}

#[test]
fn catalog_rebuilds_from_index_files() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    {
        let db = WrongoDB::open(&path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();
        coll.create_index(&mut session, "name").unwrap();
        coll.insert_one(&mut session, json!({"name": "alice"}))
            .unwrap();
        coll.checkpoint(&mut session).unwrap();

        let meta_path = path.join("test.meta.json");
        assert!(meta_path.exists());
        std::fs::remove_file(&meta_path).unwrap();
    }

    let db2 = WrongoDB::open(&path).unwrap();
    let coll2 = db2.collection("test");
    let mut session2 = db2.open_session();
    let indexes = coll2.list_indexes(&mut session2).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].field, "name");
}
