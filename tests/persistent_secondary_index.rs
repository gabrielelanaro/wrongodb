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

use wrongodb::WrongoDB;

#[test]
fn index_created_on_empty_collection() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path, ["username"], false).unwrap();

    // Insert documents
    db.insert_one(json!({"username": "alice", "age": 30})).unwrap();
    db.insert_one(json!({"username": "bob", "age": 25})).unwrap();
    db.insert_one(json!({"username": "alice", "age": 35})).unwrap(); // Duplicate username

    // Query by indexed field
    let alices = db.find(Some(json!({"username": "alice"}))).unwrap();
    assert_eq!(alices.len(), 2, "Expected 2 alices");

    let bobs = db.find(Some(json!({"username": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1, "Expected 1 bob");

    // Checkpoint to ensure persistence
    db.checkpoint().unwrap();

    // Verify index file exists
    let index_path = tmp.path().join("test.db.username.idx.wt");
    assert!(index_path.exists(), "Index file should exist after checkpoint");
}

#[test]
fn index_survives_database_restart() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create database and insert documents
    {
        let mut db = WrongoDB::open(&path, ["email"], false).unwrap();
        db.insert_one(json!({"email": "alice@example.com", "name": "Alice"}))
            .unwrap();
        db.insert_one(json!({"email": "bob@example.com", "name": "Bob"}))
            .unwrap();
        db.checkpoint().unwrap();
    }

    // Reopen database
    {
        let mut db = WrongoDB::open(&path, ["email"], false).unwrap();

        // Query should work immediately without rebuilding
        let results = db.find(Some(json!({"email": "alice@example.com"}))).unwrap();
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
        let mut db = WrongoDB::open(&path, [] as [&str; 0], false).unwrap();
        db.insert_one(json!({"city": "nyc", "name": "alice"}))
            .unwrap();
        db.insert_one(json!({"city": "la", "name": "bob"})).unwrap();
        db.insert_one(json!({"city": "nyc", "name": "charlie"}))
            .unwrap();
        db.checkpoint().unwrap();
    }

    // Now open with city index - should build from existing documents
    {
        let mut db = WrongoDB::open(&path, ["city"], false).unwrap();
        let nyc_docs = db.find(Some(json!({"city": "nyc"}))).unwrap();
        assert_eq!(nyc_docs.len(), 2, "Expected 2 NYC docs");
    }
}

#[test]
fn index_maintenance_on_update() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path, ["status"], false).unwrap();

    db.insert_one(json!({"name": "alice", "status": "active"}))
        .unwrap();
    db.insert_one(json!({"name": "bob", "status": "inactive"}))
        .unwrap();

    // Verify initial state
    let active = db.find(Some(json!({"status": "active"}))).unwrap();
    assert_eq!(active.len(), 1);

    // Update bob to active
    db.update_one_in(
        "test",
        Some(json!({"name": "bob"})),
        json!({"$set": {"status": "active"}}),
    )
    .unwrap();

    let active = db.find(Some(json!({"status": "active"}))).unwrap();
    assert_eq!(active.len(), 2, "Expected 2 active users after update");

    let inactive = db.find(Some(json!({"status": "inactive"}))).unwrap();
    assert_eq!(inactive.len(), 0, "Expected 0 inactive users after update");
}

#[test]
fn index_maintenance_on_delete() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path, ["category"], false).unwrap();

    db.insert_one(json!({"name": "item1", "category": "electronics"}))
        .unwrap();
    db.insert_one(json!({"name": "item2", "category": "electronics"}))
        .unwrap();
    db.insert_one(json!({"name": "item3", "category": "clothing"}))
        .unwrap();

    // Verify initial state
    let electronics = db.find(Some(json!({"category": "electronics"}))).unwrap();
    assert_eq!(electronics.len(), 2);

    // Delete one electronics item
    db.delete_one_in("test", Some(json!({"name": "item1"}))).unwrap();

    let electronics = db.find(Some(json!({"category": "electronics"}))).unwrap();
    assert_eq!(electronics.len(), 1, "Expected 1 electronics item after delete");
}

#[test]
fn multiple_indexes_on_same_collection() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path, ["dept", "role"], false).unwrap();

    db.insert_one(json!({"name": "alice", "dept": "eng", "role": "dev"}))
        .unwrap();
    db.insert_one(json!({"name": "bob", "dept": "eng", "role": "manager"}))
        .unwrap();
    db.insert_one(json!({"name": "charlie", "dept": "sales", "role": "manager"}))
        .unwrap();

    // Query by dept
    let eng = db.find(Some(json!({"dept": "eng"}))).unwrap();
    assert_eq!(eng.len(), 2);

    // Query by role
    let managers = db.find(Some(json!({"role": "manager"}))).unwrap();
    assert_eq!(managers.len(), 2);

    // Checkpoint and verify both index files exist
    db.checkpoint().unwrap();

    let dept_idx = tmp.path().join("test.db.dept.idx.wt");
    let role_idx = tmp.path().join("test.db.role.idx.wt");
    assert!(dept_idx.exists(), "Dept index file should exist");
    assert!(role_idx.exists(), "Role index file should exist");
}

#[test]
fn index_persistence_with_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create and populate database
    {
        let mut db = WrongoDB::open(&path, ["priority"], false).unwrap();
        db.insert_one(json!({"task": "task1", "priority": 1}))
            .unwrap();
        db.insert_one(json!({"task": "task2", "priority": 2}))
            .unwrap();
        db.checkpoint().unwrap();
    }

    // Verify index file has content
    let index_path = tmp.path().join("test.db.priority.idx.wt");
    let metadata = fs::metadata(&index_path).unwrap();
    assert!(metadata.len() > 0, "Index file should have content");

    // Reopen and query
    {
        let mut db = WrongoDB::open(&path, ["priority"], false).unwrap();
        let high_priority = db.find(Some(json!({"priority": 2}))).unwrap();
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
    let mut db = WrongoDB::open(&path, [] as [&str; 0], false).unwrap();
    db.insert_one(json!({"name": "alice", "country": "usa"}))
        .unwrap();
    db.insert_one(json!({"name": "bob", "country": "uk"}))
        .unwrap();

    // Create index dynamically
    db.create_index("test", "country").unwrap();

    // Query using new index
    let usa_docs = db.find(Some(json!({"country": "usa"}))).unwrap();
    assert_eq!(usa_docs.len(), 1);

    // Verify index file was created
    db.checkpoint().unwrap();
    let index_path = tmp.path().join("test.db.country.idx.wt");
    assert!(index_path.exists(), "Dynamically created index file should exist");
}
