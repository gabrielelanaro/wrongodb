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

    let mut db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test").unwrap();

        // Create index
        coll.create_index("username").unwrap();

        // Insert documents
        coll.insert_one(json!({"username": "alice", "age": 30}))
            .unwrap();
        coll.insert_one(json!({"username": "bob", "age": 25})).unwrap();
        coll.insert_one(json!({"username": "alice", "age": 35}))
            .unwrap(); // Duplicate username

        // Query by indexed field
        let alices = coll.find(Some(json!({"username": "alice"}))).unwrap();
        assert_eq!(alices.len(), 2, "Expected 2 alices");

        let bobs = coll.find(Some(json!({"username": "bob"}))).unwrap();
        assert_eq!(bobs.len(), 1, "Expected 1 bob");

        coll.checkpoint().unwrap();
    }

    // Verify index file exists (collection "test" creates files with .test extension)
    let index_path = tmp.path().join("test.db.test.username.idx.wt");
    assert!(index_path.exists(), "Index file should exist after checkpoint");
}

#[test]
fn index_survives_database_restart() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create database and insert documents
    {
        let mut db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test").unwrap();
            coll.create_index("email").unwrap();
            coll.insert_one(json!({"email": "alice@example.com", "name": "Alice"}))
                .unwrap();
            coll.insert_one(json!({"email": "bob@example.com", "name": "Bob"}))
                .unwrap();
            coll.checkpoint().unwrap();
        }
    }

    // Reopen database
    {
        let mut db = WrongoDB::open(&path).unwrap();

        // Query should work immediately without rebuilding
        let coll = db.collection("test").unwrap();
        let results = coll
            .find(Some(json!({"email": "alice@example.com"})))
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
        let mut db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test").unwrap();
            coll.insert_one(json!({"city": "nyc", "name": "alice"}))
                .unwrap();
            coll.insert_one(json!({"city": "la", "name": "bob"})).unwrap();
            coll.insert_one(json!({"city": "nyc", "name": "charlie"}))
                .unwrap();
            coll.checkpoint().unwrap();
        }
    }

    // Now create city index and query
    {
        let mut db = WrongoDB::open(&path).unwrap();
        let coll = db.collection("test").unwrap();
        coll.create_index("city").unwrap();
        let nyc_docs = coll.find(Some(json!({"city": "nyc"}))).unwrap();
        assert_eq!(nyc_docs.len(), 2, "Expected 2 NYC docs");
    }
}

#[test]
fn index_maintenance_on_update() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path).unwrap();

    let coll = db.collection("test").unwrap();
    coll.create_index("status").unwrap();
    coll.insert_one(json!({"name": "alice", "status": "active"}))
        .unwrap();
    coll.insert_one(json!({"name": "bob", "status": "inactive"}))
        .unwrap();

    // Verify initial state
    let active = coll.find(Some(json!({"status": "active"}))).unwrap();
    assert_eq!(active.len(), 1);

    // Update bob to active
    coll.update_one(
        Some(json!({"name": "bob"})),
        json!({"$set": {"status": "active"}}),
    )
    .unwrap();

    let active = coll.find(Some(json!({"status": "active"}))).unwrap();
    assert_eq!(active.len(), 2, "Expected 2 active users after update");

    let inactive = coll.find(Some(json!({"status": "inactive"}))).unwrap();
    assert_eq!(inactive.len(), 0, "Expected 0 inactive users after update");
}

#[test]
fn index_maintenance_on_delete() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path).unwrap();

    let coll = db.collection("test").unwrap();
    coll.create_index("category").unwrap();
    coll.insert_one(json!({"name": "item1", "category": "electronics"}))
        .unwrap();
    coll.insert_one(json!({"name": "item2", "category": "electronics"}))
        .unwrap();
    coll.insert_one(json!({"name": "item3", "category": "clothing"}))
        .unwrap();

    // Verify initial state
    let electronics = coll
        .find(Some(json!({"category": "electronics"})))
        .unwrap();
    assert_eq!(electronics.len(), 2);

    // Delete one electronics item
    coll.delete_one(Some(json!({"name": "item1"}))).unwrap();

    let electronics = coll
        .find(Some(json!({"category": "electronics"})))
        .unwrap();
    assert_eq!(electronics.len(), 1, "Expected 1 electronics item after delete");
}

#[test]
fn multiple_indexes_on_same_collection() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    let mut db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test").unwrap();
        coll.create_index("dept").unwrap();
        coll.create_index("role").unwrap();
        coll.insert_one(json!({"name": "alice", "dept": "eng", "role": "dev"}))
            .unwrap();
        coll.insert_one(json!({"name": "bob", "dept": "eng", "role": "manager"}))
            .unwrap();
        coll.insert_one(json!({"name": "charlie", "dept": "sales", "role": "manager"}))
            .unwrap();

        // Query by dept
        let eng = coll.find(Some(json!({"dept": "eng"}))).unwrap();
        assert_eq!(eng.len(), 2);

        // Query by role
        let managers = coll.find(Some(json!({"role": "manager"}))).unwrap();
        assert_eq!(managers.len(), 2);

        coll.checkpoint().unwrap();
    }

    let dept_idx = tmp.path().join("test.db.test.dept.idx.wt");
    let role_idx = tmp.path().join("test.db.test.role.idx.wt");
    assert!(dept_idx.exists(), "Dept index file should exist");
    assert!(role_idx.exists(), "Role index file should exist");
}

#[test]
fn index_persistence_with_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.db");

    // Create and populate database
    {
        let mut db = WrongoDB::open(&path).unwrap();
        {
            let coll = db.collection("test").unwrap();
            coll.create_index("priority").unwrap();
            coll.insert_one(json!({"task": "task1", "priority": 1}))
                .unwrap();
            coll.insert_one(json!({"task": "task2", "priority": 2}))
                .unwrap();
            coll.checkpoint().unwrap();
        }
    }

    // Verify index file has content (collection "test" creates files with .test extension)
    let index_path = tmp.path().join("test.db.test.priority.idx.wt");
    let metadata = fs::metadata(&index_path).unwrap();
    assert!(metadata.len() > 0, "Index file should have content");

    // Reopen and query
    {
        let mut db = WrongoDB::open(&path).unwrap();
        let coll = db.collection("test").unwrap();
        let high_priority = coll.find(Some(json!({"priority": 2}))).unwrap();
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
    let mut db = WrongoDB::open(&path).unwrap();
    {
        let coll = db.collection("test").unwrap();
        coll.insert_one(json!({"name": "alice", "country": "usa"}))
            .unwrap();
        coll.insert_one(json!({"name": "bob", "country": "uk"}))
            .unwrap();

        // Create index dynamically
        coll.create_index("country").unwrap();

        // Query using new index
        let usa_docs = coll.find(Some(json!({"country": "usa"}))).unwrap();
        assert_eq!(usa_docs.len(), 1);

        coll.checkpoint().unwrap();
    }

    // Verify index file was created (collection "test" creates files with .test extension)
    let index_path = tmp.path().join("test.db.test.country.idx.wt");
    assert!(index_path.exists(), "Dynamically created index file should exist");
}
