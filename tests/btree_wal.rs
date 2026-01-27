//! WAL integration tests for BTree operations.

use tempfile::tempdir;
use wrongodb::BTree;

#[test]
fn btree_creates_with_wal_enabled() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let tree = BTree::create(&path, 512, true).unwrap();

    // Verify WAL file exists
    let wal_path = path.with_extension("wt.wal");
    assert!(wal_path.exists());
}

#[test]
fn btree_creates_without_wal() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let tree = BTree::create(&path, 512, false).unwrap();

    // Verify WAL file does NOT exist
    let wal_path = path.with_extension("wt.wal");
    assert!(!wal_path.exists());
}

#[test]
fn leaf_insert_logs_wal_record() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut tree = BTree::create(&path, 512, true).unwrap();
    tree.put(b"key1", b"value1").unwrap();
    // Sync WAL to flush the buffer
    tree.sync_wal().unwrap();

    // Verify WAL file has content beyond header
    let wal_path = path.with_extension("wt.wal");
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > 512);  // More than header size
}

#[test]
fn leaf_split_logs_wal_record() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test_split.wt");

    // Use small page size to force splits
    let mut tree = BTree::create(&path, 256, true).unwrap();

    // Insert many keys to cause split
    for i in 0..20 {
        let key = format!("key{:05}", i);
        let value = vec![i as u8; 100];
        tree.put(key.as_bytes(), &value).unwrap();
    }
    // Sync WAL to flush the buffer
    tree.sync_wal().unwrap();

    // Verify WAL file grew
    let wal_path = path.with_extension("wt.wal");
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > 2000);  // Should have multiple records
}

#[test]
fn batch_sync_threshold() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test_batch.wt");

    let mut tree = BTree::create(&path, 512, true).unwrap();
    tree.set_wal_sync_threshold(5);  // Sync every 5 operations

    // Insert 10 keys (should trigger 2 syncs)
    for i in 0..10 {
        let key = format!("key{:02}", i);
        tree.put(key.as_bytes(), b"value").unwrap();
    }

    // Force explicit sync
    tree.sync_wal().unwrap();

    // Verify WAL exists
    let wal_path = path.with_extension("wt.wal");
    assert!(wal_path.exists());
}

#[test]
fn checkpoint_logs_wal_record() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test_ckpt.wt");

    let mut tree = BTree::create(&path, 512, true).unwrap();
    tree.put(b"key", b"value").unwrap();
    tree.checkpoint().unwrap();

    // Verify WAL has checkpoint record
    let wal_path = path.with_extension("wt.wal");
    assert!(wal_path.exists());

    // TODO: In Phase 4, verify checkpoint record exists by reading WAL
}

#[test]
fn wal_disabled_no_file_created() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test_no_wal.wt");

    let mut tree = BTree::create(&path, 512, false).unwrap();
    tree.put(b"key", b"value").unwrap();
    tree.checkpoint().unwrap();

    // Verify WAL file does NOT exist
    let wal_path = path.with_extension("wt.wal");
    assert!(!wal_path.exists());
}

#[test]
fn open_with_wal_reopens_existing_wal() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test_reopen.wt");

    // Create with WAL
    {
        let mut tree = BTree::create(&path, 512, true).unwrap();
        tree.put(b"key1", b"value1").unwrap();
        // Checkpoint to persist data
        tree.checkpoint().unwrap();
    }

    // Verify WAL exists
    let wal_path = path.with_extension("wt.wal");
    assert!(wal_path.exists());

    // Reopen with WAL enabled
    {
        let mut tree = BTree::open(&path, true).unwrap();
        // Verify data is accessible
        let value = tree.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    // WAL should still exist
    assert!(wal_path.exists());
}
