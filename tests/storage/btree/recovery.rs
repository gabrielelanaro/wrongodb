//! Integration tests for WAL recovery

use std::path::PathBuf;
use tempfile::tempdir;
use wrongodb::BTree;

/// Helper to create a test database path
fn test_db_path(name: &str) -> PathBuf {
    let tmp = tempdir().unwrap();
    tmp.path().join(name)
}

#[test]
fn recover_from_wal_after_crash() {
    let db_path = test_db_path("recover_crash.db");

    // Create database with WAL enabled
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        // Insert some records
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sync WAL to ensure all records are on disk
        tree.sync_wal().unwrap();
    }

    // Reopen database - should recover from WAL
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        // Verify all data is present
        let result = tree.get(b"key0").unwrap();
        assert_eq!(result, Some(b"value0".to_vec()));
    }
}

#[test]
fn recover_single_insert_no_split() {
    let db_path = test_db_path("recover_single.db");

    // Create database with WAL enabled
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        // Insert only one record (no split)
        tree.put(b"key0", b"value0").unwrap();
        tree.sync_wal().unwrap();
    }

    // Reopen database - should recover from WAL
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        let result = tree.get(b"key0").unwrap();
        assert_eq!(result, Some(b"value0".to_vec()));
    }
}

#[test]
fn recovery_after_checkpoint() {
    let db_path = test_db_path("recover_checkpoint.db");

    // Create database with WAL and insert initial data
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Checkpoint to flush to disk
        tree.checkpoint().unwrap();
    }

    // Add more data after checkpoint
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        for i in 5..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sync WAL to ensure records are durable before "crash"
        tree.sync_wal().unwrap();

        // Simulate crash: close without checkpointing
    }

    // Reopen - should have checkpointed data + WAL-recovered data
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        // Verify all data is present (0-4 from checkpoint, 5-9 from WAL)
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }
}

#[test]
fn recovery_idempotent() {
    let db_path = test_db_path("recover_idempotent.db");

    // Create database with data
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sync WAL to ensure records are durable
        tree.sync_wal().unwrap();
    }

    // First recovery
    {
        let mut tree = BTree::open(&db_path, true).unwrap();
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }

    // Second recovery (should be idempotent - same data)
    {
        let mut tree = BTree::open(&db_path, true).unwrap();
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }
}

#[test]
fn recovery_with_multiple_splits() {
    let db_path = test_db_path("recover_splits.db");

    // Create database with WAL and insert enough data to cause splits
    {
        let mut tree = BTree::create(&db_path, 256, true).unwrap();  // Small page size to force splits

        // Insert many records to trigger multiple leaf splits
        for i in 0..50 {
            let key = format!("key{:010}", i);  // Padded for sorting
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sync WAL to ensure records are durable
        tree.sync_wal().unwrap();

        // Simulate crash
    }

    // Recover and verify all data
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        for i in 0..50 {
            let key = format!("key{:010}", i);
            let value = format!("value{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }

        // Verify range scan works
        let mut iter = tree.range(Some(b"key0000000000"), Some(b"key0000000100")).unwrap();
        let mut count = 0;
        while let Some(Ok((k, v))) = iter.next() {
            assert!(k.starts_with(b"key"));
            assert!(v.starts_with(b"value"));
            count += 1;
        }
        assert_eq!(count, 50);
    }
}

#[test]
fn recovery_with_corrupted_wal() {
    let db_path = test_db_path("recover_corrupt.db");
    let wal_path = db_path.with_extension("db.wal");

    // Create database with WAL
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Ensure WAL is durable before corruption
        tree.sync_wal().unwrap();
    }

    // Corrupt the WAL file
    {
        use std::fs::OpenOptions;
        use std::io::Write;

        let mut file = OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();

        // Overwrite some bytes in the WAL
        use std::io::Seek;
        use std::io::SeekFrom;
        file.seek(SeekFrom::Start(600)).unwrap();
        file.write_all(b"CORRUPT_DATA_HERE").unwrap();
    }

    // Reopen - should handle corrupted WAL gracefully
    {
        // This should not fail - corrupted WAL should be handled gracefully
        let result = BTree::open(&db_path, true);

        // Either it opens successfully (with warnings) or fails gracefully
        // We don't assert success/failure, just that it doesn't panic
        match result {
            Ok(mut tree) => {
                // If it opened, try to read some data
                // (Some data may be lost due to corruption, which is acceptable)
                let _ = tree.get(b"key0");
            }
            Err(_) => {
                // Failed to open - also acceptable for corrupted WAL
            }
        }
    }
}

#[test]
fn recovery_empty_database() {
    let db_path = test_db_path("recover_empty.db");

    // Create empty database
    {
        let _tree = BTree::create(&db_path, 512, true).unwrap();
    }

    // Reopen empty database
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        // Should be able to query and get None
        let result = tree.get(b"nonexistent").unwrap();
        assert_eq!(result, None);
    }
}

#[test]
fn recovery_large_keys_and_values() {
    let db_path = test_db_path("recover_large.db");

    // Create database with large keys and values
    {
        let mut tree = BTree::create(&db_path, 1024, true).unwrap();

        for i in 0..10 {
            let key = format!("key_{}_with_lots_of_padding_data_{}", i, i);
            let value = format!("value_{}_with_even_more_padding_data_{}", i, i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sync WAL to ensure records are on disk
        tree.sync_wal().unwrap();
    }

    // Recover and verify
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        for i in 0..10 {
            let key = format!("key_{}_with_lots_of_padding_data_{}", i, i);
            let value = format!("value_{}_with_even_more_padding_data_{}", i, i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }
}

#[test]
fn recovery_with_wal_disabled() {
    let db_path = test_db_path("recover_no_wal.db");

    // Create database WITHOUT WAL
    {
        let mut tree = BTree::create(&db_path, 512, false).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Without WAL, durability requires checkpoint
        tree.checkpoint().unwrap();
    }

    // Reopen - should work normally without WAL
    {
        let mut tree = BTree::open(&db_path, false).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }
}
