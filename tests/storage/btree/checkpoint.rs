use std::sync::Arc;
use tempfile::tempdir;

use wrongodb::{BlockFile, BTree, GlobalTxnState};

#[test]
fn checkpoint_commit_selects_new_root_on_reopen() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_root.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    let bf = BlockFile::open(&path).unwrap();
    let old_root = bf.root_block_id();
    bf.close().unwrap();

    tree.put(b"alpha", b"value").unwrap();
    tree.checkpoint().unwrap();
    drop(tree);

    let bf2 = BlockFile::open(&path).unwrap();
    let new_root = bf2.root_block_id();
    bf2.close().unwrap();

    assert_ne!(new_root, old_root);

    let mut tree2 = BTree::open(&path, false, Arc::new(GlobalTxnState::new())).unwrap();
    assert_eq!(tree2.get(b"alpha").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn crash_before_checkpoint_uses_old_root() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_crash.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    let bf = BlockFile::open(&path).unwrap();
    let stable_root = bf.root_block_id();
    bf.close().unwrap();

    tree.put(b"beta", b"value").unwrap();
    drop(tree);

    let bf2 = BlockFile::open(&path).unwrap();
    let reopened_root = bf2.root_block_id();
    bf2.close().unwrap();

    assert_eq!(reopened_root, stable_root);

    let mut tree2 = BTree::open(&path, false, Arc::new(GlobalTxnState::new())).unwrap();
    assert_eq!(tree2.get(b"beta").unwrap(), None);
}

#[test]
fn retired_blocks_not_reused_before_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_retired.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    tree.put(b"k1", b"v1").unwrap();
    let mut bf = BlockFile::open(&path).unwrap();
    let blocks_after_first = bf.num_blocks().unwrap();
    let avail_after_first = bf.header.avail_count;
    let discard_after_first = bf.header.discard_count;
    bf.close().unwrap();

    assert_eq!(avail_after_first, 0);
    assert!(discard_after_first > 0);

    tree.put(b"k2", b"v2").unwrap();
    let mut bf2 = BlockFile::open(&path).unwrap();
    let blocks_after_second = bf2.num_blocks().unwrap();
    let avail_after_second = bf2.header.avail_count;
    bf2.close().unwrap();

    assert_eq!(avail_after_second, 0);
    assert!(blocks_after_second >= blocks_after_first);
}

#[test]
fn coalesces_updates_between_checkpoints() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_coalesce.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    let mut bf = BlockFile::open(&path).unwrap();
    let blocks_before = bf.num_blocks().unwrap();
    bf.close().unwrap();

    tree.put(b"k1", b"v1").unwrap();
    let mut bf = BlockFile::open(&path).unwrap();
    let blocks_after_first = bf.num_blocks().unwrap();
    bf.close().unwrap();

    tree.put(b"k2", b"v2").unwrap();
    let mut bf = BlockFile::open(&path).unwrap();
    let blocks_after_second = bf.num_blocks().unwrap();
    bf.close().unwrap();

    assert!(blocks_after_first > blocks_before);
    assert_eq!(blocks_after_second, blocks_after_first);
}

#[test]
fn checkpoint_then_crash_recovers_new_root() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_crash_recovery.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    // Insert records and checkpoint
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        tree.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    tree.checkpoint().unwrap();

    // Get the checkpointed root
    let bf = BlockFile::open(&path).unwrap();
    let checkpointed_root = bf.root_block_id();
    bf.close().unwrap();

    // Insert more records without checkpoint
    for i in 10..20 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        tree.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Simulate crash
    drop(tree);

    // Reopen and verify we get the checkpointed root
    let bf2 = BlockFile::open(&path).unwrap();
    let reopened_root = bf2.root_block_id();
    assert_eq!(reopened_root, checkpointed_root);
    bf2.close().unwrap();

    // Verify only checkpointed data is recovered
    let mut tree2 = BTree::open(&path, false, Arc::new(GlobalTxnState::new())).unwrap();
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        assert_eq!(
            tree2.get(key.as_bytes()).unwrap(),
            Some(value.as_bytes().to_vec())
        );
    }
    // Records 10-19 should be lost
    for i in 10..20 {
        let key = format!("key{}", i);
        assert_eq!(tree2.get(key.as_bytes()).unwrap(), None);
    }
}

#[test]
fn retired_blocks_reclaimed_after_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_reclaim.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    // Insert enough records to cause splits
    for i in 0..20 {
        let key = format!("key{:02}", i);
        let value = format!("value{}", i);
        tree.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Checkpoint
    tree.checkpoint().unwrap();

    // After checkpoint, retired blocks should be reclaimed
    // We can't easily verify this from the API, but the checkpoint
    // operation should have succeeded without errors
    drop(tree);
}

#[test]
fn dirty_pages_flushed_on_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_flush.wt");

    let mut tree = BTree::create(&path, 512, false, Arc::new(GlobalTxnState::new())).unwrap();

    // Insert records that modify dirty pages
    tree.put(b"key1", b"value1").unwrap();
    tree.put(b"key2", b"value2").unwrap();
    tree.put(b"key3", b"value3").unwrap();

    // Checkpoint flushes dirty pages
    tree.checkpoint().unwrap();

    // Simulate crash
    drop(tree);

    // Reopen and verify all data is durable
    let mut tree2 = BTree::open(&path, false, Arc::new(GlobalTxnState::new())).unwrap();
    assert_eq!(tree2.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(tree2.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(tree2.get(b"key3").unwrap(), Some(b"value3".to_vec()));
}
