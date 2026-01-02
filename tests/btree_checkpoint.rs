use tempfile::tempdir;

use wrongodb::{BlockFile, BTree, NONE_BLOCK_ID};

#[test]
fn checkpoint_commit_selects_new_root_on_reopen() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_root.wt");

    let mut tree = BTree::create(&path, 512).unwrap();

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

    let mut tree2 = BTree::open(&path).unwrap();
    assert_eq!(tree2.get(b"alpha").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn crash_before_checkpoint_uses_old_root() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_crash.wt");

    let mut tree = BTree::create(&path, 512).unwrap();

    let bf = BlockFile::open(&path).unwrap();
    let stable_root = bf.root_block_id();
    bf.close().unwrap();

    tree.put(b"beta", b"value").unwrap();
    drop(tree);

    let bf2 = BlockFile::open(&path).unwrap();
    let reopened_root = bf2.root_block_id();
    bf2.close().unwrap();

    assert_eq!(reopened_root, stable_root);

    let mut tree2 = BTree::open(&path).unwrap();
    assert_eq!(tree2.get(b"beta").unwrap(), None);
}

#[test]
fn retired_blocks_not_reused_before_checkpoint() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_checkpoint_retired.wt");

    let mut tree = BTree::create(&path, 512).unwrap();

    tree.put(b"k1", b"v1").unwrap();
    let mut bf = BlockFile::open(&path).unwrap();
    let blocks_after_first = bf.num_blocks().unwrap();
    let free_head_after_first = bf.header.free_list_head;
    bf.close().unwrap();

    assert_eq!(free_head_after_first, NONE_BLOCK_ID);

    tree.put(b"k2", b"v2").unwrap();
    let mut bf2 = BlockFile::open(&path).unwrap();
    let blocks_after_second = bf2.num_blocks().unwrap();
    let free_head_after_second = bf2.header.free_list_head;
    bf2.close().unwrap();

    assert_eq!(free_head_after_second, NONE_BLOCK_ID);
    assert!(blocks_after_second >= blocks_after_first);
}

#[test]
fn coalesces_updates_between_checkpoints() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_coalesce.wt");

    let mut tree = BTree::create(&path, 512).unwrap();

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
