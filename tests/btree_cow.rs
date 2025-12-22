use tempfile::tempdir;

use wrongodb::{BlockFile, BTree, LeafPage};

#[test]
fn cow_put_preserves_old_root_leaf() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_cow.wt");

    let mut tree = BTree::create(&path, 512).unwrap();

    let bf = BlockFile::open(&path).unwrap();
    let old_root = bf.root_block_id();
    bf.close().unwrap();

    let key = b"alpha";
    let value = b"value";
    tree.put(key, value).unwrap();
    assert_eq!(tree.get(key).unwrap().unwrap(), value);

    let mut bf2 = BlockFile::open(&path).unwrap();
    let new_root = bf2.root_block_id();
    assert_ne!(new_root, old_root);

    let mut old_payload = bf2.read_block(old_root, true).unwrap();
    let old_leaf = LeafPage::open(&mut old_payload).unwrap();
    assert!(old_leaf.get(key).unwrap().is_none());

    let mut new_payload = bf2.read_block(new_root, true).unwrap();
    let new_leaf = LeafPage::open(&mut new_payload).unwrap();
    assert_eq!(new_leaf.get(key).unwrap().unwrap(), value);
    bf2.close().unwrap();
}
