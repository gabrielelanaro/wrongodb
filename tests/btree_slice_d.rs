use tempfile::tempdir;

use wrongodb::{BTree, BlockFile, InternalPage};

#[test]
fn splits_root_leaf_into_internal_root() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree.wt");

    let mut tree = BTree::create(&path, 256).unwrap();

    for i in 0..20u32 {
        let k = format!("k{i:04}").into_bytes();
        let v = vec![b'v'; 24];
        tree.put(&k, &v).unwrap();
    }

    for i in 0..20u32 {
        let k = format!("k{i:04}").into_bytes();
        let got = tree.get(&k).unwrap().unwrap();
        assert_eq!(got, vec![b'v'; 24]);
    }

    // Root should now be internal.
    let mut bf = BlockFile::open(&path).unwrap();
    let root = bf.header.root_block_id;
    assert!(root != 0);
    let payload = bf.read_block(root, true).unwrap();
    assert_eq!(payload[0], 2);
    bf.close().unwrap();

    // Reopen and confirm reads still work.
    let mut tree2 = BTree::open(&path).unwrap();
    for i in 0..20u32 {
        let k = format!("k{i:04}").into_bytes();
        assert!(tree2.get(&k).unwrap().is_some());
    }
}

#[test]
fn multiple_leaf_splits_update_root_separators() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree2.wt");

    // Slightly larger pages to ensure the root internal node has room for multiple separators.
    let mut tree = BTree::create(&path, 512).unwrap();
    for i in 0..60u32 {
        let k = format!("k{i:04}").into_bytes();
        let v = vec![b'x'; 16];
        tree.put(&k, &v).unwrap();
    }

    for i in 0..60u32 {
        let k = format!("k{i:04}").into_bytes();
        let got = tree.get(&k).unwrap().unwrap();
        assert_eq!(got, vec![b'x'; 16]);
    }

    let mut bf = BlockFile::open(&path).unwrap();
    let root = bf.header.root_block_id;
    let mut root_payload = bf.read_block(root, true).unwrap();
    assert_eq!(root_payload[0], 2);

    let root_page = InternalPage::open(&mut root_payload).unwrap();
    let slots = root_page.slot_count().unwrap();
    assert!(slots >= 2);
    bf.close().unwrap();
}
