use std::sync::Arc;
use tempfile::tempdir;

use wrongodb::{BTree, BlockFile, GlobalTxnState, InternalPage, NONE_BLOCK_ID};

fn internal_levels(path: &std::path::Path) -> usize {
    let mut bf = BlockFile::open(path).unwrap();
    let mut node_id = bf.root_block_id();
    assert!(node_id != NONE_BLOCK_ID);

    let mut levels = 0usize;
    loop {
        let mut payload = bf.read_block(node_id, false).unwrap();
        match payload[0] {
            1 => break,
            2 => {
                levels += 1;
                let page = InternalPage::open(&mut payload).unwrap();
                node_id = page.first_child().unwrap();
            }
            other => panic!("unexpected page type: {other}"),
        }
    }
    bf.close().unwrap();
    levels
}

#[test]
fn grows_tree_height_past_two_levels_and_survives_reopen() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_slice_e_height.wt");

    let mut tree = BTree::create(&path, 256, Arc::new(GlobalTxnState::new())).unwrap();
    for i in 0..800u32 {
        let k = format!("k{i:04}").into_bytes();
        let v = vec![b'v'; 24];
        tree.put(&k, &v).unwrap();
    }

    for i in 0..800u32 {
        let k = format!("k{i:04}").into_bytes();
        let got = tree.get(&k).unwrap().unwrap();
        assert_eq!(got, vec![b'v'; 24]);
    }

    tree.checkpoint().unwrap();

    // Height > 2 implies at least two internal levels (root internal + one more).
    assert!(internal_levels(&path) >= 2);

    drop(tree);

    let mut tree2 = BTree::open(&path, Arc::new(GlobalTxnState::new())).unwrap();
    for i in 0..800u32 {
        let k = format!("k{i:04}").into_bytes();
        assert!(tree2.get(&k).unwrap().is_some());
    }
    assert!(internal_levels(&path) >= 2);
}

#[test]
fn ordered_range_scan_is_sorted_and_respects_bounds() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("btree_slice_e_scan.wt");

    let mut tree = BTree::create(&path, 384, Arc::new(GlobalTxnState::new())).unwrap();
    for i in 0..500u32 {
        let k = format!("k{i:04}").into_bytes();
        let v = format!("v{i:04}").into_bytes();
        tree.put(&k, &v).unwrap();
    }

    let all: Vec<Vec<u8>> = tree
        .range(None, None)
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(all.len(), 500);
    assert!(all.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(all.first().unwrap(), b"k0000");
    assert_eq!(all.last().unwrap(), b"k0499");

    let slice: Vec<Vec<u8>> = tree
        .range(Some(b"k0100"), Some(b"k0200"))
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(slice.len(), 100);
    assert_eq!(slice.first().unwrap(), b"k0100");
    assert_eq!(slice.last().unwrap(), b"k0199");
    assert!(slice.windows(2).all(|w| w[0] < w[1]));

    tree.checkpoint().unwrap();

    drop(tree);

    let mut tree2 = BTree::open(&path, Arc::new(GlobalTxnState::new())).unwrap();
    let slice2: Vec<Vec<u8>> = tree2
        .range(Some(b"k0100"), Some(b"k0200"))
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(slice2, slice);
}
