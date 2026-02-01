use super::{GlobalTxnState, Update, UpdateChain, UpdateType, TXN_NONE};

#[test]
fn snapshot_visibility_basic() {
    let global = GlobalTxnState::new();
    let t1 = global.begin_snapshot_txn();
    let t2 = global.begin_snapshot_txn();

    let s1 = t1.snapshot().expect("t1 snapshot");
    assert!(s1.is_visible(t1.id()));
    assert!(!s1.is_visible(t2.id()));
    assert!(s1.is_visible(TXN_NONE));

    let s2 = t2.snapshot().expect("t2 snapshot");
    assert!(s2.is_visible(t2.id()));
    assert!(!s2.is_visible(t1.id()));
    assert!(s2.is_visible(TXN_NONE));
}

#[test]
fn update_chain_visibility() {
    let global = GlobalTxnState::new();
    let t1 = global.begin_snapshot_txn();
    let t2 = global.begin_snapshot_txn();

    let mut chain = UpdateChain::default();
    chain.prepend(Update::new(t1.id(), UpdateType::Standard, b"v1".to_vec()));
    chain.prepend(Update::new(t2.id(), UpdateType::Standard, b"v2".to_vec()));

    let visible_t2 = chain.find_visible(&t2).expect("t2 visible update");
    assert_eq!(visible_t2.data, b"v2".to_vec());

    let visible_t1 = chain.find_visible(&t1).expect("t1 visible update");
    assert_eq!(visible_t1.data, b"v1".to_vec());

    let t3 = global.begin_snapshot_txn();
    assert!(chain.find_visible(&t3).is_none());
}

#[test]
fn txn_allocates_unique_ids() {
    let global = GlobalTxnState::new();
    let t1 = global.begin_snapshot_txn();
    let t2 = global.begin_snapshot_txn();
    assert!(t2.id() > t1.id());
}
