use super::{GlobalTxnState, Transaction, Update, UpdateChain, UpdateType, TXN_NONE};

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

    let visible_t2 = find_visible(&chain, &t2).expect("t2 visible update");
    assert_eq!(visible_t2.data, b"v2".to_vec());

    let visible_t1 = find_visible(&chain, &t1).expect("t1 visible update");
    assert_eq!(visible_t1.data, b"v1".to_vec());

    let t3 = global.begin_snapshot_txn();
    assert!(find_visible(&chain, &t3).is_none());
}

fn find_visible<'a>(chain: &'a UpdateChain, txn: &Transaction) -> Option<&'a Update> {
    let mut current = chain.iter().next();
    while let Some(update) = current {
        if txn.can_see(update) {
            if update.type_ == UpdateType::Reserve {
                current = update.next.as_deref();
                continue;
            }
            return Some(update);
        }
        current = update.next.as_deref();
    }
    None
}

#[test]
fn txn_allocates_unique_ids() {
    let global = GlobalTxnState::new();
    let t1 = global.begin_snapshot_txn();
    let t2 = global.begin_snapshot_txn();
    assert!(t2.id() > t1.id());
}
