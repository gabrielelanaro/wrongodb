use super::{GlobalTxnState, Transaction, TXN_NONE};
use crate::storage::mvcc::{Update, UpdateChain, UpdateHandle, UpdateType};

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
    assert_eq!(visible_t2.read().data, b"v2".to_vec());

    let visible_t1 = find_visible(&chain, &t1).expect("t1 visible update");
    assert_eq!(visible_t1.read().data, b"v1".to_vec());

    let t3 = global.begin_snapshot_txn();
    assert!(find_visible(&chain, &t3).is_none());
}

fn find_visible(chain: &UpdateChain, txn: &Transaction) -> Option<UpdateHandle> {
    let mut current = chain.iter().next();
    while let Some(update_ref) = current {
        let update = update_ref.read();
        if txn.can_see(&update) {
            if update.type_ == UpdateType::Reserve {
                current = update.next.clone();
                continue;
            }
            drop(update);
            return Some(update_ref);
        }
        current = update.next.clone();
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

#[test]
fn transaction_commit_marks_recorded_ops_committed() {
    let global = GlobalTxnState::new();
    let mut txn = global.begin_snapshot_txn();

    let mut chain = UpdateChain::default();
    let update_ref = chain.prepend(Update::new(txn.id(), UpdateType::Standard, b"v1".to_vec()));
    txn.track_update(update_ref.clone());

    txn.commit(&global).unwrap();

    let update = update_ref.read();
    assert!(update.is_committed());
    assert_eq!(update.time_window.start_ts, txn.id());
}

#[test]
fn transaction_abort_marks_recorded_ops_aborted() {
    let global = GlobalTxnState::new();
    let mut txn = global.begin_snapshot_txn();

    let mut chain = UpdateChain::default();
    let update_ref = chain.prepend(Update::new(txn.id(), UpdateType::Standard, b"v1".to_vec()));
    txn.track_update(update_ref.clone());

    txn.abort(&global).unwrap();

    let update = update_ref.read();
    assert!(update.is_aborted());
}

#[test]
fn transaction_abort_restores_previous_current_update() {
    let global = GlobalTxnState::new();
    let mut txn = global.begin_snapshot_txn();

    let mut chain = UpdateChain::default();
    let base_ref = chain.prepend(Update::new(TXN_NONE, UpdateType::Standard, b"v1".to_vec()));
    base_ref.write().mark_committed(1);

    let update_ref = chain.prepend(Update::new(txn.id(), UpdateType::Standard, b"v2".to_vec()));
    base_ref.write().mark_stopped(txn.id());
    txn.track_update(update_ref.clone());

    txn.abort(&global).unwrap();

    assert!(update_ref.read().is_aborted());
    assert!(base_ref.read().is_current());
}

#[test]
fn transaction_commit_preserves_stop_metadata_on_overwritten_updates() {
    let global = GlobalTxnState::new();
    let mut txn = global.begin_snapshot_txn();

    let mut chain = UpdateChain::default();
    let base_ref = chain.prepend(Update::new(TXN_NONE, UpdateType::Standard, b"v1".to_vec()));
    base_ref.write().mark_committed(1);

    let update_ref = chain.prepend(Update::new(txn.id(), UpdateType::Standard, b"v2".to_vec()));
    base_ref.write().mark_stopped(txn.id());
    txn.track_update(update_ref.clone());

    txn.commit(&global).unwrap();

    let update = update_ref.read();
    assert!(update.is_committed());
    assert!(update.is_current());

    let base = base_ref.read();
    assert_eq!(base.time_window.stop_txn, txn.id());
    assert_eq!(base.time_window.stop_ts, txn.id());
}
