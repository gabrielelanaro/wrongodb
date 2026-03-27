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

#[test]
fn gc_threshold_uses_checkpoint_pinned_id() {
    let global = GlobalTxnState::new();

    // Start txn 1 and commit it
    let mut txn1 = global.begin_snapshot_txn();
    txn1.commit(&global).unwrap();

    // Start txn 2 (active)
    let mut txn2 = global.begin_snapshot_txn();

    // Without checkpoint, gc_threshold = oldest_active (txn2's id)
    let threshold_without_checkpoint = global.gc_threshold();

    // Begin checkpoint - should pin at txn2's id
    global.begin_checkpoint();
    assert_eq!(global.gc_threshold(), threshold_without_checkpoint);

    // Commit txn2
    txn2.commit(&global).unwrap();

    // gc_threshold should still return the checkpoint's pinned value
    // even though oldest_active has advanced
    assert_eq!(global.gc_threshold(), threshold_without_checkpoint);

    // End checkpoint
    global.end_checkpoint();

    // Now gc_threshold should advance past the old checkpoint
    assert!(global.gc_threshold() > threshold_without_checkpoint);
}

#[test]
fn gc_threshold_without_checkpoint_uses_oldest_active() {
    let global = GlobalTxnState::new();

    // No checkpoint active, gc_threshold should equal oldest_active
    let mut txn1 = global.begin_snapshot_txn();
    txn1.commit(&global).unwrap();

    let mut txn2 = global.begin_snapshot_txn();
    assert_eq!(global.gc_threshold(), global.oldest_active_txn_id());

    txn2.commit(&global).unwrap();
    assert_eq!(global.gc_threshold(), global.oldest_active_txn_id());
}

#[test]
fn gc_threshold_returns_minimum_of_oldest_and_checkpoint() {
    let global = GlobalTxnState::new();

    // Start txn 1 (will be the checkpoint pinned value)
    let mut txn1 = global.begin_snapshot_txn();

    // Begin checkpoint - pins at txn1's id
    global.begin_checkpoint();
    let checkpoint_pinned = global.gc_threshold();

    // Commit txn1
    txn1.commit(&global).unwrap();

    // Start txn2 (now the oldest active)
    let txn2 = global.begin_snapshot_txn();

    // gc_threshold should return the checkpoint's pinned value (lower than oldest_active)
    assert_eq!(global.gc_threshold(), checkpoint_pinned);
    assert!(global.oldest_active_txn_id() > checkpoint_pinned);

    // End checkpoint
    global.end_checkpoint();

    // Now gc_threshold should return the oldest_active
    assert_eq!(global.gc_threshold(), global.oldest_active_txn_id());

    drop(txn2);
}
