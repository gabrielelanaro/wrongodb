use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::core::errors::WrongoDBError;
use crate::core::lock_stats::{begin_lock_hold, record_lock_wait, LockStatKind};
use crate::txn::{GlobalTxnState, TxnId, Update, UpdateChain, UpdateType, TS_NONE, TXN_ABORTED};

use super::BTree;

#[derive(Debug)]
pub(super) struct MvccState {
    pub(super) global: Arc<GlobalTxnState>,
    chains: HashMap<Vec<u8>, UpdateChain>,
}

impl MvccState {
    pub(super) fn new(global: Arc<GlobalTxnState>) -> Self {
        Self {
            global,
            chains: HashMap::new(),
        }
    }

    pub(super) fn chain(&self, key: &[u8]) -> Option<&UpdateChain> {
        self.chains.get(key)
    }

    pub(super) fn chain_mut_or_create(&mut self, key: &[u8]) -> &mut UpdateChain {
        self.chains.entry(key.to_vec()).or_default()
    }

    /// Run garbage collection on all update chains.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    pub(super) fn run_gc(&mut self) -> (usize, usize, usize) {
        let threshold = self.global.oldest_active_txn_id();
        let mut chains_cleaned = 0;
        let mut updates_removed = 0;
        let mut keys_to_remove = Vec::new();

        for (key, chain) in self.chains.iter_mut() {
            let removed = chain.truncate_obsolete(threshold);
            if removed > 0 {
                chains_cleaned += 1;
                updates_removed += removed;
            }
            if chain.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }

        let chains_dropped = keys_to_remove.len();
        for key in keys_to_remove {
            self.chains.remove(&key);
        }

        (chains_cleaned, updates_removed, chains_dropped)
    }

    /// Get the number of update chains currently stored.
    #[allow(dead_code)]
    pub(super) fn chain_count(&self) -> usize {
        self.chains.len()
    }

    pub(super) fn keys_in_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self.chains.keys().cloned().collect();
        if start.is_some() || end.is_some() {
            keys.retain(|key| {
                let after_start = start.is_none_or(|s| key.as_slice() >= s);
                let before_end = end.is_none_or(|e| key.as_slice() < e);
                after_start && before_end
            });
        }
        keys.sort();
        keys
    }

    pub(super) fn latest_committed_entries(&self) -> Vec<(Vec<u8>, UpdateType, Vec<u8>)> {
        let mut out = Vec::new();
        for (key, chain) in self.chains.iter() {
            for update in chain.iter() {
                let is_aborted = update.time_window.stop_txn == TXN_ABORTED
                    && update.time_window.stop_ts == TS_NONE;
                if is_aborted {
                    continue;
                }

                // A version is durable-materializable if its writer transaction is committed
                // (i.e. not active and not aborted). TXN_NONE is always committed.
                if update.txn_id != crate::txn::TXN_NONE
                    && (self.global.is_aborted(update.txn_id)
                        || self.global.is_active(update.txn_id))
                {
                    continue;
                }
                match update.type_ {
                    UpdateType::Standard => {
                        out.push((key.clone(), UpdateType::Standard, update.data.clone()))
                    }
                    UpdateType::Tombstone => {
                        out.push((key.clone(), UpdateType::Tombstone, Vec::new()))
                    }
                    UpdateType::Reserve => {}
                }
                break;
            }
        }
        out
    }
}

impl BTree {
    pub fn get_version(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        record_lock_wait(LockStatKind::MvccShard, Duration::ZERO);
        let _hold = begin_lock_hold(LockStatKind::MvccShard);
        if let Some(chain) = self.mvcc.chain(key) {
            for update in chain.iter() {
                let is_aborted = update.time_window.stop_txn == crate::txn::TXN_ABORTED
                    && update.time_window.stop_ts == crate::txn::TS_NONE;
                if is_aborted {
                    continue;
                }

                if update.txn_id <= txn_id {
                    if update.type_ == UpdateType::Standard {
                        return Ok(Some(update.data.clone()));
                    } else if update.type_ == UpdateType::Tombstone {
                        return Ok(None);
                    }
                }
            }
        }

        self.get(key)
    }

    pub fn put_version(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        record_lock_wait(LockStatKind::MvccShard, Duration::ZERO);
        let _hold = begin_lock_hold(LockStatKind::MvccShard);
        self.log_wal_put(key, value, txn_id)?;

        let chain = self.mvcc.chain_mut_or_create(key);

        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Standard, value.to_vec());
        chain.prepend(update);
        Ok(())
    }

    pub fn delete_version(&mut self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        record_lock_wait(LockStatKind::MvccShard, Duration::ZERO);
        let _hold = begin_lock_hold(LockStatKind::MvccShard);
        self.log_wal_delete(key, txn_id)?;

        let chain = self.mvcc.chain_mut_or_create(key);

        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Tombstone, Vec::new());
        chain.prepend(update);
        Ok(())
    }

    /// Commit visibility is derived from global transaction state, so there is
    /// no per-chain commit mark pass on the hot path.
    pub fn mark_updates_committed(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let _ = txn_id;
        Ok(())
    }

    /// Mark all updates from this transaction as aborted in MVCC chains.
    /// Note: This does NOT update the transaction state - that's done by Session.
    pub fn mark_updates_aborted(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        // Abort is relatively rare, so we keep a full scan here and avoid tracking
        // per-transaction touched keys on every write.
        for (_, chain) in self.mvcc.chains.iter_mut() {
            chain.mark_aborted(txn_id);
        }

        Ok(())
    }
}
