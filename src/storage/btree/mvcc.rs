use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::core::errors::WrongoDBError;
use crate::core::lock_stats::{begin_lock_hold, record_lock_wait, LockStatKind};
use crate::txn::{GlobalTxnState, TxnId, Update, UpdateChain, UpdateType, TS_NONE, TXN_ABORTED};

use super::BTree;

const MVCC_SHARD_COUNT: usize = 256;

type ChainMap = HashMap<Vec<u8>, UpdateChain>;

#[derive(Debug)]
struct MvccShard {
    chains: Mutex<ChainMap>,
}

#[derive(Debug)]
pub(super) struct MvccState {
    pub(super) global: Arc<GlobalTxnState>,
    shards: Box<[MvccShard]>,
}

impl MvccState {
    pub(super) fn new(global: Arc<GlobalTxnState>) -> Self {
        let shards = (0..MVCC_SHARD_COUNT)
            .map(|_| MvccShard {
                chains: Mutex::new(HashMap::new()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { global, shards }
    }

    fn shard_index(key: &[u8]) -> usize {
        // FNV-1a 64-bit hash keeps shard assignment stable across runs.
        let mut hash = 0xcbf29ce484222325u64;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % MVCC_SHARD_COUNT
    }

    fn visible_value_for_txn_in_chain(
        chain: &UpdateChain,
        txn_id: TxnId,
    ) -> Option<Option<Vec<u8>>> {
        for update in chain.iter() {
            let is_aborted =
                update.time_window.stop_txn == TXN_ABORTED && update.time_window.stop_ts == TS_NONE;
            if is_aborted {
                continue;
            }

            if update.txn_id <= txn_id {
                if update.type_ == UpdateType::Standard {
                    return Some(Some(update.data.clone()));
                }
                if update.type_ == UpdateType::Tombstone {
                    return Some(None);
                }
            }
        }
        None
    }

    pub(super) fn visible_value_for_txn(
        &self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Option<Option<Vec<u8>>> {
        let shard_idx = Self::shard_index(key);
        let wait_start = Instant::now();
        let chains = self.shards[shard_idx].chains.lock();
        record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::MvccShard);
        let chain = chains.get(key)?;
        Self::visible_value_for_txn_in_chain(chain, txn_id)
    }

    pub(super) fn append_version_with<F>(
        &self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
        before_append: F,
    ) -> Result<(), WrongoDBError>
    where
        F: FnOnce() -> Result<(), WrongoDBError>,
    {
        let shard_idx = Self::shard_index(key);
        let wait_start = Instant::now();
        let mut chains = self.shards[shard_idx].chains.lock();
        record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::MvccShard);

        before_append()?;

        let chain = chains.entry(key.to_vec()).or_default();
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Standard, value.to_vec());
        chain.prepend(update);
        Ok(())
    }

    pub(super) fn append_tombstone_with<F>(
        &self,
        key: &[u8],
        txn_id: TxnId,
        before_append: F,
    ) -> Result<(), WrongoDBError>
    where
        F: FnOnce() -> Result<(), WrongoDBError>,
    {
        let shard_idx = Self::shard_index(key);
        let wait_start = Instant::now();
        let mut chains = self.shards[shard_idx].chains.lock();
        record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::MvccShard);

        before_append()?;

        let chain = chains.entry(key.to_vec()).or_default();
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Tombstone, Vec::new());
        chain.prepend(update);
        Ok(())
    }

    pub(super) fn insert_if_absent_with<F>(
        &self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
        before_append: F,
    ) -> Result<bool, WrongoDBError>
    where
        F: FnOnce() -> Result<(), WrongoDBError>,
    {
        let shard_idx = Self::shard_index(key);
        let wait_start = Instant::now();
        let mut chains = self.shards[shard_idx].chains.lock();
        record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
        let _hold = begin_lock_hold(LockStatKind::MvccShard);

        if let Some(chain) = chains.get(key) {
            if matches!(
                Self::visible_value_for_txn_in_chain(chain, txn_id),
                Some(Some(_))
            ) {
                return Ok(false);
            }
        }

        before_append()?;

        let chain = chains.entry(key.to_vec()).or_default();
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }
        let update = Update::new(txn_id, UpdateType::Standard, value.to_vec());
        chain.prepend(update);
        Ok(true)
    }

    /// Run garbage collection on all update chains.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    pub(super) fn run_gc(&self) -> (usize, usize, usize) {
        let threshold = self.global.oldest_active_txn_id();
        let mut chains_cleaned = 0;
        let mut updates_removed = 0;
        let mut chains_dropped = 0;

        for shard in self.shards.iter() {
            let wait_start = Instant::now();
            let mut chains = shard.chains.lock();
            record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::MvccShard);

            let mut keys_to_remove = Vec::new();
            for (key, chain) in chains.iter_mut() {
                let removed = chain.truncate_obsolete(threshold);
                if removed > 0 {
                    chains_cleaned += 1;
                    updates_removed += removed;
                }
                if chain.is_empty() {
                    keys_to_remove.push(key.clone());
                }
            }

            chains_dropped += keys_to_remove.len();
            for key in keys_to_remove {
                chains.remove(&key);
            }
        }

        (chains_cleaned, updates_removed, chains_dropped)
    }

    /// Get the number of update chains currently stored.
    #[allow(dead_code)]
    pub(super) fn chain_count(&self) -> usize {
        let mut count = 0usize;
        for shard in self.shards.iter() {
            let wait_start = Instant::now();
            let chains = shard.chains.lock();
            record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::MvccShard);
            count += chains.len();
        }
        count
    }

    pub(super) fn keys_in_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for shard in self.shards.iter() {
            let wait_start = Instant::now();
            let chains = shard.chains.lock();
            record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::MvccShard);
            keys.extend(chains.keys().cloned());
        }
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
        for shard in self.shards.iter() {
            let wait_start = Instant::now();
            let chains = shard.chains.lock();
            record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::MvccShard);

            for (key, chain) in chains.iter() {
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
        if let Some(value) = self.mvcc.visible_value_for_txn(key, txn_id) {
            return Ok(value);
        }
        self.get(key)
    }

    pub fn put_version(
        &self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.mvcc
            .append_version_with(key, value, txn_id, || self.log_wal_put(key, value, txn_id))
    }

    pub fn put_version_if_absent(
        &self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        self.mvcc
            .insert_if_absent_with(key, value, txn_id, || self.log_wal_put(key, value, txn_id))
    }

    pub fn delete_version(&self, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.mvcc
            .append_tombstone_with(key, txn_id, || self.log_wal_delete(key, txn_id))
    }

    /// Commit visibility is derived from global transaction state, so there is
    /// no per-chain commit mark pass on the hot path.
    pub fn mark_updates_committed(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let _ = txn_id;
        Ok(())
    }

    /// Mark all updates from this transaction as aborted in MVCC chains.
    /// Note: This does NOT update the transaction state - that's done by Session.
    pub fn mark_updates_aborted(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        // Abort is relatively rare, so we keep a full scan here and avoid tracking
        // per-transaction touched keys on every write.
        for shard in self.mvcc.shards.iter() {
            let wait_start = Instant::now();
            let mut chains = shard.chains.lock();
            record_lock_wait(LockStatKind::MvccShard, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::MvccShard);
            for (_, chain) in chains.iter_mut() {
                chain.mark_aborted(txn_id);
            }
        }

        Ok(())
    }
}
