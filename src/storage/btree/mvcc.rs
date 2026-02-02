use std::collections::HashMap;
use std::sync::Arc;

use crate::core::errors::WrongoDBError;
use crate::txn::{
    GlobalTxnState, IsolationLevel, ReadContext, Transaction, TxnId, Update, UpdateChain, UpdateType,
    WriteOp,
};

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
}

impl BTree {
    pub fn begin_txn(&self, isolation: IsolationLevel) -> Transaction {
        match isolation {
            IsolationLevel::Snapshot => self.mvcc.global.begin_snapshot_txn(),
        }
    }

    pub fn get_mvcc(&mut self, key: &[u8], txn: &Transaction) -> Result<Option<Vec<u8>>, WrongoDBError> {
        if let Some(chain) = self.mvcc.chain(key) {
            if let Some(update) = chain.find_visible(txn) {
                return match update.type_ {
                    UpdateType::Standard => Ok(Some(update.data.clone())),
                    UpdateType::Tombstone => Ok(None),
                    UpdateType::Reserve => Ok(None),
                };
            }
        }

        self.get(key)
    }

    /// Unified read - uses MVCC if txn provided, else reads committed data
    pub fn get_ctx(&mut self, key: &[u8], ctx: &dyn ReadContext) -> Result<Option<Vec<u8>>, WrongoDBError> {
        match ctx.txn() {
            Some(txn) => self.get_mvcc(key, txn),
            None => self.get(key),
        }
    }

    pub fn put_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        // Log to WAL with transaction ID
        if let Some(wal) = self.wal.as_mut() {
            wal.log_put(key, value, txn.id())?;
        }

        let chain = self.mvcc.chain_mut_or_create(key);

        // Mark the current head as stopped (overwritten) before prepending
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn.id());
        }

        let update = Update::new(txn.id(), UpdateType::Standard, value.to_vec());
        chain.prepend(update);
        txn.track_write(key, WriteOp::Put);
        Ok(())
    }

    pub fn delete_mvcc(
        &mut self,
        key: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        // Log to WAL with transaction ID
        if let Some(wal) = self.wal.as_mut() {
            wal.log_delete(key, txn.id())?;
        }

        let chain = self.mvcc.chain_mut_or_create(key);

        // Mark the current head as stopped (deleted) before prepending tombstone
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn.id());
        }

        let update = Update::new(txn.id(), UpdateType::Tombstone, Vec::new());
        chain.prepend(update);
        txn.track_write(key, WriteOp::Delete);
        Ok(())
    }

    /// Mark all updates from this transaction as committed in MVCC chains
    /// and log the commit to WAL.
    /// Note: This does NOT update the transaction state - that's done by Session.
    pub fn mark_updates_committed(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        // Log commit to WAL first (for durability)
        if let Some(wal) = self.wal.as_mut() {
            let _commit_ts = wal.log_txn_commit(txn_id, txn_id)?;
            wal.sync()?; // Ensure commit is durable
        }

        // Update time windows for all modifications to mark them as committed
        // We scan all chains to find updates from this transaction
        for (_, chain) in self.mvcc.chains.iter_mut() {
            // The head of the chain is our most recent update
            if let Some(head) = chain.head_mut() {
                if head.txn_id == txn_id {
                    head.time_window.start_ts = txn_id;
                    head.time_window.stop_ts = u64::MAX;
                }
            }
        }

        Ok(())
    }

    /// Mark all updates from this transaction as aborted in MVCC chains
    /// and log the abort to WAL.
    /// Note: This does NOT update the transaction state - that's done by Session.
    pub fn mark_updates_aborted(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        // Log abort to WAL
        if let Some(wal) = self.wal.as_mut() {
            wal.log_txn_abort(txn_id)?;
        }

        // Mark all updates in chains as aborted
        for (_, chain) in self.mvcc.chains.iter_mut() {
            chain.mark_aborted(txn_id);
        }

        Ok(())
    }
}
