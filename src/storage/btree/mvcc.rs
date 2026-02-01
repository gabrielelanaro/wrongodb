use std::collections::HashMap;
use std::sync::Arc;

use crate::core::errors::WrongoDBError;
use crate::txn::{
    GlobalTxnState, IsolationLevel, Transaction, Update, UpdateChain, UpdateType, WriteOp,
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

        let update = Update::new(txn.id(), UpdateType::Standard, value.to_vec());
        self.mvcc.chain_mut_or_create(key).prepend(update);
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

        let update = Update::new(txn.id(), UpdateType::Tombstone, Vec::new());
        self.mvcc.chain_mut_or_create(key).prepend(update);
        txn.track_write(key, WriteOp::Delete);
        Ok(())
    }

    /// Commit a transaction, making all its updates visible.
    pub fn commit_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        // Log commit to WAL first (for durability)
        if let Some(wal) = self.wal.as_mut() {
            let _commit_ts = wal.log_txn_commit(txn.id(), txn.id())?;
            wal.sync()?; // Ensure commit is durable
        }

        // Update the transaction state
        let _commit_ts = txn.commit(&self.mvcc.global)?;

        // Update time windows for all modifications to mark them as committed
        for write_ref in &txn.modifications {
            if let Some(chain) = self.mvcc.chains.get_mut(&write_ref.key) {
                // The head of the chain is our most recent update
                // Update its time window to reflect the commit
                if let Some(head) = chain.head_mut() {
                    if head.txn_id == txn.id() {
                        head.time_window.start_ts = txn.id();
                        head.time_window.stop_ts = u64::MAX;
                    }
                }
            }
        }

        Ok(())
    }

    /// Abort a transaction, discarding all its updates.
    pub fn abort_txn(&mut self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        // Log abort to WAL
        if let Some(wal) = self.wal.as_mut() {
            wal.log_txn_abort(txn.id())?;
        }

        // Mark all updates in chains as aborted
        for write_ref in &txn.modifications {
            if let Some(chain) = self.mvcc.chains.get_mut(&write_ref.key) {
                // Mark all updates from this transaction as aborted
                chain.mark_aborted(txn.id());
            }
        }

        // Update the transaction state
        txn.abort(&self.mvcc.global)?;

        Ok(())
    }
}
