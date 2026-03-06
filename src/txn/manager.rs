use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::core::errors::WrongoDBError;
use crate::storage::btree::BTree;
use crate::storage::mvcc::{MvccState, ReconcileStats};
use crate::txn::{
    GlobalTxnState, Timestamp, Transaction, TxnId, Update, UpdateType, TS_MAX, TS_NONE,
    TXN_ABORTED, TXN_NONE,
};

#[derive(Debug)]
pub struct TransactionManager {
    global_txn: Arc<GlobalTxnState>,
    stores: RwLock<HashMap<String, MvccState>>,
}

impl TransactionManager {
    pub fn new(global_txn: Arc<GlobalTxnState>) -> Self {
        Self {
            global_txn,
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn has_active_transactions(&self) -> bool {
        self.global_txn.has_active_transactions()
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        self.global_txn.begin_snapshot_txn()
    }

    pub fn commit_txn_state(&self, txn: &mut Transaction) -> Result<Timestamp, WrongoDBError> {
        txn.commit(&self.global_txn)
    }

    pub fn abort_txn_state(&self, txn: &mut Transaction) -> Result<(), WrongoDBError> {
        txn.abort(&self.global_txn)
    }

    pub fn put(
        &self,
        store_name: &str,
        btree: &mut BTree,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if txn_id == TXN_NONE {
            btree.put(key, value)?;
            return Ok(());
        }

        let mut stores = self.stores.write();
        let state = stores
            .entry(store_name.to_string())
            .or_insert_with(MvccState::new);

        let chain = state.chain_mut_or_create(key);
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Standard, value.to_vec());
        chain.prepend(update);
        Ok(())
    }

    pub fn delete(
        &self,
        store_name: &str,
        btree: &mut BTree,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        if txn_id == TXN_NONE {
            return btree.delete(key);
        }

        let mut stores = self.stores.write();
        let state = stores
            .entry(store_name.to_string())
            .or_insert_with(MvccState::new);

        let chain = state.chain_mut_or_create(key);
        if let Some(head) = chain.head_mut() {
            head.mark_stopped(txn_id);
        }

        let update = Update::new(txn_id, UpdateType::Tombstone, Vec::new());
        chain.prepend(update);
        Ok(true)
    }

    pub fn get(
        &self,
        store_name: &str,
        btree: &mut BTree,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        {
            let stores = self.stores.read();
            if let Some(state) = stores.get(store_name) {
                if let Some(chain) = state.chain(key) {
                    for update in chain.iter() {
                        let is_aborted = update.time_window.stop_txn == TXN_ABORTED
                            && update.time_window.stop_ts == TS_NONE;
                        if is_aborted {
                            continue;
                        }

                        if update.txn_id <= txn_id {
                            if update.type_ == UpdateType::Standard {
                                return Ok(Some(update.data.clone()));
                            }
                            if update.type_ == UpdateType::Tombstone {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }

        btree.get(key)
    }

    pub fn mark_updates_committed(
        &self,
        store_name: &str,
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        let mut stores = self.stores.write();
        let Some(state) = stores.get_mut(store_name) else {
            return Ok(());
        };

        for chain in state.chains_mut() {
            if let Some(head) = chain.head_mut() {
                if head.txn_id == txn_id {
                    head.time_window.start_ts = txn_id;
                    head.time_window.stop_ts = TS_MAX;
                }
            }
        }
        Ok(())
    }

    pub fn mark_updates_aborted(
        &self,
        store_name: &str,
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        let mut stores = self.stores.write();
        let Some(state) = stores.get_mut(store_name) else {
            return Ok(());
        };

        for chain in state.chains_mut() {
            chain.mark_aborted(txn_id);
        }
        Ok(())
    }

    pub fn mvcc_keys_in_range(
        &self,
        store_name: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<Vec<u8>> {
        let stores = self.stores.read();
        stores
            .get(store_name)
            .map(|state| state.keys_in_range(start, end))
            .unwrap_or_default()
    }

    pub(crate) fn reconcile_store_for_checkpoint(
        &self,
        store_name: &str,
        btree: &mut BTree,
    ) -> Result<ReconcileStats, WrongoDBError> {
        let oldest_active_txn_id = self.global_txn.oldest_active_txn_id();
        let no_active_txns = !self.global_txn.has_active_transactions();
        let (entries, stats) = {
            let mut stores = self.stores.write();
            let Some(state) = stores.get_mut(store_name) else {
                return Ok(ReconcileStats::default());
            };
            state.reconcile_for_checkpoint(oldest_active_txn_id, no_active_txns)
        };

        for (key, update_type, data) in entries {
            match update_type {
                UpdateType::Standard => btree.put(&key, &data)?,
                UpdateType::Tombstone => {
                    let _ = btree.delete(&key)?;
                }
                UpdateType::Reserve => {}
            }
        }

        Ok(stats)
    }
}
