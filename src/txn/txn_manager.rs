use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::errors::WrongoDBError;
use crate::storage::btree::BTree;
use crate::storage::mvcc::MvccState;
use crate::storage::wal::GlobalWal;
use crate::txn::{
    GlobalTxnState, Transaction, TxnId, Update, UpdateType, TS_MAX, TS_NONE, TXN_ABORTED, TXN_NONE,
};

#[derive(Debug)]
pub struct TxnManager {
    global_txn: Arc<GlobalTxnState>,
    global_wal: Option<Arc<Mutex<GlobalWal>>>,
    wal_enabled: bool,
    stores: RwLock<HashMap<String, MvccState>>,
}

impl TxnManager {
    pub fn new(
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
    ) -> Self {
        Self {
            global_txn,
            global_wal,
            wal_enabled,
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    pub fn has_active_transactions(&self) -> bool {
        self.global_txn.has_active_transactions()
    }

    pub fn global_txn(&self) -> &GlobalTxnState {
        &self.global_txn
    }

    pub fn begin_snapshot_txn(&self) -> Transaction {
        self.global_txn.begin_snapshot_txn()
    }

    pub fn put(
        &self,
        store_name: &str,
        btree: &mut BTree,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.log_wal_put(store_name, key, value, txn_id)?;

        if txn_id == TXN_NONE {
            btree.put(key, value)?;
            return Ok(());
        }

        let mut stores = self.stores.write();
        let state = stores
            .entry(store_name.to_string())
            .or_insert_with(|| MvccState::new(self.global_txn.clone()));

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
        self.log_wal_delete(store_name, key, txn_id)?;

        if txn_id == TXN_NONE {
            return btree.delete(key);
        }

        let mut stores = self.stores.write();
        let state = stores
            .entry(store_name.to_string())
            .or_insert_with(|| MvccState::new(self.global_txn.clone()));

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

    pub fn materialize_committed_updates(
        &self,
        store_name: &str,
        btree: &mut BTree,
    ) -> Result<(), WrongoDBError> {
        let entries = {
            let stores = self.stores.read();
            stores
                .get(store_name)
                .map(|state| state.latest_committed_entries())
                .unwrap_or_default()
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

        Ok(())
    }

    pub fn run_gc_for_store(&self, store_name: &str) -> (usize, usize, usize) {
        let mut stores = self.stores.write();
        stores
            .get_mut(store_name)
            .map(MvccState::run_gc)
            .unwrap_or((0, 0, 0))
    }

    pub fn put_recovery(
        &self,
        btree: &mut BTree,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        btree.put(key, value)
    }

    pub fn delete_recovery(&self, btree: &mut BTree, key: &[u8]) -> Result<(), WrongoDBError> {
        let _ = btree.delete(key)?;
        Ok(())
    }

    pub fn log_txn_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            let mut wal = global_wal.lock();
            wal.log_txn_commit(txn_id, commit_ts)?;
            wal.sync()?;
        }
        Ok(())
    }

    pub fn log_txn_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            let mut wal = global_wal.lock();
            wal.log_txn_abort(txn_id)?;
        }
        Ok(())
    }

    pub fn checkpoint_global_wal(&self) -> Result<(), WrongoDBError> {
        if !self.wal_enabled || self.global_txn.has_active_transactions() {
            return Ok(());
        }

        if let Some(global_wal) = self.global_wal.as_ref() {
            let mut wal = global_wal.lock();
            if self.global_txn.has_active_transactions() {
                return Ok(());
            }

            let checkpoint_lsn = wal.log_checkpoint()?;
            wal.set_checkpoint_lsn(checkpoint_lsn)?;
            wal.sync()?;
            wal.truncate_to_checkpoint()?;
        }

        Ok(())
    }

    fn log_wal_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            global_wal.lock().log_put(store_name, key, value, txn_id)?;
        }
        Ok(())
    }

    fn log_wal_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            global_wal.lock().log_delete(store_name, key, txn_id)?;
        }
        Ok(())
    }
}
