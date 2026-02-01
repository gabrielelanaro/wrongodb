use std::collections::HashMap;
use std::sync::Arc;

use crate::core::errors::WrongoDBError;
use crate::txn::{
    GlobalTxnState, IsolationLevel, Transaction, Update, UpdateChain, UpdateType,
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
        txn.ensure_snapshot(&self.mvcc.global);

        let update = Update::new(txn.id(), UpdateType::Standard, value.to_vec());
        self.mvcc.chain_mut_or_create(key).prepend(update);
        Ok(())
    }

    pub fn delete_mvcc(
        &mut self,
        key: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        txn.ensure_snapshot(&self.mvcc.global);

        let update = Update::new(txn.id(), UpdateType::Tombstone, Vec::new());
        self.mvcc.chain_mut_or_create(key).prepend(update);
        Ok(())
    }
}
