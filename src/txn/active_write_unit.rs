use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::txn::Transaction;

#[derive(Debug)]
pub(crate) struct ActiveWriteUnit {
    txn: Transaction,
    touched_stores: Arc<Mutex<HashSet<String>>>,
}

impl ActiveWriteUnit {
    pub(crate) fn new(txn: Transaction) -> Self {
        Self {
            txn,
            touched_stores: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn txn(&self) -> &Transaction {
        &self.txn
    }

    pub(crate) fn txn_mut(&mut self) -> &mut Transaction {
        &mut self.txn
    }

    pub(crate) fn touched_stores(&self) -> Arc<Mutex<HashSet<String>>> {
        self.touched_stores.clone()
    }
}
