use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::txn::Transaction;

#[derive(Debug)]
pub(crate) struct ActiveWriteUnit {
    txn: Arc<Mutex<Transaction>>,
    touched_stores: Arc<Mutex<HashSet<String>>>,
}

impl ActiveWriteUnit {
    pub(crate) fn new(txn: Transaction) -> Self {
        Self {
            txn: Arc::new(Mutex::new(txn)),
            touched_stores: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn txn_handle(&self) -> Arc<Mutex<Transaction>> {
        self.txn.clone()
    }

    pub(crate) fn txn_id(&self) -> crate::txn::TxnId {
        self.txn.lock().id()
    }

    pub(crate) fn touched_stores(&self) -> Arc<Mutex<HashSet<String>>> {
        self.touched_stores.clone()
    }
}
