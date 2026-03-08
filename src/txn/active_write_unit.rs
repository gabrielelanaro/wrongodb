use std::sync::Arc;

use parking_lot::Mutex;

use crate::txn::Transaction;

#[derive(Debug)]
pub(crate) struct ActiveWriteUnit {
    txn: Arc<Mutex<Transaction>>,
}

impl ActiveWriteUnit {
    pub(crate) fn new(txn: Transaction) -> Self {
        Self {
            txn: Arc::new(Mutex::new(txn)),
        }
    }

    pub(crate) fn txn_handle(&self) -> Arc<Mutex<Transaction>> {
        self.txn.clone()
    }

    pub(crate) fn txn_id(&self) -> crate::txn::TxnId {
        self.txn.lock().id()
    }
}
