use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::txn::{RecoveryUnit, Transaction};

#[derive(Debug)]
pub(crate) struct WriteUnitOfWork {
    txn: Transaction,
    touched_stores: Arc<Mutex<HashSet<String>>>,
    recovery_unit: Arc<dyn RecoveryUnit>,
}

impl WriteUnitOfWork {
    pub(crate) fn new(txn: Transaction, recovery_unit: Arc<dyn RecoveryUnit>) -> Self {
        Self {
            txn,
            touched_stores: Arc::new(Mutex::new(HashSet::new())),
            recovery_unit,
        }
    }

    pub(crate) fn txn(&self) -> &Transaction {
        &self.txn
    }

    pub(crate) fn txn_mut(&mut self) -> &mut Transaction {
        &mut self.txn
    }

    pub(crate) fn recovery_unit(&self) -> Arc<dyn RecoveryUnit> {
        self.recovery_unit.clone()
    }

    pub(crate) fn touched_stores(&self) -> Arc<Mutex<HashSet<String>>> {
        self.touched_stores.clone()
    }
}
