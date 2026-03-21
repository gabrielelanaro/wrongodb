use std::fmt::Debug;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::errors::WrongoDBError;
use crate::storage::wal::GlobalWal;
use crate::txn::TxnId;

pub(crate) trait RecoveryUnit: Send + Sync + Debug {
    fn begin_unit_of_work(&self) -> Result<(), WrongoDBError>;

    fn record_put(
        &self,
        uri: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError>;

    fn record_delete(&self, uri: &str, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError>;

    fn commit_unit_of_work(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError>;

    fn abort_unit_of_work(&self, txn_id: TxnId) -> Result<(), WrongoDBError>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopRecoveryUnit;

impl RecoveryUnit for NoopRecoveryUnit {
    fn begin_unit_of_work(&self) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_put(
        &self,
        _uri: &str,
        _key: &[u8],
        _value: &[u8],
        _txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_delete(&self, _uri: &str, _key: &[u8], _txn_id: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn commit_unit_of_work(&self, _txn_id: TxnId, _commit_ts: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn abort_unit_of_work(&self, _txn_id: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct WalRecoveryUnit {
    wal: Arc<Mutex<GlobalWal>>,
}

impl WalRecoveryUnit {
    pub(crate) fn new(wal: Arc<Mutex<GlobalWal>>) -> Self {
        Self { wal }
    }
}

impl RecoveryUnit for WalRecoveryUnit {
    fn begin_unit_of_work(&self) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_put(
        &self,
        uri: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.wal.lock().log_put(uri, key, value, txn_id)?;
        Ok(())
    }

    fn record_delete(&self, uri: &str, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.wal.lock().log_delete(uri, key, txn_id)?;
        Ok(())
    }

    fn commit_unit_of_work(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
        let mut wal = self.wal.lock();
        wal.log_txn_commit(txn_id, commit_ts)?;
        wal.sync()?;
        Ok(())
    }

    fn abort_unit_of_work(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.wal.lock().log_txn_abort(txn_id)?;
        Ok(())
    }
}
