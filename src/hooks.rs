use crate::txn::TxnId;
use crate::WrongoDBError;

pub trait MutationHooks: Send + Sync + std::fmt::Debug {
    fn before_put(
        &self,
        _store_name: &str,
        _key: &[u8],
        _value: &[u8],
        _txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn before_delete(
        &self,
        _store_name: &str,
        _key: &[u8],
        _txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn before_commit(&self, _txn_id: TxnId, _commit_ts: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn before_abort(&self, _txn_id: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn should_apply_locally(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
pub struct NoopMutationHooks;

impl MutationHooks for NoopMutationHooks {}
