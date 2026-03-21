use crate::txn::TxnId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageCommand {
    Put {
        uri: String,
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: TxnId,
    },
    Delete {
        uri: String,
        key: Vec<u8>,
        txn_id: TxnId,
    },
    TxnCommit {
        txn_id: TxnId,
        commit_ts: TxnId,
    },
    TxnAbort {
        txn_id: TxnId,
    },
    Checkpoint,
}
