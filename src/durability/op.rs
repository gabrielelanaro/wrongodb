use crate::raft::command::RaftCommand;
use crate::txn::TxnId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DurableOp {
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

impl From<DurableOp> for RaftCommand {
    fn from(op: DurableOp) -> Self {
        match op {
            DurableOp::Put {
                uri,
                key,
                value,
                txn_id,
            } => RaftCommand::Put {
                uri,
                key,
                value,
                txn_id,
            },
            DurableOp::Delete { uri, key, txn_id } => RaftCommand::Delete { uri, key, txn_id },
            DurableOp::TxnCommit { txn_id, commit_ts } => {
                RaftCommand::TxnCommit { txn_id, commit_ts }
            }
            DurableOp::TxnAbort { txn_id } => RaftCommand::TxnAbort { txn_id },
            DurableOp::Checkpoint => RaftCommand::Checkpoint,
        }
    }
}

impl From<RaftCommand> for DurableOp {
    fn from(command: RaftCommand) -> Self {
        match command {
            RaftCommand::Put {
                uri,
                key,
                value,
                txn_id,
            } => DurableOp::Put {
                uri,
                key,
                value,
                txn_id,
            },
            RaftCommand::Delete { uri, key, txn_id } => DurableOp::Delete { uri, key, txn_id },
            RaftCommand::TxnCommit { txn_id, commit_ts } => {
                DurableOp::TxnCommit { txn_id, commit_ts }
            }
            RaftCommand::TxnAbort { txn_id } => DurableOp::TxnAbort { txn_id },
            RaftCommand::Checkpoint => DurableOp::Checkpoint,
        }
    }
}
