use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::txn::TxnId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DurableOp {
    Put {
        store_name: String,
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: TxnId,
    },
    Delete {
        store_name: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedDurableOp {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) op: DurableOp,
}

impl From<DurableOp> for RaftCommand {
    fn from(op: DurableOp) -> Self {
        match op {
            DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            } => RaftCommand::Put {
                store_name,
                key,
                value,
                txn_id,
            },
            DurableOp::Delete {
                store_name,
                key,
                txn_id,
            } => RaftCommand::Delete {
                store_name,
                key,
                txn_id,
            },
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
                store_name,
                key,
                value,
                txn_id,
            } => DurableOp::Put {
                store_name,
                key,
                value,
                txn_id,
            },
            RaftCommand::Delete {
                store_name,
                key,
                txn_id,
            } => DurableOp::Delete {
                store_name,
                key,
                txn_id,
            },
            RaftCommand::TxnCommit { txn_id, commit_ts } => {
                DurableOp::TxnCommit { txn_id, commit_ts }
            }
            RaftCommand::TxnAbort { txn_id } => DurableOp::TxnAbort { txn_id },
            RaftCommand::Checkpoint => DurableOp::Checkpoint,
        }
    }
}

impl From<CommittedCommand> for CommittedDurableOp {
    fn from(command: CommittedCommand) -> Self {
        Self {
            index: command.index,
            term: command.term,
            op: command.command.into(),
        }
    }
}
