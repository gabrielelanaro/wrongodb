use crate::core::errors::StorageError;
use crate::txn::{Timestamp, TxnId};
use crate::WrongoDBError;

const CMD_PUT: u8 = 1;
const CMD_DELETE: u8 = 2;
const CMD_TXN_COMMIT: u8 = 3;
const CMD_TXN_ABORT: u8 = 4;
const CMD_CHECKPOINT: u8 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaftCommand {
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
        commit_ts: Timestamp,
    },
    TxnAbort {
        txn_id: TxnId,
    },
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedCommand {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) command: RaftCommand,
}

impl RaftCommand {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            RaftCommand::Put {
                uri,
                key,
                value,
                txn_id,
            } => {
                out.push(CMD_PUT);
                encode_string(&mut out, uri);
                encode_bytes(&mut out, key);
                encode_bytes(&mut out, value);
                out.extend_from_slice(&txn_id.to_le_bytes());
            }
            RaftCommand::Delete { uri, key, txn_id } => {
                out.push(CMD_DELETE);
                encode_string(&mut out, uri);
                encode_bytes(&mut out, key);
                out.extend_from_slice(&txn_id.to_le_bytes());
            }
            RaftCommand::TxnCommit { txn_id, commit_ts } => {
                out.push(CMD_TXN_COMMIT);
                out.extend_from_slice(&txn_id.to_le_bytes());
                out.extend_from_slice(&commit_ts.to_le_bytes());
            }
            RaftCommand::TxnAbort { txn_id } => {
                out.push(CMD_TXN_ABORT);
                out.extend_from_slice(&txn_id.to_le_bytes());
            }
            RaftCommand::Checkpoint => {
                out.push(CMD_CHECKPOINT);
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, WrongoDBError> {
        if bytes.is_empty() {
            return Err(StorageError("raft command payload is empty".into()).into());
        }

        let tag = bytes[0];
        let mut cursor = 1usize;
        let cmd = match tag {
            CMD_PUT => {
                let uri = decode_string(bytes, &mut cursor)?;
                let key = decode_bytes(bytes, &mut cursor)?;
                let value = decode_bytes(bytes, &mut cursor)?;
                let txn_id = decode_u64(bytes, &mut cursor, "txn_id")?;
                RaftCommand::Put {
                    uri,
                    key,
                    value,
                    txn_id,
                }
            }
            CMD_DELETE => {
                let uri = decode_string(bytes, &mut cursor)?;
                let key = decode_bytes(bytes, &mut cursor)?;
                let txn_id = decode_u64(bytes, &mut cursor, "txn_id")?;
                RaftCommand::Delete { uri, key, txn_id }
            }
            CMD_TXN_COMMIT => {
                let txn_id = decode_u64(bytes, &mut cursor, "txn_id")?;
                let commit_ts = decode_u64(bytes, &mut cursor, "commit_ts")?;
                RaftCommand::TxnCommit { txn_id, commit_ts }
            }
            CMD_TXN_ABORT => {
                let txn_id = decode_u64(bytes, &mut cursor, "txn_id")?;
                RaftCommand::TxnAbort { txn_id }
            }
            CMD_CHECKPOINT => RaftCommand::Checkpoint,
            other => {
                return Err(StorageError(format!("unknown raft command tag: {other}")).into());
            }
        };

        if cursor != bytes.len() {
            return Err(StorageError(
                "raft command payload has trailing bytes after decode".into(),
            )
            .into());
        }

        Ok(cmd)
    }
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    encode_bytes(out, value.as_bytes());
}

fn encode_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn decode_string(data: &[u8], cursor: &mut usize) -> Result<String, WrongoDBError> {
    let bytes = decode_bytes(data, cursor)?;
    String::from_utf8(bytes)
        .map_err(|e| StorageError(format!("invalid UTF-8 in raft command: {e}")).into())
}

fn decode_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>, WrongoDBError> {
    let len = decode_u32(data, cursor, "bytes_len")? as usize;
    let end = cursor.saturating_add(len);
    if end > data.len() {
        return Err(StorageError("raft command payload is truncated".into()).into());
    }
    let bytes = data[*cursor..end].to_vec();
    *cursor = end;
    Ok(bytes)
}

fn decode_u32(data: &[u8], cursor: &mut usize, field: &str) -> Result<u32, WrongoDBError> {
    let end = cursor.saturating_add(4);
    if end > data.len() {
        return Err(StorageError(format!("raft command missing u32 field: {field}")).into());
    }
    let value = u32::from_le_bytes(
        data[*cursor..end]
            .try_into()
            .map_err(|_| StorageError(format!("raft command invalid u32 field: {field}")))?,
    );
    *cursor = end;
    Ok(value)
}

fn decode_u64(data: &[u8], cursor: &mut usize, field: &str) -> Result<u64, WrongoDBError> {
    let end = cursor.saturating_add(8);
    if end > data.len() {
        return Err(StorageError(format!("raft command missing u64 field: {field}")).into());
    }
    let value = u64::from_le_bytes(
        data[*cursor..end]
            .try_into()
            .map_err(|_| StorageError(format!("raft command invalid u64 field: {field}")))?,
    );
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_command_variants() {
        let commands = vec![
            RaftCommand::Put {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 7,
            },
            RaftCommand::Delete {
                uri: "table:users".to_string(),
                key: b"k1".to_vec(),
                txn_id: 8,
            },
            RaftCommand::TxnCommit {
                txn_id: 9,
                commit_ts: 10,
            },
            RaftCommand::TxnAbort { txn_id: 11 },
            RaftCommand::Checkpoint,
        ];

        for command in commands {
            let encoded = command.encode();
            let decoded = RaftCommand::decode(&encoded).unwrap();
            assert_eq!(decoded, command);
        }
    }

    #[test]
    fn round_trip_reserved_metadata_uri() {
        let command = RaftCommand::Put {
            uri: "metadata:".to_string(),
            key: b"table:users".to_vec(),
            value: b"metadata-row".to_vec(),
            txn_id: 7,
        };

        let encoded = command.encode();
        let decoded = RaftCommand::decode(&encoded).unwrap();
        assert_eq!(decoded, command);
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        let err = RaftCommand::decode(&[0xFF]).unwrap_err();
        assert!(err.to_string().contains("unknown raft command tag"));
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let command = RaftCommand::Put {
            uri: "table:users".to_string(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            txn_id: 7,
        };
        let mut encoded = command.encode();
        encoded.truncate(encoded.len() - 1);

        let err = RaftCommand::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("missing u64 field"));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut encoded = RaftCommand::Checkpoint.encode();
        encoded.extend_from_slice(&[1, 2, 3]);

        let err = RaftCommand::decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("trailing bytes"));
    }
}
