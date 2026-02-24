use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::raft::protocol::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::runtime::{RaftInboundMessage, RaftOutboundMessage};
use crate::WrongoDBError;

const MAX_FRAME_SIZE: usize = 32 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RaftWireMessage {
    RequestVote { req: RequestVoteRequest },
    AppendEntries { req: AppendEntriesRequest },
    RequestVoteResponse { resp: RequestVoteResponse },
    AppendEntriesResponse { resp: AppendEntriesResponse },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RaftWireEnvelope {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) msg: RaftWireMessage,
}

pub(crate) fn outbound_to_wire(
    local_node_id: &str,
    outbound: RaftOutboundMessage,
) -> RaftWireEnvelope {
    match outbound {
        RaftOutboundMessage::RequestVote { to, req } => RaftWireEnvelope {
            from: local_node_id.to_string(),
            to,
            msg: RaftWireMessage::RequestVote { req },
        },
        RaftOutboundMessage::AppendEntries { to, req } => RaftWireEnvelope {
            from: local_node_id.to_string(),
            to,
            msg: RaftWireMessage::AppendEntries { req },
        },
        RaftOutboundMessage::RequestVoteResponse { to, resp } => RaftWireEnvelope {
            from: local_node_id.to_string(),
            to,
            msg: RaftWireMessage::RequestVoteResponse { resp },
        },
        RaftOutboundMessage::AppendEntriesResponse { to, resp } => RaftWireEnvelope {
            from: local_node_id.to_string(),
            to,
            msg: RaftWireMessage::AppendEntriesResponse { resp },
        },
    }
}

pub(crate) fn wire_to_inbound(
    local_node_id: &str,
    envelope: RaftWireEnvelope,
) -> Option<RaftInboundMessage> {
    if envelope.to != local_node_id {
        return None;
    }

    let inbound = match envelope.msg {
        RaftWireMessage::RequestVote { req } => RaftInboundMessage::RequestVote {
            from: envelope.from,
            req,
        },
        RaftWireMessage::AppendEntries { req } => RaftInboundMessage::AppendEntries {
            from: envelope.from,
            req,
        },
        RaftWireMessage::RequestVoteResponse { resp } => RaftInboundMessage::RequestVoteResponse {
            from: envelope.from,
            resp,
        },
        RaftWireMessage::AppendEntriesResponse { resp } => {
            RaftInboundMessage::AppendEntriesResponse {
                from: envelope.from,
                resp,
            }
        }
    };
    Some(inbound)
}

pub(crate) fn send_wire_envelope(
    addr: &str,
    envelope: &RaftWireEnvelope,
) -> Result<(), WrongoDBError> {
    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|e| StorageError(format!("invalid raft peer address '{addr}': {e}")))?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, CONNECT_TIMEOUT)?;
    stream.set_nodelay(true)?;

    let payload = bincode::serialize(envelope)
        .map_err(|e| StorageError(format!("failed to serialize raft envelope: {e}")))?;
    if payload.len() > MAX_FRAME_SIZE {
        return Err(StorageError(format!(
            "raft frame too large: {} bytes (max {})",
            payload.len(),
            MAX_FRAME_SIZE
        ))
        .into());
    }
    let len = u32::try_from(payload.len())
        .map_err(|_| StorageError("raft frame length overflow".into()))?;
    stream.write_all(&len.to_le_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

pub(crate) fn read_wire_envelope(
    stream: &mut TcpStream,
) -> Result<RaftWireEnvelope, WrongoDBError> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let frame_len = u32::from_le_bytes(len_bytes) as usize;
    if frame_len > MAX_FRAME_SIZE {
        return Err(StorageError(format!(
            "raft frame exceeds max size: {} > {}",
            frame_len, MAX_FRAME_SIZE
        ))
        .into());
    }

    let mut payload = vec![0u8; frame_len];
    stream.read_exact(&mut payload)?;
    bincode::deserialize::<RaftWireEnvelope>(&payload)
        .map_err(|e| StorageError(format!("failed to deserialize raft envelope: {e}")).into())
}
