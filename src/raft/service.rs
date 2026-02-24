use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::core::errors::StorageError;
use crate::raft::command::RaftCommand;
use crate::raft::node::{RaftLeadershipState, RaftNodeCore};
use crate::raft::runtime::{RaftInboundMessage, RaftRuntime};
use crate::raft::transport::{
    outbound_to_wire, read_wire_envelope, send_wire_envelope, wire_to_inbound,
};
use crate::WrongoDBError;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_PROPOSAL_TIMEOUT: Duration = Duration::from_secs(5);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub(crate) struct RaftServiceConfig {
    pub(crate) local_node_id: String,
    pub(crate) local_raft_addr: Option<String>,
    pub(crate) peer_addrs: HashMap<String, String>,
    pub(crate) tick_interval: Duration,
    pub(crate) proposal_timeout: Duration,
}

impl RaftServiceConfig {
    pub(crate) fn with_defaults(
        local_node_id: String,
        local_raft_addr: Option<String>,
        peer_addrs: HashMap<String, String>,
    ) -> Self {
        Self {
            local_node_id,
            local_raft_addr,
            peer_addrs,
            tick_interval: DEFAULT_TICK_INTERVAL,
            proposal_timeout: DEFAULT_PROPOSAL_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RaftServiceHandle {
    cmd_tx: Sender<RaftServiceCommand>,
    shutdown: Arc<AtomicBool>,
    actor_join: Option<JoinHandle<()>>,
    listener_join: Option<JoinHandle<()>>,
}

impl RaftServiceHandle {
    pub(crate) fn start(node: RaftNodeCore, cfg: RaftServiceConfig) -> Result<Self, WrongoDBError> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<RaftServiceCommand>();
        let shutdown = Arc::new(AtomicBool::new(false));

        let actor_shutdown = shutdown.clone();
        let actor_cfg = cfg.clone();
        let actor_join = thread::spawn(move || run_actor(node, actor_cfg, cmd_rx, actor_shutdown));

        let listener_join = if let Some(local_addr) = cfg.local_raft_addr.clone() {
            let listener = TcpListener::bind(&local_addr).map_err(|e| {
                StorageError(format!("failed binding raft listener on {local_addr}: {e}"))
            })?;
            listener.set_nonblocking(true).map_err(|e| {
                StorageError(format!("failed setting raft listener nonblocking: {e}"))
            })?;

            let listener_tx = cmd_tx.clone();
            let listener_shutdown = shutdown.clone();
            let local_node_id = cfg.local_node_id.clone();
            Some(thread::spawn(move || {
                run_listener(
                    listener,
                    listener_tx,
                    listener_shutdown,
                    local_node_id,
                    local_addr,
                )
            }))
        } else {
            None
        };

        Ok(Self {
            cmd_tx,
            shutdown,
            actor_join: Some(actor_join),
            listener_join,
        })
    }

    pub(crate) fn propose(&self, command: RaftCommand) -> Result<u64, WrongoDBError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(RaftServiceCommand::Propose {
                command,
                reply: reply_tx,
            })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        reply_rx
            .recv()
            .map_err(|_| StorageError("raft service dropped proposal response".into()))?
    }

    pub(crate) fn leadership(&self) -> Result<RaftLeadershipState, WrongoDBError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(RaftServiceCommand::GetLeadership { reply: reply_tx })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        reply_rx
            .recv()
            .map_err(|_| StorageError("raft service dropped leadership response".into()))?
    }

    pub(crate) fn sync(&self) -> Result<(), WrongoDBError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(RaftServiceCommand::Sync { reply: reply_tx })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        reply_rx
            .recv()
            .map_err(|_| StorageError("raft service dropped sync response".into()))?
    }

    pub(crate) fn truncate_to_checkpoint(&self) -> Result<(), WrongoDBError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(RaftServiceCommand::TruncateToCheckpoint { reply: reply_tx })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        reply_rx
            .recv()
            .map_err(|_| StorageError("raft service dropped truncate response".into()))?
    }

    #[allow(dead_code)]
    pub(crate) fn force_tick(&self) -> Result<(), WrongoDBError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(RaftServiceCommand::Tick { reply: reply_tx })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        reply_rx
            .recv()
            .map_err(|_| StorageError("raft service dropped tick response".into()))?
    }

    #[allow(dead_code)]
    pub(crate) fn inject_inbound(&self, msg: RaftInboundMessage) -> Result<(), WrongoDBError> {
        self.cmd_tx
            .send(RaftServiceCommand::Inbound { msg })
            .map_err(|_| StorageError("raft service is not running".into()))?;
        Ok(())
    }

    fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.cmd_tx.send(RaftServiceCommand::Shutdown);

        if let Some(listener_join) = self.listener_join.take() {
            let _ = listener_join.join();
        }
        if let Some(actor_join) = self.actor_join.take() {
            let _ = actor_join.join();
        }
    }
}

impl Drop for RaftServiceHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug)]
enum RaftServiceCommand {
    Propose {
        command: RaftCommand,
        reply: Sender<Result<u64, WrongoDBError>>,
    },
    GetLeadership {
        reply: Sender<Result<RaftLeadershipState, WrongoDBError>>,
    },
    Sync {
        reply: Sender<Result<(), WrongoDBError>>,
    },
    TruncateToCheckpoint {
        reply: Sender<Result<(), WrongoDBError>>,
    },
    Tick {
        reply: Sender<Result<(), WrongoDBError>>,
    },
    Inbound {
        msg: RaftInboundMessage,
    },
    Shutdown,
}

type PendingProposal = (RaftCommand, Sender<Result<u64, WrongoDBError>>);

fn run_actor(
    node: RaftNodeCore,
    cfg: RaftServiceConfig,
    rx: Receiver<RaftServiceCommand>,
    shutdown: Arc<AtomicBool>,
) {
    let mut runtime = RaftRuntime::new(node);
    let mut pending_proposals: VecDeque<PendingProposal> = VecDeque::new();
    let mut running = true;

    while running {
        if let Some((command, reply)) = pending_proposals.pop_front() {
            running = process_proposal(
                &mut runtime,
                &cfg,
                &rx,
                &mut pending_proposals,
                command,
                reply,
            );
            continue;
        }

        match rx.recv_timeout(cfg.tick_interval) {
            Ok(cmd) => {
                running =
                    handle_non_proposal_command(&mut runtime, &cfg, &mut pending_proposals, cmd);
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Err(err) = tick_and_flush(&mut runtime, cfg.tick_interval, &cfg) {
                    eprintln!("raft actor tick failed: {err}");
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                running = false;
            }
        }
    }

    shutdown.store(true, Ordering::SeqCst);

    for (_, reply) in pending_proposals.drain(..) {
        let _ = reply.send(Err(StorageError("raft service shutting down".into()).into()));
    }
}

fn process_proposal(
    runtime: &mut RaftRuntime,
    cfg: &RaftServiceConfig,
    rx: &Receiver<RaftServiceCommand>,
    pending_proposals: &mut VecDeque<PendingProposal>,
    command: RaftCommand,
    reply: Sender<Result<u64, WrongoDBError>>,
) -> bool {
    let proposal_index = match runtime.propose_without_wait(&command) {
        Ok(index) => index,
        Err(err) => {
            let _ = reply.send(Err(err));
            return true;
        }
    };

    if let Err(err) = flush_outbound_and_apply(runtime, cfg) {
        let _ = reply.send(Err(err));
        return true;
    }

    let deadline = Instant::now() + cfg.proposal_timeout;
    loop {
        if runtime.is_index_committed_and_applied(proposal_index) {
            let _ = reply.send(Ok(proposal_index));
            return true;
        }

        let now = Instant::now();
        if now >= deadline {
            let _ = reply.send(Err(StorageError(format!(
                "raft proposal at index {proposal_index} timed out before quorum commit"
            ))
            .into()));
            return true;
        }

        let remaining = deadline.saturating_duration_since(now);
        let wait = cmp::min(remaining, cfg.tick_interval);
        match rx.recv_timeout(wait) {
            Ok(cmd) => match cmd {
                RaftServiceCommand::Propose { command, reply } => {
                    pending_proposals.push_back((command, reply));
                }
                RaftServiceCommand::GetLeadership { reply } => {
                    let _ = reply.send(Ok(runtime.leadership()));
                }
                RaftServiceCommand::Sync { reply } => {
                    let _ = reply.send(runtime.sync_node());
                }
                RaftServiceCommand::TruncateToCheckpoint { reply } => {
                    let _ = reply.send(runtime.truncate_to_checkpoint());
                }
                RaftServiceCommand::Tick { reply } => {
                    let res = tick_and_flush(runtime, cfg.tick_interval, cfg);
                    let _ = reply.send(res);
                }
                RaftServiceCommand::Inbound { msg } => {
                    if let Err(err) = runtime.handle_inbound(msg) {
                        eprintln!("raft actor inbound handling failed: {err}");
                    } else if let Err(err) = flush_outbound_and_apply(runtime, cfg) {
                        eprintln!("raft actor outbound flush failed: {err}");
                    }
                }
                RaftServiceCommand::Shutdown => {
                    let _ =
                        reply.send(Err(StorageError("raft service shutting down".into()).into()));
                    return false;
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                if let Err(err) = tick_and_flush(runtime, cfg.tick_interval, cfg) {
                    eprintln!("raft actor tick during proposal failed: {err}");
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = reply.send(Err(StorageError(
                    "raft service command channel closed".into(),
                )
                .into()));
                return false;
            }
        }
    }
}

fn handle_non_proposal_command(
    runtime: &mut RaftRuntime,
    cfg: &RaftServiceConfig,
    pending_proposals: &mut VecDeque<PendingProposal>,
    cmd: RaftServiceCommand,
) -> bool {
    match cmd {
        RaftServiceCommand::Propose { command, reply } => {
            pending_proposals.push_back((command, reply));
            true
        }
        RaftServiceCommand::GetLeadership { reply } => {
            let _ = reply.send(Ok(runtime.leadership()));
            true
        }
        RaftServiceCommand::Sync { reply } => {
            let _ = reply.send(runtime.sync_node());
            true
        }
        RaftServiceCommand::TruncateToCheckpoint { reply } => {
            let _ = reply.send(runtime.truncate_to_checkpoint());
            true
        }
        RaftServiceCommand::Tick { reply } => {
            let res = tick_and_flush(runtime, cfg.tick_interval, cfg);
            let _ = reply.send(res);
            true
        }
        RaftServiceCommand::Inbound { msg } => {
            if let Err(err) = runtime.handle_inbound(msg) {
                eprintln!("raft actor inbound handling failed: {err}");
            } else if let Err(err) = flush_outbound_and_apply(runtime, cfg) {
                eprintln!("raft actor outbound flush failed: {err}");
            }
            true
        }
        RaftServiceCommand::Shutdown => false,
    }
}

fn tick_and_flush(
    runtime: &mut RaftRuntime,
    _tick_interval: Duration,
    cfg: &RaftServiceConfig,
) -> Result<(), WrongoDBError> {
    runtime.tick()?;
    flush_outbound_and_apply(runtime, cfg)
}

fn flush_outbound_and_apply(
    runtime: &mut RaftRuntime,
    cfg: &RaftServiceConfig,
) -> Result<(), WrongoDBError> {
    let outbound = runtime.drain_outbound();
    for msg in outbound {
        let envelope = outbound_to_wire(&cfg.local_node_id, msg);
        let Some(peer_addr) = cfg.peer_addrs.get(&envelope.to) else {
            eprintln!(
                "raft outbound destination '{}' has no configured address, dropping message",
                envelope.to
            );
            continue;
        };
        if let Err(err) = send_wire_envelope(peer_addr, &envelope) {
            eprintln!(
                "raft outbound send to {} ({}) failed: {}",
                envelope.to, peer_addr, err
            );
        }
    }

    runtime.apply_committed_entries()?;
    Ok(())
}

fn run_listener(
    listener: TcpListener,
    cmd_tx: Sender<RaftServiceCommand>,
    shutdown: Arc<AtomicBool>,
    local_node_id: String,
    local_addr: String,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if let Err(err) = stream.set_nonblocking(false) {
                    eprintln!("raft listener failed to set accepted stream blocking mode: {err}");
                    continue;
                }
                match read_wire_envelope(&mut stream) {
                    Ok(envelope) => {
                        if let Some(inbound) = wire_to_inbound(&local_node_id, envelope) {
                            if cmd_tx
                                .send(RaftServiceCommand::Inbound { msg: inbound })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("raft listener failed reading envelope on {local_addr}: {err}");
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                thread::sleep(LISTENER_POLL_INTERVAL);
            }
            Err(err) => {
                eprintln!("raft listener accept error on {local_addr}: {err}");
                thread::sleep(LISTENER_POLL_INTERVAL);
            }
        }
    }
}
