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
use crate::raft::command::{CommittedCommand, RaftCommand};
use crate::raft::node::{RaftLeadershipState, RaftNodeCore};
use crate::raft::runtime::{RaftInboundMessage, RaftRuntime};
use crate::raft::transport::{
    outbound_to_wire, read_wire_envelope, send_wire_envelope, wire_to_inbound,
};
use crate::WrongoDBError;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_PROPOSAL_TIMEOUT: Duration = Duration::from_secs(5);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) trait CommittedCommandExecutor: Send + Sync + std::fmt::Debug {
    fn execute(&self, cmd: CommittedCommand) -> Result<(), WrongoDBError>;
}

#[derive(Debug)]
struct NoopCommittedCommandExecutor;

impl CommittedCommandExecutor for NoopCommittedCommandExecutor {
    fn execute(&self, _cmd: CommittedCommand) -> Result<(), WrongoDBError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RaftServiceConfig {
    pub(crate) local_node_id: String,
    pub(crate) local_raft_addr: Option<String>,
    pub(crate) peer_addrs: HashMap<String, String>,
    pub(crate) tick_interval: Duration,
    pub(crate) proposal_timeout: Duration,
    pub(crate) executor: Arc<dyn CommittedCommandExecutor>,
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
            executor: Arc::new(NoopCommittedCommandExecutor),
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

#[derive(Debug, Default)]
struct ActorState {
    executor_applied_index: u64,
    fatal_reason: Option<String>,
}

fn fatal_error(reason: &str) -> WrongoDBError {
    StorageError(format!("raft fatal apply: {reason}")).into()
}

fn current_fatal_error(state: &ActorState) -> Option<WrongoDBError> {
    state.fatal_reason.as_deref().map(fatal_error)
}

fn set_fatal_error(state: &mut ActorState, err: WrongoDBError) -> WrongoDBError {
    if state.fatal_reason.is_none() {
        state.fatal_reason = Some(err.to_string());
    }
    fatal_error(
        state
            .fatal_reason
            .as_deref()
            .expect("fatal reason should be set"),
    )
}

fn run_actor(
    node: RaftNodeCore,
    cfg: RaftServiceConfig,
    rx: Receiver<RaftServiceCommand>,
    shutdown: Arc<AtomicBool>,
) {
    let mut runtime = RaftRuntime::new(node);
    let mut state = ActorState {
        executor_applied_index: runtime.applied_index(),
        ..ActorState::default()
    };
    let mut pending_proposals: VecDeque<PendingProposal> = VecDeque::new();
    let mut running = true;

    while running {
        if let Some(reason) = state.fatal_reason.as_deref() {
            for (_, reply) in pending_proposals.drain(..) {
                let _ = reply.send(Err(fatal_error(reason)));
            }
        }

        if let Some((command, reply)) = pending_proposals.pop_front() {
            running = process_proposal(
                &mut runtime,
                &cfg,
                &rx,
                &mut pending_proposals,
                &mut state,
                command,
                reply,
            );
            continue;
        }

        match rx.recv_timeout(cfg.tick_interval) {
            Ok(cmd) => {
                running = handle_non_proposal_command(
                    &mut runtime,
                    &cfg,
                    &mut pending_proposals,
                    &mut state,
                    cmd,
                );
            }
            Err(RecvTimeoutError::Timeout) => {
                if state.fatal_reason.is_some() {
                    continue;
                }
                if let Err(err) = tick_and_flush(&mut runtime, cfg.tick_interval, &cfg, &mut state)
                {
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
    state: &mut ActorState,
    command: RaftCommand,
    reply: Sender<Result<u64, WrongoDBError>>,
) -> bool {
    if let Some(err) = current_fatal_error(state) {
        let _ = reply.send(Err(err));
        return true;
    }

    let proposal_index = match runtime.propose_without_wait(&command) {
        Ok(index) => index,
        Err(err) => {
            let _ = tick_and_flush(runtime, cfg.tick_interval, cfg, state);
            let _ = reply.send(Err(err));
            return true;
        }
    };

    if let Err(err) = flush_outbound_and_apply(runtime, cfg, state) {
        let _ = reply.send(Err(err));
        return true;
    }

    let deadline = Instant::now() + cfg.proposal_timeout;
    loop {
        if let Some(err) = current_fatal_error(state) {
            let _ = reply.send(Err(err));
            return true;
        }

        if runtime.is_index_committed(proposal_index)
            && state.executor_applied_index >= proposal_index
        {
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
                    if let Some(err) = current_fatal_error(state) {
                        let _ = reply.send(Err(err));
                    } else {
                        let _ = reply.send(Ok(runtime.leadership()));
                    }
                }
                RaftServiceCommand::Sync { reply } => {
                    if let Some(err) = current_fatal_error(state) {
                        let _ = reply.send(Err(err));
                    } else {
                        let _ = reply.send(runtime.sync_node());
                    }
                }
                RaftServiceCommand::TruncateToCheckpoint { reply } => {
                    if let Some(err) = current_fatal_error(state) {
                        let _ = reply.send(Err(err));
                    } else {
                        let _ = reply.send(runtime.truncate_to_checkpoint());
                    }
                }
                RaftServiceCommand::Tick { reply } => {
                    let res = tick_and_flush(runtime, cfg.tick_interval, cfg, state);
                    let _ = reply.send(res);
                }
                RaftServiceCommand::Inbound { msg } => {
                    if state.fatal_reason.is_some() {
                        continue;
                    }
                    if let Err(err) = runtime.handle_inbound(msg) {
                        eprintln!("raft actor inbound handling failed: {err}");
                    } else if let Err(err) = flush_outbound_and_apply(runtime, cfg, state) {
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
                if let Err(err) = tick_and_flush(runtime, cfg.tick_interval, cfg, state) {
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
    state: &mut ActorState,
    cmd: RaftServiceCommand,
) -> bool {
    match cmd {
        RaftServiceCommand::Propose { command, reply } => {
            if let Some(err) = current_fatal_error(state) {
                let _ = reply.send(Err(err));
            } else {
                pending_proposals.push_back((command, reply));
            }
            true
        }
        RaftServiceCommand::GetLeadership { reply } => {
            if let Some(err) = current_fatal_error(state) {
                let _ = reply.send(Err(err));
            } else {
                let _ = reply.send(Ok(runtime.leadership()));
            }
            true
        }
        RaftServiceCommand::Sync { reply } => {
            if let Some(err) = current_fatal_error(state) {
                let _ = reply.send(Err(err));
            } else {
                let _ = reply.send(runtime.sync_node());
            }
            true
        }
        RaftServiceCommand::TruncateToCheckpoint { reply } => {
            if let Some(err) = current_fatal_error(state) {
                let _ = reply.send(Err(err));
            } else {
                let _ = reply.send(runtime.truncate_to_checkpoint());
            }
            true
        }
        RaftServiceCommand::Tick { reply } => {
            let res = tick_and_flush(runtime, cfg.tick_interval, cfg, state);
            let _ = reply.send(res);
            true
        }
        RaftServiceCommand::Inbound { msg } => {
            if state.fatal_reason.is_none() {
                if let Err(err) = runtime.handle_inbound(msg) {
                    eprintln!("raft actor inbound handling failed: {err}");
                } else if let Err(err) = flush_outbound_and_apply(runtime, cfg, state) {
                    eprintln!("raft actor outbound flush failed: {err}");
                }
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
    state: &mut ActorState,
) -> Result<(), WrongoDBError> {
    if let Some(err) = current_fatal_error(state) {
        return Err(err);
    }
    runtime.tick()?;
    flush_outbound_and_apply(runtime, cfg, state)
}

fn flush_outbound_and_apply(
    runtime: &mut RaftRuntime,
    cfg: &RaftServiceConfig,
    state: &mut ActorState,
) -> Result<(), WrongoDBError> {
    if let Some(err) = current_fatal_error(state) {
        return Err(err);
    }

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
    for applied in runtime.drain_committed_commands() {
        if let Err(err) = cfg.executor.execute(applied.clone()) {
            return Err(set_fatal_error(state, err));
        }
        state.executor_applied_index = applied.index;
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::*;
    use crate::core::errors::StorageError;

    #[derive(Debug)]
    struct DelayExecutor {
        delay: Duration,
    }

    impl CommittedCommandExecutor for DelayExecutor {
        fn execute(&self, _cmd: CommittedCommand) -> Result<(), WrongoDBError> {
            std::thread::sleep(self.delay);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailFirstExecutor {
        failed_once: AtomicBool,
    }

    impl CommittedCommandExecutor for FailFirstExecutor {
        fn execute(&self, _cmd: CommittedCommand) -> Result<(), WrongoDBError> {
            if !self.failed_once.swap(true, Ordering::SeqCst) {
                return Err(StorageError("injected apply failure".into()).into());
            }
            Ok(())
        }
    }

    #[test]
    fn proposal_ack_waits_for_executor_apply() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open(dir.path()).unwrap();
        let delay = Duration::from_millis(150);

        let mut cfg = RaftServiceConfig::with_defaults("n1".to_string(), None, HashMap::new());
        cfg.proposal_timeout = Duration::from_secs(2);
        cfg.executor = Arc::new(DelayExecutor { delay });

        let service = RaftServiceHandle::start(node, cfg).unwrap();
        let start = Instant::now();
        let index = service
            .propose(RaftCommand::Put {
                store_name: "users.main.wt".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(index, 1);
        assert!(
            elapsed >= delay,
            "proposal returned before executor apply delay elapsed: {elapsed:?}"
        );
    }

    #[test]
    fn apply_failure_enters_fail_stop() {
        let dir = tempdir().unwrap();
        let node = RaftNodeCore::open(dir.path()).unwrap();

        let mut cfg = RaftServiceConfig::with_defaults("n1".to_string(), None, HashMap::new());
        cfg.proposal_timeout = Duration::from_secs(2);
        cfg.executor = Arc::new(FailFirstExecutor {
            failed_once: AtomicBool::new(false),
        });

        let service = RaftServiceHandle::start(node, cfg).unwrap();
        let err = service
            .propose(RaftCommand::Put {
                store_name: "users.main.wt".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                txn_id: 1,
            })
            .unwrap_err();
        assert!(err.to_string().contains("raft fatal apply"));

        let err = service
            .propose(RaftCommand::Put {
                store_name: "users.main.wt".to_string(),
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                txn_id: 1,
            })
            .unwrap_err();
        assert!(err.to_string().contains("raft fatal apply"));

        let err = service.leadership().unwrap_err();
        assert!(err.to_string().contains("raft fatal apply"));
        let err = service.sync().unwrap_err();
        assert!(err.to_string().contains("raft fatal apply"));
        let err = service.truncate_to_checkpoint().unwrap_err();
        assert!(err.to_string().contains("raft fatal apply"));
    }
}
