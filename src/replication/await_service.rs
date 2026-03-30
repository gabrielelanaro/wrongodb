use std::sync::Arc;

use tokio::sync::watch;

use super::OpTime;

/// Process-local wakeup service for oplog awaitData cursors.
///
/// The service tracks the highest committed oplog index visible on this node
/// and lets waiting `getMore` calls sleep until that index advances.
#[derive(Debug, Clone)]
pub(crate) struct OplogAwaitService {
    sender: Arc<watch::Sender<u64>>,
}

impl OplogAwaitService {
    /// Build the service with the latest durable oplog index known at startup.
    pub(crate) fn new(latest_index: u64) -> Self {
        let (sender, _) = watch::channel(latest_index);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// Subscribe to oplog advancement notifications.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }

    /// Return the latest committed oplog index known to the service.
    pub(crate) fn latest_index(&self) -> u64 {
        *self.sender.borrow()
    }

    /// Wake waiters after a committed oplog append becomes visible.
    pub(crate) fn notify_committed(&self, op_time: OpTime) {
        if op_time.index <= self.latest_index() {
            return;
        }

        let _ = self.sender.send(op_time.index);
    }
}
