use super::global_txn::GlobalTxnState;
use super::snapshot::Snapshot;
use super::update::Update;
use super::{Timestamp, TxnId, TS_NONE, TXN_NONE};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IsolationLevel {
    Snapshot,
}

#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    isolation: IsolationLevel,
    snapshot: Option<Snapshot>,
    read_ts: Option<Timestamp>,
}

impl Transaction {
    pub fn new(isolation: IsolationLevel) -> Self {
        Self {
            id: TXN_NONE,
            isolation,
            snapshot: None,
            read_ts: None,
        }
    }

    pub(crate) fn new_with_snapshot(
        id: TxnId,
        isolation: IsolationLevel,
        snapshot: Snapshot,
    ) -> Self {
        Self {
            id,
            isolation,
            snapshot: Some(snapshot),
            read_ts: None,
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn set_read_ts(&mut self, ts: Timestamp) {
        if ts == TS_NONE {
            self.read_ts = None;
        } else {
            self.read_ts = Some(ts);
        }
    }

    pub fn ensure_snapshot(&mut self, global: &GlobalTxnState) {
        if self.id == TXN_NONE {
            self.id = global.allocate_txn_id();
            global.register_active(self.id);
        }

        if self.snapshot.is_none() && self.isolation == IsolationLevel::Snapshot {
            self.snapshot = Some(global.take_snapshot(self.id));
        }
    }

    pub fn end(&mut self, global: &GlobalTxnState) {
        if self.id != TXN_NONE {
            global.unregister_active(self.id);
        }
    }

    pub fn can_see(&self, update: &Update) -> bool {
        let visible_txn = match &self.snapshot {
            Some(snapshot) => snapshot.is_visible(update.txn_id),
            None => true,
        };
        if !visible_txn {
            return false;
        }

        let Some(read_ts) = self.read_ts else {
            return true;
        };
        update.time_window.start_ts <= read_ts && read_ts < update.time_window.stop_ts
    }
}
