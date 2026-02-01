use super::transaction::Transaction;
use super::{Timestamp, TxnId, TS_MAX, TS_NONE, TXN_ABORTED};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateType {
    Standard,
    Tombstone,
    Reserve,
}

#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start_ts: Timestamp,
    pub durable_start_ts: Timestamp,
    pub start_txn: TxnId,
    pub stop_ts: Timestamp,
    pub durable_stop_ts: Timestamp,
    pub stop_txn: TxnId,
    pub prepared: bool,
}

impl TimeWindow {
    pub fn new(start_txn: TxnId) -> Self {
        Self {
            start_ts: TS_NONE,
            durable_start_ts: TS_NONE,
            start_txn,
            stop_ts: TS_MAX,
            durable_stop_ts: TS_MAX,
            stop_txn: TXN_ABORTED,
            prepared: false,
        }
    }
}

#[derive(Debug)]
pub struct Update {
    pub txn_id: TxnId,
    pub time_window: TimeWindow,
    pub next: Option<Box<Update>>,
    pub type_: UpdateType,
    pub data: Vec<u8>,
}

impl Update {
    pub fn new(txn_id: TxnId, update_type: UpdateType, data: Vec<u8>) -> Self {
        Self {
            txn_id,
            time_window: TimeWindow::new(txn_id),
            next: None,
            type_: update_type,
            data,
        }
    }
}

#[derive(Debug, Default)]
pub struct UpdateChain {
    head: Option<Box<Update>>,
}

impl UpdateChain {
    pub fn prepend(&mut self, mut update: Update) {
        update.next = self.head.take();
        self.head = Some(Box::new(update));
    }

    pub fn find_visible<'a>(&'a self, txn: &Transaction) -> Option<&'a Update> {
        let mut current = self.head.as_deref();
        while let Some(update) = current {
            if txn.can_see(update) {
                if update.type_ == UpdateType::Reserve {
                    current = update.next.as_deref();
                    continue;
                }
                return Some(update);
            }
            current = update.next.as_deref();
        }
        None
    }
}
