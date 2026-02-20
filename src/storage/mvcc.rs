use std::collections::HashMap;
use std::sync::Arc;

use crate::txn::{GlobalTxnState, UpdateChain, UpdateType, TS_NONE, TXN_ABORTED};

#[derive(Debug)]
pub struct MvccState {
    global: Arc<GlobalTxnState>,
    chains: HashMap<Vec<u8>, UpdateChain>,
}

impl MvccState {
    pub fn new(global: Arc<GlobalTxnState>) -> Self {
        Self {
            global,
            chains: HashMap::new(),
        }
    }

    pub fn chain(&self, key: &[u8]) -> Option<&UpdateChain> {
        self.chains.get(key)
    }

    pub fn chain_mut_or_create(&mut self, key: &[u8]) -> &mut UpdateChain {
        self.chains.entry(key.to_vec()).or_default()
    }

    /// Run garbage collection on all update chains.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        let threshold = self.global.oldest_active_txn_id();
        let mut chains_cleaned = 0;
        let mut updates_removed = 0;
        let mut keys_to_remove = Vec::new();

        for (key, chain) in self.chains.iter_mut() {
            let removed = chain.truncate_obsolete(threshold);
            if removed > 0 {
                chains_cleaned += 1;
                updates_removed += removed;
            }
            if chain.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }

        let chains_dropped = keys_to_remove.len();
        for key in keys_to_remove {
            self.chains.remove(&key);
        }

        (chains_cleaned, updates_removed, chains_dropped)
    }

    #[allow(dead_code)]
    pub fn chain_count(&self) -> usize {
        self.chains.len()
    }

    pub fn keys_in_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self.chains.keys().cloned().collect();
        if start.is_some() || end.is_some() {
            keys.retain(|key| {
                let after_start = start.is_none_or(|s| key.as_slice() >= s);
                let before_end = end.is_none_or(|e| key.as_slice() < e);
                after_start && before_end
            });
        }
        keys.sort();
        keys
    }

    pub fn latest_committed_entries(&self) -> Vec<(Vec<u8>, UpdateType, Vec<u8>)> {
        let mut out = Vec::new();
        for (key, chain) in self.chains.iter() {
            for update in chain.iter() {
                let is_aborted = update.time_window.stop_txn == TXN_ABORTED
                    && update.time_window.stop_ts == TS_NONE;
                if is_aborted {
                    continue;
                }
                if update.time_window.start_ts == TS_NONE {
                    continue;
                }
                match update.type_ {
                    UpdateType::Standard => {
                        out.push((key.clone(), UpdateType::Standard, update.data.clone()))
                    }
                    UpdateType::Tombstone => {
                        out.push((key.clone(), UpdateType::Tombstone, Vec::new()))
                    }
                    UpdateType::Reserve => {}
                }
                break;
            }
        }
        out
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UpdateChain> {
        self.chains.values_mut()
    }
}
