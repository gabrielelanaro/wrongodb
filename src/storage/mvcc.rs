use std::collections::HashMap;

use crate::txn::{TxnId, UpdateChain, UpdateType};

pub(crate) type MaterializedEntry = (Vec<u8>, UpdateType, Vec<u8>);
pub(crate) type MaterializedEntries = Vec<MaterializedEntry>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconcileStats {
    pub(crate) materialized_entries: usize,
    pub(crate) obsolete_updates_removed: usize,
    pub(crate) chains_dropped: usize,
}

/// MVCC state manager for a single store.
///
/// Maintains update chains for each key, supporting snapshot isolation
/// and reconciliation for checkpointing.
#[derive(Debug)]
pub struct MvccState {
    chains: HashMap<Vec<u8>, UpdateChain>,
}

impl MvccState {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
        }
    }

    // ------------------------------------------------------------------------
    // Chain Access
    // ------------------------------------------------------------------------

    pub fn chain(&self, key: &[u8]) -> Option<&UpdateChain> {
        self.chains.get(key)
    }

    pub fn chain_mut_or_create(&mut self, key: &[u8]) -> &mut UpdateChain {
        self.chains.entry(key.to_vec()).or_default()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UpdateChain> {
        self.chains.values_mut()
    }

    // ------------------------------------------------------------------------
    // Checkpoint Reconciliation
    // ------------------------------------------------------------------------

    pub fn reconcile_for_checkpoint(
        &mut self,
        oldest_active_txn_id: TxnId,
        no_active_txns: bool,
    ) -> (MaterializedEntries, ReconcileStats) {
        let mut entries_to_materialize = Vec::new();
        let mut stats = ReconcileStats::default();
        let mut keys_to_remove = Vec::new();

        for (key, chain) in self.chains.iter_mut() {
            if let Some((update_type, data)) = chain.latest_committed_entry() {
                entries_to_materialize.push((key.clone(), update_type, data));
                stats.materialized_entries += 1;
            }

            stats.obsolete_updates_removed += chain.truncate_obsolete(oldest_active_txn_id);
            if chain.clear_if_materialized_current(no_active_txns) || chain.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }

        stats.chains_dropped = keys_to_remove.len();
        for key in keys_to_remove {
            self.chains.remove(&key);
        }

        (entries_to_materialize, stats)
    }

    // ------------------------------------------------------------------------
    // Key Range Queries
    // ------------------------------------------------------------------------

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
}
