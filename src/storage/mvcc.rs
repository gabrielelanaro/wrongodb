/// Reconciliation accounting for page-local MVCC materialization.
///
/// The checkpoint path reports how many committed entries were written into the
/// base tree, how many obsolete history nodes were pruned, and how many update
/// chains were dropped entirely after reconciliation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconcileStats {
    pub(crate) materialized_entries: usize,
    pub(crate) obsolete_updates_removed: usize,
    pub(crate) chains_dropped: usize,
}
