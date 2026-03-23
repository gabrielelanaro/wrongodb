mod cursor;
mod internal_ops;
mod iter;
mod layout;
mod leaf_ops;
mod page;
mod search;

pub(crate) use cursor::BTreeCursor;

use crate::storage::mvcc::{UpdateChain, UpdateType};
use crate::txn::ReadVisibility;

// ============================================================================
// Type Aliases (shared across btree module)
// ============================================================================

pub(super) type Key = Vec<u8>;
pub(super) type Value = Vec<u8>;
pub(super) type KeyValuePair = (Key, Value);
pub(super) type KeyChildId = (Key, u64);
pub(super) type LeafEntries = Vec<KeyValuePair>;
pub(super) type InternalEntries = (u64, Vec<KeyChildId>);

// ============================================================================
// Helper Functions
// ============================================================================

/// Resolves the visible value from an update chain for a given read visibility.
///
/// Returns `Some(Some(value))` if a visible standard update is found,
/// `Some(None)` if a visible tombstone is found (key exists but deleted),
/// or `None` if no visible update exists in the chain.
pub(super) fn visible_chain_value(
    chain: &UpdateChain,
    visibility: &ReadVisibility,
) -> Option<Option<Vec<u8>>> {
    for update_ref in chain.iter() {
        let update = update_ref.read();
        if !visibility.can_see(&update) {
            continue;
        }

        return match update.type_ {
            UpdateType::Standard => Some(Some(update.data.clone())),
            UpdateType::Tombstone => Some(None),
            UpdateType::Reserve => continue,
        };
    }

    None
}
