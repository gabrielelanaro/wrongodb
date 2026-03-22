// ============================================================================
// Reserved Store Constants
// ============================================================================

pub(crate) type StoreId = u64;

pub(crate) const METADATA_URI: &str = "metadata:";
pub(crate) const METADATA_STORE_NAME: &str = "metadata.wt";
pub(crate) const METADATA_STORE_ID: StoreId = 0;
pub(crate) const FIRST_DYNAMIC_STORE_ID: StoreId = 1;

const RESERVED_STORE_NAMES: [&str; 1] = [METADATA_STORE_NAME];

// ============================================================================
// Helpers
// ============================================================================

pub(crate) fn reserved_store_name_for_id(store_id: StoreId) -> Option<&'static str> {
    match store_id {
        METADATA_STORE_ID => Some(METADATA_STORE_NAME),
        _ => None,
    }
}

pub(crate) fn reserved_store_identity_for_uri(uri: &str) -> Option<(StoreId, &'static str)> {
    match uri {
        METADATA_URI => Some((METADATA_STORE_ID, METADATA_STORE_NAME)),
        _ => None,
    }
}

/// Returns the set of reserved store files that always exist outside
/// `metadata.wt` row lookup.
pub(crate) fn reserved_store_names() -> &'static [&'static str] {
    &RESERVED_STORE_NAMES
}
