// ============================================================================
// Reserved Store Constants
// ============================================================================

pub(crate) const METADATA_URI: &str = "metadata:";
pub(crate) const METADATA_STORE_NAME: &str = "metadata.wt";
pub(crate) const CATALOG_URI: &str = "catalog:";
pub(crate) const CATALOG_STORE_NAME: &str = "_catalog.wt";

const RESERVED_STORE_NAMES: [&str; 2] = [METADATA_STORE_NAME, CATALOG_STORE_NAME];

// ============================================================================
// Helpers
// ============================================================================

/// Resolves the hardcoded storage file for one reserved URI.
///
/// Reserved stores bootstrap the engine and therefore cannot depend on
/// metadata lookup to find their own backing files.
pub(crate) fn reserved_store_name_for_uri(uri: &str) -> Option<&'static str> {
    match uri {
        METADATA_URI => Some(METADATA_STORE_NAME),
        CATALOG_URI => Some(CATALOG_STORE_NAME),
        _ => None,
    }
}

/// Returns the set of reserved store files that always exist outside
/// `metadata.wt` row lookup.
pub(crate) fn reserved_store_names() -> &'static [&'static str] {
    &RESERVED_STORE_NAMES
}
