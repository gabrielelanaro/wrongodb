// ============================================================================
// PinnedPage - Read-only pinned page handle
// ============================================================================

#[derive(Debug)]
pub struct PinnedPage {
    pub(crate) page_id: u64,
    pub(crate) payload: Vec<u8>,
}

impl PinnedPage {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
}

// ============================================================================
// PinnedPageMut - Mutable pinned page handle (copy-on-write)
// ============================================================================

#[derive(Debug)]
pub struct PinnedPageMut {
    pub(crate) page_id: u64,
    pub(crate) payload: Vec<u8>,
    pub(crate) original_page_id: Option<u64>,
}

impl PinnedPageMut {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
}
