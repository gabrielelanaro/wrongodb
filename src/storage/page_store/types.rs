use super::page::Page;

// ============================================================================
// ReadPin - Read-only pin token
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadPin {
    pub(crate) page_id: u64,
}

impl ReadPin {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn page_id(&self) -> u64 {
        self.page_id
    }
}

// ============================================================================
// PageEdit - Owned mutable page edit (copy-on-write)
// ============================================================================

#[derive(Debug)]
pub(crate) struct PageEdit {
    pub(crate) page_id: u64,
    pub(crate) original_page_id: Option<u64>,
    pub(crate) page: Page,
}

impl PageEdit {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn page_id(&self) -> u64 {
        self.page_id
    }

    pub(crate) fn page(&self) -> &Page {
        &self.page
    }

    pub(crate) fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}
