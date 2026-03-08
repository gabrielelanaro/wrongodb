use super::page::Page;

// ============================================================================
// ReadPin - Read-only pin token
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPin {
    pub(crate) page_id: u64,
}

impl ReadPin {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn page_id(&self) -> u64 {
        self.page_id
    }
}

// ============================================================================
// PageEdit - Owned mutable page edit (copy-on-write)
// ============================================================================

#[derive(Debug)]
pub struct PageEdit {
    pub(crate) page_id: u64,
    pub(crate) original_page_id: Option<u64>,
    pub(crate) page: Page,
}

impl PageEdit {
    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    #[allow(dead_code)]
    pub fn original_page_id(&self) -> Option<u64> {
        self.original_page_id
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}
