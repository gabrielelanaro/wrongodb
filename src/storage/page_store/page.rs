use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

use crate::storage::mvcc::UpdateChain;

// ============================================================================
// Constants
// ============================================================================

const PAGE_TYPE_LEAF_TAG: u8 = 1;
const PAGE_TYPE_INTERNAL_TAG: u8 = 2;

const HDR_PAGE_TYPE: usize = 0;
const HDR_FLAGS: usize = 1;
const HDR_SLOT_COUNT: usize = 2;
const HDR_LOWER: usize = 4;
const HDR_UPPER: usize = 6;
const HDR_FIRST_CHILD: usize = 8;

const LEAF_HEADER_SIZE: usize = 8;
const INTERNAL_HEADER_SIZE: usize = 16;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PageError {
    #[error("page corrupt: {0}")]
    Corrupt(String),
}

// ============================================================================
// Page Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Leaf,
    Internal,
}

impl PageType {
    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn from_tag(tag: u8) -> Result<Self, PageError> {
        match tag {
            PAGE_TYPE_LEAF_TAG => Ok(Self::Leaf),
            PAGE_TYPE_INTERNAL_TAG => Ok(Self::Internal),
            other => Err(PageError::Corrupt(format!("unknown page type: {other}"))),
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Leaf => PAGE_TYPE_LEAF_TAG,
            Self::Internal => PAGE_TYPE_INTERNAL_TAG,
        }
    }
}

// ============================================================================
// PageHeader
// ============================================================================

/// Cached header fields for an in-memory page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: PageType,
    pub flags: u8,
    pub slot_count: u16,
    pub lower: u16,
    pub upper: u16,
    pub first_child: u64,
}

impl PageHeader {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn parse(data: &[u8]) -> Result<Self, PageError> {
        if data.len() < LEAF_HEADER_SIZE {
            return Err(PageError::Corrupt(format!(
                "page too small for header: {}",
                data.len()
            )));
        }

        let page_type = PageType::from_tag(read_u8(data, HDR_PAGE_TYPE)?)?;
        let flags = read_u8(data, HDR_FLAGS)?;
        let slot_count = read_u16(data, HDR_SLOT_COUNT)?;
        let lower = read_u16(data, HDR_LOWER)?;
        let upper = read_u16(data, HDR_UPPER)?;
        let first_child = match page_type {
            PageType::Leaf => 0,
            PageType::Internal => {
                if data.len() < INTERNAL_HEADER_SIZE {
                    return Err(PageError::Corrupt(format!(
                        "internal page too small for header: {}",
                        data.len()
                    )));
                }
                read_u64(data, HDR_FIRST_CHILD)?
            }
        };

        Ok(Self {
            page_type,
            flags,
            slot_count,
            lower,
            upper,
            first_child,
        })
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    #[cfg(test)]
    pub fn header_size(&self) -> usize {
        match self.page_type {
            PageType::Leaf => LEAF_HEADER_SIZE,
            PageType::Internal => INTERNAL_HEADER_SIZE,
        }
    }

    pub fn free_contiguous(&self) -> usize {
        usize::from(self.upper).saturating_sub(usize::from(self.lower))
    }
}

// ============================================================================
// Row-Store Modify State
// ============================================================================

/// Inserted row that exists only in the in-memory page state.
///
/// The key is not materialized in the raw page image yet. Its update chain
/// holds the visible versions for that inserted row until a later reconciliation
/// writes it into the serialized page format.
#[derive(Debug, Clone)]
pub struct RowInsert {
    key: Vec<u8>,
    updates: UpdateChain,
}

impl RowInsert {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(key: Vec<u8>, updates: UpdateChain) -> Self {
        Self { key, updates }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn updates(&self) -> &UpdateChain {
        &self.updates
    }

    pub(crate) fn updates_mut(&mut self) -> &mut UpdateChain {
        &mut self.updates
    }
}

/// Runtime row-store modify state associated with an in-memory page.
///
/// This mirrors the WT split between updates for rows already materialized in
/// the page image and inserted rows that only exist in memory.
///
/// `row_updates` is indexed by leaf slot, not by key. That shape is deliberate:
/// for an on-page row, the slot is already the stable identity after leaf-page
/// search resolves the key to a slot position. Most slots have no in-memory
/// updates, so each entry is `Option<UpdateChain>`:
///
/// - `None`: the materialized row has no runtime update chain
/// - `Some(chain)`: the materialized row is shadowed by a page-local chain
///
/// A `HashMap` or skip list would add an unnecessary second lookup structure
/// for existing rows. Ordered/search-oriented structures belong to
/// `row_inserts`, where keys do not yet have an on-page slot.
#[derive(Debug, Clone)]
pub struct RowModify {
    row_updates: Vec<Option<UpdateChain>>,
    row_inserts: Vec<Vec<RowInsert>>,
}

impl RowModify {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(slot_count: usize) -> Self {
        let row_updates = (0..slot_count).map(|_| None).collect();
        let row_inserts = vec![Vec::new(); slot_count + 1];
        Self {
            row_updates,
            row_inserts,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn row_updates(&self) -> &[Option<UpdateChain>] {
        &self.row_updates
    }

    pub(crate) fn row_updates_mut(&mut self) -> &mut [Option<UpdateChain>] {
        &mut self.row_updates
    }

    pub fn row_inserts(&self) -> &[Vec<RowInsert>] {
        &self.row_inserts
    }

    pub(crate) fn row_inserts_mut(&mut self) -> &mut [Vec<RowInsert>] {
        &mut self.row_inserts
    }
}

// ============================================================================
// RawPage
// ============================================================================

/// Raw serialized page image plus cached parsed header.
///
/// `RawPage` corresponds to the byte-oriented page representation used for
/// block I/O and page-format parsing. It intentionally contains no runtime
/// MVCC or cache state.
#[derive(Debug, Clone)]
pub struct RawPage {
    header: PageHeader,
    data: Vec<u8>,
}

impl RawPage {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn from_bytes(data: Vec<u8>) -> Result<Self, PageError> {
        let header = PageHeader::parse(&data)?;
        Ok(Self { header, data })
    }

    pub(crate) fn new_leaf(page_len: usize) -> Result<Self, PageError> {
        validate_page_len(page_len, LEAF_HEADER_SIZE)?;

        let mut data = vec![0u8; page_len];
        write_u8(&mut data, HDR_PAGE_TYPE, PageType::Leaf.tag())?;
        write_u8(&mut data, HDR_FLAGS, 0)?;
        write_u16(&mut data, HDR_SLOT_COUNT, 0)?;
        write_u16(&mut data, HDR_LOWER, LEAF_HEADER_SIZE as u16)?;
        write_u16(&mut data, HDR_UPPER, page_len as u16)?;

        Self::from_bytes(data)
    }

    pub(crate) fn new_internal(page_len: usize, first_child: u64) -> Result<Self, PageError> {
        validate_page_len(page_len, INTERNAL_HEADER_SIZE)?;

        let mut data = vec![0u8; page_len];
        write_u8(&mut data, HDR_PAGE_TYPE, PageType::Internal.tag())?;
        write_u8(&mut data, HDR_FLAGS, 0)?;
        write_u16(&mut data, HDR_SLOT_COUNT, 0)?;
        write_u16(&mut data, HDR_LOWER, INTERNAL_HEADER_SIZE as u16)?;
        write_u16(&mut data, HDR_UPPER, page_len as u16)?;
        write_u64(&mut data, HDR_FIRST_CHILD, first_child)?;

        Self::from_bytes(data)
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub(crate) fn refresh_header(&mut self) -> Result<(), PageError> {
        self.header = PageHeader::parse(&self.data)?;
        Ok(())
    }

    pub(crate) fn set_flags(&mut self, flags: u8) -> Result<(), PageError> {
        write_u8(&mut self.data, HDR_FLAGS, flags)?;
        self.header.flags = flags;
        Ok(())
    }

    pub(crate) fn set_slot_count(&mut self, slot_count: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_SLOT_COUNT, slot_count)?;
        self.header.slot_count = slot_count;
        Ok(())
    }

    pub(crate) fn set_lower(&mut self, lower: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_LOWER, lower)?;
        self.header.lower = lower;
        Ok(())
    }

    pub(crate) fn set_upper(&mut self, upper: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_UPPER, upper)?;
        self.header.upper = upper;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_first_child(&mut self, first_child: u64) -> Result<(), PageError> {
        if self.header.page_type != PageType::Internal {
            return Err(PageError::Corrupt(
                "first_child is only valid for internal pages".into(),
            ));
        }
        write_u64(&mut self.data, HDR_FIRST_CHILD, first_child)?;
        self.header.first_child = first_child;
        Ok(())
    }
}

// ============================================================================
// Page
// ============================================================================

/// In-memory page object combining a raw page image with runtime modify state.
///
/// `Page` is the object carried by the page cache and edit path. The serialized
/// bytes remain in [`RawPage`], while page-local row updates and inserts live in
/// `row_modify`.
#[derive(Debug)]
pub struct Page {
    raw: RawPage,
    row_modify: Option<RowModify>,
}

impl Page {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn from_raw(raw: RawPage) -> Self {
        Self {
            raw,
            row_modify: None,
        }
    }

    pub fn from_bytes(data: Vec<u8>) -> Result<Self, PageError> {
        Ok(Self::from_raw(RawPage::from_bytes(data)?))
    }

    pub fn new_leaf(page_len: usize) -> Result<Self, PageError> {
        Ok(Self::from_raw(RawPage::new_leaf(page_len)?))
    }

    pub fn new_internal(page_len: usize, first_child: u64) -> Result<Self, PageError> {
        Ok(Self::from_raw(RawPage::new_internal(
            page_len,
            first_child,
        )?))
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn clone_for_edit(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            row_modify: self.row_modify.clone(),
        }
    }

    pub(crate) fn raw_mut(&mut self) -> &mut RawPage {
        &mut self.raw
    }

    pub fn header(&self) -> &PageHeader {
        self.raw.header()
    }

    pub fn data(&self) -> &[u8] {
        self.raw.data()
    }

    pub(crate) fn row_modify(&self) -> Option<&RowModify> {
        self.row_modify.as_ref()
    }

    pub(crate) fn row_modify_mut(&mut self) -> Option<&mut RowModify> {
        self.row_modify.as_mut()
    }

    pub(crate) fn clear_row_modify(&mut self) {
        self.row_modify = None;
    }

    pub(crate) fn ensure_row_modify(&mut self) -> Result<&mut RowModify, PageError> {
        if self.header().page_type != PageType::Leaf {
            return Err(PageError::Corrupt(
                "row modify state is only valid for leaf pages".into(),
            ));
        }

        let slot_count = self.header().slot_count as usize;
        Ok(self
            .row_modify
            .get_or_insert_with(|| RowModify::new(slot_count)))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn validate_page_len(page_len: usize, header_size: usize) -> Result<(), PageError> {
    if page_len > (u16::MAX as usize) {
        return Err(PageError::Corrupt(format!(
            "page too large for u16 offsets: {page_len}"
        )));
    }
    if page_len < header_size {
        return Err(PageError::Corrupt(format!(
            "page too small for header: {page_len}"
        )));
    }
    Ok(())
}

fn read_u8(buf: &[u8], off: usize) -> Result<u8, PageError> {
    buf.get(off)
        .copied()
        .ok_or_else(|| PageError::Corrupt("read_u8 out of bounds".into()))
}

fn write_u8(buf: &mut [u8], off: usize, value: u8) -> Result<(), PageError> {
    let Some(byte) = buf.get_mut(off) else {
        return Err(PageError::Corrupt("write_u8 out of bounds".into()));
    };
    *byte = value;
    Ok(())
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, PageError> {
    if off + 2 > buf.len() {
        return Err(PageError::Corrupt("read_u16 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 2)]);
    rdr.read_u16::<LittleEndian>()
        .map_err(|e| PageError::Corrupt(e.to_string()))
}

fn write_u16(buf: &mut [u8], off: usize, value: u16) -> Result<(), PageError> {
    if off + 2 > buf.len() {
        return Err(PageError::Corrupt("write_u16 out of bounds".into()));
    }
    let mut writer = std::io::Cursor::new(&mut buf[off..(off + 2)]);
    writer
        .write_u16::<LittleEndian>(value)
        .map_err(|e| PageError::Corrupt(e.to_string()))
}

fn read_u64(buf: &[u8], off: usize) -> Result<u64, PageError> {
    if off + 8 > buf.len() {
        return Err(PageError::Corrupt("read_u64 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 8)]);
    rdr.read_u64::<LittleEndian>()
        .map_err(|e| PageError::Corrupt(e.to_string()))
}

fn write_u64(buf: &mut [u8], off: usize, value: u64) -> Result<(), PageError> {
    if off + 8 > buf.len() {
        return Err(PageError::Corrupt("write_u64 out of bounds".into()));
    }
    let mut writer = std::io::Cursor::new(&mut buf[off..(off + 8)]);
    writer
        .write_u64::<LittleEndian>(value)
        .map_err(|e| PageError::Corrupt(e.to_string()))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::{Update, UpdateChain, UpdateType};

    #[test]
    fn leaf_constructor_sets_cached_header() {
        let page = Page::new_leaf(256).unwrap();
        assert_eq!(page.header().page_type, PageType::Leaf);
        assert_eq!(page.header().slot_count, 0);
        assert_eq!(page.header().lower, 8);
        assert_eq!(page.header().upper, 256);
        assert_eq!(page.header().first_child, 0);
        assert_eq!(page.header().header_size(), 8);
        assert_eq!(page.header().free_contiguous(), 248);
    }

    #[test]
    fn internal_constructor_sets_cached_header() {
        let page = Page::new_internal(256, 42).unwrap();
        assert_eq!(page.header().page_type, PageType::Internal);
        assert_eq!(page.header().slot_count, 0);
        assert_eq!(page.header().lower, 16);
        assert_eq!(page.header().upper, 256);
        assert_eq!(page.header().first_child, 42);
        assert_eq!(page.header().header_size(), 16);
        assert_eq!(page.header().free_contiguous(), 240);
    }

    #[test]
    fn setters_update_bytes_and_cached_header() {
        let mut page = Page::new_internal(256, 7).unwrap();

        page.raw_mut().set_flags(3).unwrap();
        page.raw_mut().set_slot_count(5).unwrap();
        page.raw_mut().set_lower(40).unwrap();
        page.raw_mut().set_upper(200).unwrap();
        page.raw_mut().set_first_child(99).unwrap();

        let reparsed = Page::from_bytes(page.data().to_vec()).unwrap();
        assert_eq!(reparsed.header(), page.header());
    }

    #[test]
    fn refresh_header_reloads_cached_fields_after_full_rewrite() {
        let mut page = Page::new_leaf(256).unwrap();
        let replacement = Page::new_internal(256, 17).unwrap();

        page.raw_mut()
            .data_mut()
            .copy_from_slice(replacement.data());
        page.raw_mut().refresh_header().unwrap();

        assert_eq!(page.header().page_type, PageType::Internal);
        assert_eq!(page.header().first_child, 17);
        assert_eq!(page.header().header_size(), 16);
    }

    #[test]
    fn ensure_row_modify_uses_current_leaf_slot_count() {
        let mut page = Page::new_leaf(256).unwrap();
        page.raw_mut().set_slot_count(2).unwrap();

        let row_modify = page.ensure_row_modify().unwrap();
        assert_eq!(row_modify.row_updates().len(), 2);
        assert_eq!(row_modify.row_inserts().len(), 3);
    }

    #[test]
    fn ensure_row_modify_rejects_internal_pages() {
        let mut page = Page::new_internal(256, 7).unwrap();
        let err = page.ensure_row_modify().unwrap_err();
        assert_eq!(
            err,
            PageError::Corrupt("row modify state is only valid for leaf pages".into())
        );
    }

    #[test]
    fn clone_for_edit_preserves_row_modify_state() {
        let mut page = Page::new_leaf(256).unwrap();
        page.raw_mut().set_slot_count(1).unwrap();

        let mut chain = UpdateChain::default();
        chain.prepend(Update::new(7, UpdateType::Standard, b"v1".to_vec()));
        page.ensure_row_modify().unwrap().row_updates_mut()[0] = Some(chain);

        let cloned = page.clone_for_edit();
        let updates = cloned.row_modify().unwrap().row_updates();
        let cloned_chain = updates[0].as_ref().unwrap();

        assert_eq!(
            cloned_chain.iter().next().unwrap().read().data,
            b"v1".to_vec()
        );
    }
}
