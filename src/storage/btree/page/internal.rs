use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

use crate::storage::page_store::{Page, PageError, PageType};

// ============================================================================
// Constants
// ============================================================================

const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 4;

const RECORD_HEADER_SIZE: usize = 4;
const RECORD_KLEN_OFF: usize = 0;
const RECORD_VLEN_OFF: usize = 2;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur when working with internal B+tree pages.
///
/// Internal pages store separator keys and child page pointers, forming the
/// upper levels of the B+tree. This error type covers both space constraints
/// and data corruption scenarios.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InternalPageError {
    #[error("page full")]
    PageFull,

    #[error("page corrupt: {0}")]
    Corrupt(String),
}

// ============================================================================
// InternalPage (Read API)
// ============================================================================

/// Read-only view of an internal B+tree page.
///
/// Internal pages form the upper levels of a B+tree, storing separator keys
/// and child page pointers. Each internal page has a "first child" pointer
/// and a series of slots containing (separator key, child page) pairs.
///
/// Navigation state is computed by the B+tree search layer; this type only
/// exposes the raw separator and child entries needed for that search.
#[derive(Debug)]
pub struct InternalPage<'a> {
    page: &'a Page,
}

impl<'a> InternalPage<'a> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Opens a read-only view of an internal page.
    ///
    /// Validates that the page is correctly formatted as an internal page
    /// before returning the view.
    pub fn open(page: &'a Page) -> Result<Self, InternalPageError> {
        validate_internal(page)?;
        Ok(Self { page })
    }

    // ------------------------------------------------------------------------
    // Public API: Read Operations
    // ------------------------------------------------------------------------

    pub fn first_child(&self) -> u64 {
        self.page.header().first_child
    }

    pub fn slot_count(&self) -> usize {
        self.page.header().slot_count as usize
    }

    pub fn key_at(&self, index: usize) -> Result<&[u8], InternalPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, vlen) = self.record_header(off, len)?;
        if vlen != 8 {
            return Err(InternalPageError::Corrupt(format!(
                "internal record vlen must be 8, got {vlen}"
            )));
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let key_end = key_start + klen;
        Ok(&self.page.data()[key_start..key_end])
    }

    pub fn child_at(&self, index: usize) -> Result<u64, InternalPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, vlen) = self.record_header(off, len)?;
        if vlen != 8 {
            return Err(InternalPageError::Corrupt(format!(
                "internal record vlen must be 8, got {vlen}"
            )));
        }
        let child_off = off + RECORD_HEADER_SIZE + klen;
        read_u64(self.page.data(), child_off)
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn slot(&self, index: usize) -> Result<(usize, usize), InternalPageError> {
        let slot_count = self.slot_count();
        if index >= slot_count {
            return Err(InternalPageError::Corrupt(format!(
                "slot index out of bounds: {index} (slot_count={slot_count})"
            )));
        }

        let base = HEADER_SIZE + index * SLOT_SIZE;
        let off = read_u16(self.page.data(), base)? as usize;
        let len = read_u16(self.page.data(), base + 2)? as usize;
        if off < HEADER_SIZE {
            return Err(InternalPageError::Corrupt(format!(
                "record offset too small: {off}"
            )));
        }
        if off + len > self.page.data().len() {
            return Err(InternalPageError::Corrupt(format!(
                "record out of bounds: off={off} len={len} page_len={}",
                self.page.data().len()
            )));
        }
        Ok((off, len))
    }

    fn record_header(&self, off: usize, len: usize) -> Result<(usize, usize), InternalPageError> {
        if len < RECORD_HEADER_SIZE {
            return Err(InternalPageError::Corrupt("record too small".into()));
        }
        let klen = read_u16(self.page.data(), off + RECORD_KLEN_OFF)? as usize;
        let vlen = read_u16(self.page.data(), off + RECORD_VLEN_OFF)? as usize;
        let expected = RECORD_HEADER_SIZE
            .checked_add(klen)
            .and_then(|value| value.checked_add(vlen))
            .ok_or_else(|| InternalPageError::Corrupt("record length overflow".into()))?;
        if expected != len {
            return Err(InternalPageError::Corrupt(format!(
                "record length mismatch: slot_len={len} expected={expected}"
            )));
        }
        Ok((klen, vlen))
    }
}

// ============================================================================
// InternalPageMut (Write API)
// ============================================================================

/// Mutable view of an internal B+tree page.
///
/// This type exposes low-level mutation primitives over the raw internal page
/// format. Higher-level separator update and compaction behavior lives in
/// `btree::internal_ops`.
#[derive(Debug)]
pub struct InternalPageMut<'a> {
    page: &'a mut Page,
}

impl<'a> InternalPageMut<'a> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Opens a mutable view of an internal page.
    ///
    /// Validates that the page is correctly formatted as an internal page
    /// before returning the mutable view.
    pub fn open(page: &'a mut Page) -> Result<Self, InternalPageError> {
        validate_internal(page)?;
        Ok(Self { page })
    }

    // ------------------------------------------------------------------------
    // Public API: Primitive Access
    // ------------------------------------------------------------------------

    pub(crate) fn page(&self) -> &Page {
        self.page
    }

    pub(crate) fn slot_count(&self) -> usize {
        self.page.header().slot_count as usize
    }

    pub(crate) fn free_contiguous(&self) -> usize {
        self.page.header().free_contiguous()
    }

    pub(crate) fn compact_in_place(&mut self) -> Result<(), InternalPageError> {
        validate_internal(self.page)?;

        let slot_count = self.slot_count();
        let page_len = self.page.data().len();
        let flags = self.page.header().flags;
        let first_child = self.page.header().first_child;
        let mut new_page = Page::new_internal(page_len, first_child).map_err(map_page_err)?;
        new_page.raw_mut().set_flags(flags).map_err(map_page_err)?;
        new_page
            .raw_mut()
            .set_slot_count(slot_count as u16)
            .map_err(map_page_err)?;
        new_page
            .raw_mut()
            .set_lower((HEADER_SIZE + slot_count * SLOT_SIZE) as u16)
            .map_err(map_page_err)?;

        let mut upper = page_len;
        for i in (0..slot_count).rev() {
            let (old_off, old_len) = self.slot(i)?;
            let record = self.page.data()[old_off..(old_off + old_len)].to_vec();
            if old_len == 0 {
                return Err(InternalPageError::Corrupt("zero-length record".into()));
            }
            upper = upper
                .checked_sub(old_len)
                .ok_or_else(|| InternalPageError::Corrupt("compact upper underflow".into()))?;
            new_page.raw_mut().data_mut()[upper..(upper + old_len)].copy_from_slice(&record);
            write_u16(
                new_page.raw_mut().data_mut(),
                HEADER_SIZE + i * SLOT_SIZE,
                upper as u16,
            )?;
            write_u16(
                new_page.raw_mut().data_mut(),
                HEADER_SIZE + i * SLOT_SIZE + 2,
                old_len as u16,
            )?;
        }
        new_page
            .raw_mut()
            .set_upper(upper as u16)
            .map_err(map_page_err)?;

        *self.page = new_page;
        self.page.raw_mut().refresh_header().map_err(map_page_err)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    pub(crate) fn slot(&self, index: usize) -> Result<(usize, usize), InternalPageError> {
        InternalPage::open(self.page)?.slot(index)
    }

    pub(crate) fn upper(&self) -> usize {
        self.page.header().upper as usize
    }

    pub(crate) fn set_slot_count(&mut self, value: u16) -> Result<(), InternalPageError> {
        self.page
            .raw_mut()
            .set_slot_count(value)
            .map_err(map_page_err)
    }

    pub(crate) fn set_lower(&mut self, value: u16) -> Result<(), InternalPageError> {
        self.page.raw_mut().set_lower(value).map_err(map_page_err)
    }

    pub(crate) fn set_upper(&mut self, value: usize) -> Result<(), InternalPageError> {
        let upper = u16::try_from(value)
            .map_err(|_| InternalPageError::Corrupt(format!("upper too large: {value}")))?;
        self.page.raw_mut().set_upper(upper).map_err(map_page_err)
    }

    pub(crate) fn write_slot(
        &mut self,
        index: usize,
        off: u16,
        len: u16,
    ) -> Result<(), InternalPageError> {
        let base = HEADER_SIZE + index * SLOT_SIZE;
        write_u16(self.page.raw_mut().data_mut(), base, off)?;
        write_u16(self.page.raw_mut().data_mut(), base + 2, len)?;
        Ok(())
    }

    pub(crate) fn delete_at(&mut self, index: usize) -> Result<(), InternalPageError> {
        let slot_count = self.slot_count();
        if index >= slot_count {
            return Err(InternalPageError::Corrupt(
                "delete index out of bounds".into(),
            ));
        }
        if index + 1 < slot_count {
            let slots_start = HEADER_SIZE;
            let src_start = slots_start + (index + 1) * SLOT_SIZE;
            let src_end = slots_start + slot_count * SLOT_SIZE;
            let dst_start = slots_start + index * SLOT_SIZE;
            self.page
                .raw_mut()
                .data_mut()
                .copy_within(src_start..src_end, dst_start);
        }
        self.set_slot_count((slot_count - 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count - 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    pub(crate) fn shift_slots_right(&mut self, index: usize) -> Result<(), InternalPageError> {
        let slot_count = self.slot_count();
        let slots_start = HEADER_SIZE;
        let src_start = slots_start + index * SLOT_SIZE;
        let src_end = slots_start + slot_count * SLOT_SIZE;
        let dst_start = slots_start + (index + 1) * SLOT_SIZE;
        self.page
            .raw_mut()
            .data_mut()
            .copy_within(src_start..src_end, dst_start);
        Ok(())
    }

    pub(crate) fn write_record(
        &mut self,
        off: usize,
        key: &[u8],
        child: u64,
    ) -> Result<(), InternalPageError> {
        let klen_u16 = u16::try_from(key.len())
            .map_err(|_| InternalPageError::Corrupt(format!("key too large: {}", key.len())))?;
        let vlen_u16: u16 = 8;

        if off + RECORD_HEADER_SIZE + key.len() + 8 > self.page.data().len() {
            return Err(InternalPageError::Corrupt(
                "write_record out of bounds".into(),
            ));
        }

        {
            let mut writer = std::io::Cursor::new(&mut self.page.raw_mut().data_mut()[off..]);
            writer
                .write_u16::<LittleEndian>(klen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
            writer
                .write_u16::<LittleEndian>(vlen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let child_start = key_start + key.len();
        self.page.raw_mut().data_mut()[key_start..child_start].copy_from_slice(key);
        write_u64(self.page.raw_mut().data_mut(), child_start, child)?;
        Ok(())
    }

    pub(crate) fn record_len(&self, klen: usize, vlen: usize) -> Result<usize, InternalPageError> {
        record_len(klen, vlen)
    }

    pub(crate) const fn header_size() -> usize {
        HEADER_SIZE
    }

    pub(crate) const fn slot_size() -> usize {
        SLOT_SIZE
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn validate_internal(page: &Page) -> Result<(), InternalPageError> {
    if page.header().page_type != PageType::Internal {
        return Err(InternalPageError::Corrupt(format!(
            "unexpected page type: {:?}",
            page.header().page_type
        )));
    }
    if page.data().len() < HEADER_SIZE {
        return Err(InternalPageError::Corrupt("buffer too small".into()));
    }

    let slot_count = page.header().slot_count as usize;
    let lower = page.header().lower as usize;
    let upper = page.header().upper as usize;
    if lower != HEADER_SIZE + slot_count * SLOT_SIZE {
        return Err(InternalPageError::Corrupt(format!(
            "lower mismatch: lower={lower} expected={}",
            HEADER_SIZE + slot_count * SLOT_SIZE
        )));
    }
    if lower > upper {
        return Err(InternalPageError::Corrupt(format!(
            "invalid free space: lower={lower} > upper={upper}"
        )));
    }
    if upper > page.data().len() {
        return Err(InternalPageError::Corrupt(format!(
            "upper out of bounds: upper={upper} len={}",
            page.data().len()
        )));
    }
    Ok(())
}

fn map_page_err(err: PageError) -> InternalPageError {
    InternalPageError::Corrupt(err.to_string())
}

fn record_len(klen: usize, vlen: usize) -> Result<usize, InternalPageError> {
    let _ = u16::try_from(klen)
        .map_err(|_| InternalPageError::Corrupt(format!("key too large: {klen}")))?;
    let _ = u16::try_from(vlen)
        .map_err(|_| InternalPageError::Corrupt(format!("value too large: {vlen}")))?;
    Ok(RECORD_HEADER_SIZE + klen + vlen)
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, InternalPageError> {
    if off + 2 > buf.len() {
        return Err(InternalPageError::Corrupt("read_u16 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 2)]);
    rdr.read_u16::<LittleEndian>()
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn write_u16(buf: &mut [u8], off: usize, value: u16) -> Result<(), InternalPageError> {
    if off + 2 > buf.len() {
        return Err(InternalPageError::Corrupt("write_u16 out of bounds".into()));
    }
    let mut writer = std::io::Cursor::new(&mut buf[off..(off + 2)]);
    writer
        .write_u16::<LittleEndian>(value)
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn read_u64(buf: &[u8], off: usize) -> Result<u64, InternalPageError> {
    if off + 8 > buf.len() {
        return Err(InternalPageError::Corrupt("read_u64 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 8)]);
    rdr.read_u64::<LittleEndian>()
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn write_u64(buf: &mut [u8], off: usize, value: u64) -> Result<(), InternalPageError> {
    if off + 8 > buf.len() {
        return Err(InternalPageError::Corrupt("write_u64 out of bounds".into()));
    }
    let mut writer = std::io::Cursor::new(&mut buf[off..(off + 8)]);
    writer
        .write_u64::<LittleEndian>(value)
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}
