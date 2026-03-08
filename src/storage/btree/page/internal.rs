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
/// Navigation uses binary search: for a given key, the page finds the child
/// whose subtree should contain that key.
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

    /// Finds the child page that should contain the given key.
    ///
    /// Uses binary search on the separator keys to determine which subtree
    /// should contain the key. If all separators are less than the key,
    /// returns the last child. If no slots exist, returns the first child.
    pub fn child_for_key(&self, key: &[u8]) -> Result<u64, InternalPageError> {
        let n = self.slot_count();
        if n == 0 {
            return Ok(self.first_child());
        }

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key_at(mid)?;
            if mid_key <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            Ok(self.first_child())
        } else {
            self.child_at(lo - 1)
        }
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
/// Provides write operations for internal pages, including inserting
/// separator-key/child pairs and compaction to reclaim space from deleted
/// entries.
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
    // Public API: Read Operations
    // ------------------------------------------------------------------------

    pub fn slot_count(&self) -> usize {
        self.page.header().slot_count as usize
    }

    pub fn free_contiguous(&self) -> usize {
        self.page.header().free_contiguous()
    }

    // ------------------------------------------------------------------------
    // Public API: Write Operations
    // ------------------------------------------------------------------------

    /// Inserts or updates a separator key and child page pair.
    ///
    /// If the key already exists, the existing entry is deleted and replaced.
    /// Attempts compaction if space is insufficient. Returns [`PageFull`]
    /// if the page cannot accommodate the entry even after compaction.
    ///
    /// [`PageFull`]: InternalPageError::PageFull
    pub fn put_separator(&mut self, key: &[u8], child: u64) -> Result<(), InternalPageError> {
        let (idx, found) = self.find_slot(key)?;
        if found {
            self.delete_at(idx)?;
        }

        let record_len = record_len(key.len(), 8)?;
        let need = record_len + SLOT_SIZE;
        if self.free_contiguous() < need {
            self.compact()?;
        }
        if self.free_contiguous() < need {
            return Err(InternalPageError::PageFull);
        }

        let upper = self.upper();
        let new_upper = upper
            .checked_sub(record_len)
            .ok_or_else(|| InternalPageError::Corrupt("upper underflow".into()))?;
        self.write_record(new_upper, key, child)?;
        self.set_upper(new_upper)?;

        let slot_count = self.slot_count();
        if idx > slot_count {
            return Err(InternalPageError::Corrupt(
                "insertion index out of bounds".into(),
            ));
        }

        if idx < slot_count {
            let slots_start = HEADER_SIZE;
            let src_start = slots_start + idx * SLOT_SIZE;
            let src_end = slots_start + slot_count * SLOT_SIZE;
            let dst_start = slots_start + (idx + 1) * SLOT_SIZE;
            self.page
                .data_mut()
                .copy_within(src_start..src_end, dst_start);
        }

        self.write_slot(idx, new_upper as u16, record_len as u16)?;
        self.set_slot_count((slot_count + 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count + 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Public API: Maintenance Operations
    // ------------------------------------------------------------------------

    /// Compacts the page by rewriting all records contiguously.
    ///
    /// Reclaims space left by deleted or modified records. Creates a new page
    /// with the same content but with records packed from the end, then swaps
    /// it in place.
    pub fn compact(&mut self) -> Result<(), InternalPageError> {
        validate_internal(self.page)?;

        let slot_count = self.slot_count();
        let page_len = self.page.data().len();
        let flags = self.page.header().flags;
        let first_child = self.page.header().first_child;
        let mut new_page = Page::new_internal(page_len, first_child).map_err(map_page_err)?;
        new_page.set_flags(flags).map_err(map_page_err)?;
        new_page
            .set_slot_count(slot_count as u16)
            .map_err(map_page_err)?;
        new_page
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
            new_page.data_mut()[upper..(upper + old_len)].copy_from_slice(&record);
            write_u16(
                new_page.data_mut(),
                HEADER_SIZE + i * SLOT_SIZE,
                upper as u16,
            )?;
            write_u16(
                new_page.data_mut(),
                HEADER_SIZE + i * SLOT_SIZE + 2,
                old_len as u16,
            )?;
        }
        new_page.set_upper(upper as u16).map_err(map_page_err)?;

        *self.page = new_page;
        self.page.refresh_header().map_err(map_page_err)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn find_slot(&self, key: &[u8]) -> Result<(usize, bool), InternalPageError> {
        let n = self.slot_count();
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.record_key_at(mid)?;
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok((mid, true)),
            }
        }
        Ok((lo, false))
    }

    fn slot(&self, index: usize) -> Result<(usize, usize), InternalPageError> {
        InternalPage::open(self.page)?.slot(index)
    }

    fn record_key_at(&self, index: usize) -> Result<&[u8], InternalPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, _vlen) = self.record_header(off, len)?;
        let key_start = off + RECORD_HEADER_SIZE;
        let key_end = key_start + klen;
        Ok(&self.page.data()[key_start..key_end])
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

    fn upper(&self) -> usize {
        self.page.header().upper as usize
    }

    fn set_slot_count(&mut self, value: u16) -> Result<(), InternalPageError> {
        self.page.set_slot_count(value).map_err(map_page_err)
    }

    fn set_lower(&mut self, value: u16) -> Result<(), InternalPageError> {
        self.page.set_lower(value).map_err(map_page_err)
    }

    fn set_upper(&mut self, value: usize) -> Result<(), InternalPageError> {
        let upper = u16::try_from(value)
            .map_err(|_| InternalPageError::Corrupt(format!("upper too large: {value}")))?;
        self.page.set_upper(upper).map_err(map_page_err)
    }

    fn write_slot(&mut self, index: usize, off: u16, len: u16) -> Result<(), InternalPageError> {
        let base = HEADER_SIZE + index * SLOT_SIZE;
        write_u16(self.page.data_mut(), base, off)?;
        write_u16(self.page.data_mut(), base + 2, len)?;
        Ok(())
    }

    fn delete_at(&mut self, index: usize) -> Result<(), InternalPageError> {
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
                .data_mut()
                .copy_within(src_start..src_end, dst_start);
        }
        self.set_slot_count((slot_count - 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count - 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    fn write_record(
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
            let mut writer = std::io::Cursor::new(&mut self.page.data_mut()[off..]);
            writer
                .write_u16::<LittleEndian>(klen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
            writer
                .write_u16::<LittleEndian>(vlen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let child_start = key_start + key.len();
        self.page.data_mut()[key_start..child_start].copy_from_slice(key);
        write_u64(self.page.data_mut(), child_start, child)?;
        Ok(())
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
