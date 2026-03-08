use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

use crate::storage::page_store::{Page, PageError, PageType};

// ============================================================================
// Constants
// ============================================================================

const HEADER_SIZE: usize = 8;
const SLOT_SIZE: usize = 4;

const RECORD_KLEN_OFF: usize = 0;
const RECORD_VLEN_OFF: usize = 2;
const RECORD_HEADER_SIZE: usize = 4;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur when working with leaf B+tree pages.
///
/// Leaf pages store the actual key-value pairs at the bottom of the B+tree.
/// This error type covers both space constraints and data corruption scenarios.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeafPageError {
    #[error("page full")]
    PageFull,

    #[error("page corrupt: {0}")]
    Corrupt(String),
}

// ============================================================================
// LeafPage (Read API)
// ============================================================================

/// Read-only view of a leaf B+tree page.
///
/// Leaf pages store the actual key-value data at the bottom level of the
/// B+tree. Each entry contains a key, value, and their lengths. Pages use
/// a slot-based layout with records growing from the end and slots from
/// the beginning.
#[derive(Debug)]
pub struct LeafPage<'a> {
    page: &'a Page,
}

impl<'a> LeafPage<'a> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Opens a read-only view of a leaf page.
    ///
    /// Validates that the page is correctly formatted as a leaf page
    /// before returning the view.
    pub fn open(page: &'a Page) -> Result<Self, LeafPageError> {
        validate_leaf(page)?;
        Ok(Self { page })
    }

    // ------------------------------------------------------------------------
    // Public API: Read Operations
    // ------------------------------------------------------------------------

    /// Looks up a key in the leaf page.
    ///
    /// Uses binary search to find the key. Returns `Some(value)` if found,
    /// `None` if the key is not present.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, LeafPageError> {
        let (idx, found) = self.find_slot(key)?;
        if !found {
            return Ok(None);
        }
        Ok(Some(self.value_at(idx)?.to_vec()))
    }

    pub fn slot_count(&self) -> usize {
        self.page.header().slot_count as usize
    }

    pub fn key_at(&self, index: usize) -> Result<&[u8], LeafPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, _vlen) = self.record_header(off, len)?;
        let key_start = off + RECORD_HEADER_SIZE;
        let key_end = key_start + klen;
        Ok(&self.page.data()[key_start..key_end])
    }

    pub fn value_at(&self, index: usize) -> Result<&[u8], LeafPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, vlen) = self.record_header(off, len)?;
        let val_start = off + RECORD_HEADER_SIZE + klen;
        let val_end = val_start + vlen;
        Ok(&self.page.data()[val_start..val_end])
    }

    pub fn contains_key(&self, key: &[u8]) -> Result<bool, LeafPageError> {
        let (_idx, found) = self.find_slot(key)?;
        Ok(found)
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn validate(&self) -> Result<(), LeafPageError> {
        validate_leaf(self.page)
    }

    fn slot(&self, index: usize) -> Result<(usize, usize), LeafPageError> {
        let slot_count = self.slot_count();
        if index >= slot_count {
            return Err(LeafPageError::Corrupt(format!(
                "slot index out of bounds: {index} (slot_count={slot_count})"
            )));
        }

        let base = HEADER_SIZE + index * SLOT_SIZE;
        let off = read_u16(self.page.data(), base)? as usize;
        let len = read_u16(self.page.data(), base + 2)? as usize;
        if off < HEADER_SIZE {
            return Err(LeafPageError::Corrupt(format!(
                "record offset too small: {off}"
            )));
        }
        if off + len > self.page.data().len() {
            return Err(LeafPageError::Corrupt(format!(
                "record out of bounds: off={off} len={len} page_len={}",
                self.page.data().len()
            )));
        }
        Ok((off, len))
    }

    fn find_slot(&self, key: &[u8]) -> Result<(usize, bool), LeafPageError> {
        self.validate()?;
        let slot_count = self.slot_count();
        let mut lo = 0usize;
        let mut hi = slot_count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mid_key = self.key_at(mid)?;
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Ok((mid, true)),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok((lo, false))
    }

    fn record_header(&self, off: usize, len: usize) -> Result<(usize, usize), LeafPageError> {
        if len < RECORD_HEADER_SIZE {
            return Err(LeafPageError::Corrupt("record too small".into()));
        }
        let klen = read_u16(self.page.data(), off + RECORD_KLEN_OFF)? as usize;
        let vlen = read_u16(self.page.data(), off + RECORD_VLEN_OFF)? as usize;
        let expected = RECORD_HEADER_SIZE
            .checked_add(klen)
            .and_then(|value| value.checked_add(vlen))
            .ok_or_else(|| LeafPageError::Corrupt("record length overflow".into()))?;
        if expected != len {
            return Err(LeafPageError::Corrupt(format!(
                "record length mismatch: slot_len={len} expected={expected}"
            )));
        }
        Ok((klen, vlen))
    }
}

// ============================================================================
// LeafPageMut (Write API)
// ============================================================================

/// Mutable view of a leaf B+tree page.
///
/// Provides write operations for leaf pages, including inserting, updating,
/// deleting key-value pairs, and compaction to reclaim space.
#[derive(Debug)]
pub struct LeafPageMut<'a> {
    page: &'a mut Page,
}

impl<'a> LeafPageMut<'a> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Opens a mutable view of a leaf page.
    ///
    /// Validates that the page is correctly formatted as a leaf page
    /// before returning the mutable view.
    pub fn open(page: &'a mut Page) -> Result<Self, LeafPageError> {
        validate_leaf(page)?;
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

    pub fn contains_key(&self, key: &[u8]) -> Result<bool, LeafPageError> {
        LeafPage::open(self.page)?.contains_key(key)
    }

    // ------------------------------------------------------------------------
    // Public API: Write Operations
    // ------------------------------------------------------------------------

    /// Inserts or updates a key-value pair.
    ///
    /// If the key already exists, the existing entry is deleted and replaced.
    /// Attempts compaction if space is insufficient. Returns [`PageFull`]
    /// if the page cannot accommodate the entry even after compaction.
    ///
    /// [`PageFull`]: LeafPageError::PageFull
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), LeafPageError> {
        let (idx, found) = self.find_slot(key)?;
        if found {
            self.delete_at(idx)?;
        }

        let record_len = record_len(key.len(), value.len())?;
        let need = record_len + SLOT_SIZE;
        if self.free_contiguous() < need {
            self.compact()?;
        }
        if self.free_contiguous() < need {
            return Err(LeafPageError::PageFull);
        }

        let upper = self.upper();
        let new_upper = upper
            .checked_sub(record_len)
            .ok_or_else(|| LeafPageError::Corrupt("upper underflow".into()))?;
        self.write_record(new_upper, key, value)?;
        self.set_upper(new_upper)?;

        let slot_count = self.slot_count();
        if idx > slot_count {
            return Err(LeafPageError::Corrupt(
                "insertion index out of bounds".into(),
            ));
        }

        if idx < slot_count {
            let slots_start = HEADER_SIZE;
            let src_start = slots_start + idx * SLOT_SIZE;
            let src_end = slots_start + slot_count * SLOT_SIZE;
            let dst_start = slots_start + (idx + 1) * SLOT_SIZE;
            self.page
                .raw_mut()
                .data_mut()
                .copy_within(src_start..src_end, dst_start);
        }

        self.write_slot(idx, new_upper as u16, record_len as u16)?;
        self.set_slot_count((slot_count + 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count + 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    /// Deletes a key from the leaf page.
    ///
    /// Returns `true` if the key was found and deleted, `false` if the key
    /// was not present.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, LeafPageError> {
        let (idx, found) = self.find_slot(key)?;
        if !found {
            return Ok(false);
        }
        self.delete_at(idx)?;
        Ok(true)
    }

    // ------------------------------------------------------------------------
    // Public API: Maintenance Operations
    // ------------------------------------------------------------------------

    /// Compacts the page by rewriting all records contiguously.
    ///
    /// Reclaims space left by deleted or modified records. Creates a new page
    /// with the same content but with records packed from the end, then swaps
    /// it in place.
    pub fn compact(&mut self) -> Result<(), LeafPageError> {
        self.validate()?;

        let slot_count = self.slot_count();
        let page_len = self.page.data().len();
        let flags = self.page.header().flags;
        let mut new_page = Page::new_leaf(page_len).map_err(map_page_err)?;
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
                return Err(LeafPageError::Corrupt("zero-length record".into()));
            }
            upper = upper
                .checked_sub(old_len)
                .ok_or_else(|| LeafPageError::Corrupt("compact upper underflow".into()))?;
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
        new_page.raw_mut().set_upper(upper as u16).map_err(map_page_err)?;

        *self.page = new_page;
        self.page.raw_mut().refresh_header().map_err(map_page_err)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn validate(&self) -> Result<(), LeafPageError> {
        validate_leaf(self.page)
    }

    fn find_slot(&self, key: &[u8]) -> Result<(usize, bool), LeafPageError> {
        LeafPage::open(self.page)?.find_slot(key)
    }

    fn slot(&self, index: usize) -> Result<(usize, usize), LeafPageError> {
        LeafPage::open(self.page)?.slot(index)
    }

    fn upper(&self) -> usize {
        self.page.header().upper as usize
    }

    fn set_slot_count(&mut self, value: u16) -> Result<(), LeafPageError> {
        self.page.raw_mut().set_slot_count(value).map_err(map_page_err)
    }

    fn set_lower(&mut self, value: u16) -> Result<(), LeafPageError> {
        self.page.raw_mut().set_lower(value).map_err(map_page_err)
    }

    fn set_upper(&mut self, value: usize) -> Result<(), LeafPageError> {
        let upper = u16::try_from(value)
            .map_err(|_| LeafPageError::Corrupt(format!("upper too large: {value}")))?;
        self.page.raw_mut().set_upper(upper).map_err(map_page_err)
    }

    fn write_slot(&mut self, index: usize, off: u16, len: u16) -> Result<(), LeafPageError> {
        let base = HEADER_SIZE + index * SLOT_SIZE;
        write_u16(self.page.raw_mut().data_mut(), base, off)?;
        write_u16(self.page.raw_mut().data_mut(), base + 2, len)?;
        Ok(())
    }

    fn delete_at(&mut self, index: usize) -> Result<(), LeafPageError> {
        self.validate()?;
        let slot_count = self.slot_count();
        if index >= slot_count {
            return Err(LeafPageError::Corrupt("delete index out of bounds".into()));
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

    fn write_record(&mut self, off: usize, key: &[u8], value: &[u8]) -> Result<(), LeafPageError> {
        let klen_u16 = u16::try_from(key.len())
            .map_err(|_| LeafPageError::Corrupt(format!("key too large: {}", key.len())))?;
        let vlen_u16 = u16::try_from(value.len())
            .map_err(|_| LeafPageError::Corrupt(format!("value too large: {}", value.len())))?;

        if off + RECORD_HEADER_SIZE + key.len() + value.len() > self.page.data().len() {
            return Err(LeafPageError::Corrupt("write_record out of bounds".into()));
        }

        {
            let mut writer = std::io::Cursor::new(&mut self.page.raw_mut().data_mut()[off..]);
            writer
                .write_u16::<LittleEndian>(klen_u16)
                .map_err(|e| LeafPageError::Corrupt(e.to_string()))?;
            writer
                .write_u16::<LittleEndian>(vlen_u16)
                .map_err(|e| LeafPageError::Corrupt(e.to_string()))?;
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let val_start = key_start + key.len();
        self.page.raw_mut().data_mut()[key_start..val_start].copy_from_slice(key);
        self.page.raw_mut().data_mut()[val_start..(val_start + value.len())]
            .copy_from_slice(value);
        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn validate_leaf(page: &Page) -> Result<(), LeafPageError> {
    if page.header().page_type != PageType::Leaf {
        return Err(LeafPageError::Corrupt(format!(
            "unexpected page type: {:?}",
            page.header().page_type
        )));
    }
    if page.data().len() < HEADER_SIZE {
        return Err(LeafPageError::Corrupt("buffer too small".into()));
    }

    let slot_count = page.header().slot_count as usize;
    let lower = page.header().lower as usize;
    let upper = page.header().upper as usize;
    if lower != HEADER_SIZE + slot_count * SLOT_SIZE {
        return Err(LeafPageError::Corrupt(format!(
            "lower mismatch: lower={lower} expected={}",
            HEADER_SIZE + slot_count * SLOT_SIZE
        )));
    }
    if lower > upper {
        return Err(LeafPageError::Corrupt(format!(
            "invalid free space: lower={lower} > upper={upper}"
        )));
    }
    if upper > page.data().len() {
        return Err(LeafPageError::Corrupt(format!(
            "upper out of bounds: upper={upper} len={}",
            page.data().len()
        )));
    }
    Ok(())
}

fn map_page_err(err: PageError) -> LeafPageError {
    LeafPageError::Corrupt(err.to_string())
}

fn record_len(klen: usize, vlen: usize) -> Result<usize, LeafPageError> {
    let _ = u16::try_from(klen)
        .map_err(|_| LeafPageError::Corrupt(format!("key too large: {klen}")))?;
    let _ = u16::try_from(vlen)
        .map_err(|_| LeafPageError::Corrupt(format!("value too large: {vlen}")))?;
    Ok(RECORD_HEADER_SIZE + klen + vlen)
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, LeafPageError> {
    if off + 2 > buf.len() {
        return Err(LeafPageError::Corrupt("read_u16 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 2)]);
    rdr.read_u16::<LittleEndian>()
        .map_err(|e| LeafPageError::Corrupt(e.to_string()))
}

fn write_u16(buf: &mut [u8], off: usize, value: u16) -> Result<(), LeafPageError> {
    if off + 2 > buf.len() {
        return Err(LeafPageError::Corrupt("write_u16 out of bounds".into()));
    }
    let mut writer = std::io::Cursor::new(&mut buf[off..(off + 2)]);
    writer
        .write_u16::<LittleEndian>(value)
        .map_err(|e| LeafPageError::Corrupt(e.to_string()))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block::file::BlockFile;
    use crate::storage::page_store::Page;
    use tempfile::tempdir;

    #[test]
    fn put_get_delete_roundtrip() {
        let mut page = Page::new_leaf(256).unwrap();
        {
            let mut leaf = LeafPageMut::open(&mut page).unwrap();
            leaf.put(b"b", b"two").unwrap();
            leaf.put(b"a", b"one").unwrap();
            leaf.put(b"c", b"three").unwrap();
        }

        let leaf = LeafPage::open(&page).unwrap();
        assert_eq!(leaf.get(b"a").unwrap(), Some(b"one".to_vec()));
        assert_eq!(leaf.get(b"b").unwrap(), Some(b"two".to_vec()));
        assert_eq!(leaf.get(b"c").unwrap(), Some(b"three".to_vec()));
        assert_eq!(leaf.get(b"nope").unwrap(), None);

        {
            let mut leaf = LeafPageMut::open(&mut page).unwrap();
            assert!(leaf.delete(b"b").unwrap());
        }
        assert_eq!(LeafPage::open(&page).unwrap().get(b"b").unwrap(), None);
        assert!(!LeafPageMut::open(&mut page).unwrap().delete(b"b").unwrap());

        {
            let mut leaf = LeafPageMut::open(&mut page).unwrap();
            leaf.put(b"b", b"two-again").unwrap();
        }
        assert_eq!(
            LeafPage::open(&page).unwrap().get(b"b").unwrap(),
            Some(b"two-again".to_vec())
        );
    }

    #[test]
    fn delete_leaves_garbage_but_put_compacts_when_needed() {
        let mut page = Page::new_leaf(128).unwrap();
        {
            let mut leaf = LeafPageMut::open(&mut page).unwrap();
            leaf.put(b"a", &[b'x'; 30]).unwrap();
            leaf.put(b"b", &[b'y'; 30]).unwrap();
            leaf.put(b"c", &[b'z'; 30]).unwrap();
            leaf.delete(b"b").unwrap();
            leaf.put(b"d", &[b'w'; 30]).unwrap();
        }

        let leaf = LeafPage::open(&page).unwrap();
        assert_eq!(leaf.get(b"d").unwrap().unwrap().len(), 30);
        assert_eq!(leaf.page.header().slot_count, 3);
        assert_eq!(leaf.page.header().lower, 20);
    }

    #[test]
    fn page_full_is_reported() {
        let mut page = Page::new_leaf(96).unwrap();
        let mut leaf = LeafPageMut::open(&mut page).unwrap();

        leaf.put(b"a", &[b'x'; 40]).unwrap();
        let err = leaf.put(b"b", &[b'y'; 40]).unwrap_err();
        assert_eq!(err, LeafPageError::PageFull);
    }

    #[test]
    fn persists_via_blockfile_payload() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("leaf.wt");

        let mut bf = BlockFile::create(&path, 512).unwrap();
        let block = bf.allocate_block().unwrap();
        let payload_len = bf.page_payload_len();
        let mut page = Page::new_leaf(payload_len).unwrap();
        {
            let mut leaf = LeafPageMut::open(&mut page).unwrap();
            leaf.put(b"k1", b"v1").unwrap();
            leaf.put(b"k2", b"v2").unwrap();
        }
        bf.write_block(block, page.data()).unwrap();
        bf.close().unwrap();

        let mut bf2 = BlockFile::open(&path).unwrap();
        let read = bf2.read_block(block, true).unwrap();
        let page = Page::from_bytes(read).unwrap();
        let leaf = LeafPage::open(&page).unwrap();
        assert_eq!(leaf.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(leaf.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        bf2.close().unwrap();
    }
}
