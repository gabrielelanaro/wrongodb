use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

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
// Page
// ============================================================================

#[derive(Debug, Clone)]
pub struct Page {
    header: PageHeader,
    data: Vec<u8>,
}

impl Page {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn from_bytes(data: Vec<u8>) -> Result<Self, PageError> {
        let header = PageHeader::parse(&data)?;
        Ok(Self { header, data })
    }

    pub fn new_leaf(page_len: usize) -> Result<Self, PageError> {
        validate_page_len(page_len, LEAF_HEADER_SIZE)?;

        let mut data = vec![0u8; page_len];
        write_u8(&mut data, HDR_PAGE_TYPE, PageType::Leaf.tag())?;
        write_u8(&mut data, HDR_FLAGS, 0)?;
        write_u16(&mut data, HDR_SLOT_COUNT, 0)?;
        write_u16(&mut data, HDR_LOWER, LEAF_HEADER_SIZE as u16)?;
        write_u16(&mut data, HDR_UPPER, page_len as u16)?;

        Self::from_bytes(data)
    }

    pub fn new_internal(page_len: usize, first_child: u64) -> Result<Self, PageError> {
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

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn refresh_header(&mut self) -> Result<(), PageError> {
        self.header = PageHeader::parse(&self.data)?;
        Ok(())
    }

    pub fn set_flags(&mut self, flags: u8) -> Result<(), PageError> {
        write_u8(&mut self.data, HDR_FLAGS, flags)?;
        self.header.flags = flags;
        Ok(())
    }

    pub fn set_slot_count(&mut self, slot_count: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_SLOT_COUNT, slot_count)?;
        self.header.slot_count = slot_count;
        Ok(())
    }

    pub fn set_lower(&mut self, lower: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_LOWER, lower)?;
        self.header.lower = lower;
        Ok(())
    }

    pub fn set_upper(&mut self, upper: u16) -> Result<(), PageError> {
        write_u16(&mut self.data, HDR_UPPER, upper)?;
        self.header.upper = upper;
        Ok(())
    }

    #[cfg(test)]
    pub fn set_first_child(&mut self, first_child: u64) -> Result<(), PageError> {
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

        page.set_flags(3).unwrap();
        page.set_slot_count(5).unwrap();
        page.set_lower(40).unwrap();
        page.set_upper(200).unwrap();
        page.set_first_child(99).unwrap();

        let reparsed = Page::from_bytes(page.data().to_vec()).unwrap();
        assert_eq!(reparsed.header(), page.header());
    }

    #[test]
    fn refresh_header_reloads_cached_fields_after_full_rewrite() {
        let mut page = Page::new_leaf(256).unwrap();
        let replacement = Page::new_internal(256, 17).unwrap();

        page.data_mut().copy_from_slice(replacement.data());
        page.refresh_header().unwrap();

        assert_eq!(page.header().page_type, PageType::Internal);
        assert_eq!(page.header().first_child, 17);
        assert_eq!(page.header().header_size(), 16);
    }
}
