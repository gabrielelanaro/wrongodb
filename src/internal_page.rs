use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

const PAGE_TYPE_INTERNAL: u8 = 2;

// Header layout (little-endian), offsets in bytes:
//   0: page_type (u8)
//   1: flags (u8)          - reserved for future use
//   2: slot_count (u16)    - number of separator slots (keys) stored in the page
//   4: lower (u16)         - end of slot directory (start of free space)
//   6: upper (u16)         - start of packed records (end of free space)
//   8: first_child (u64)   - child pointer for keys < key_at(0)
const HDR_PAGE_TYPE: usize = 0;
const HDR_FLAGS: usize = 1;
const HDR_SLOT_COUNT: usize = 2;
const HDR_LOWER: usize = 4;
const HDR_UPPER: usize = 6;
const HDR_FIRST_CHILD: usize = 8;
const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 4;

// Record bytes are identical to `LeafPage` records, but `vlen` is always 8 and stores a
// little-endian `u64` child block id.
const RECORD_HEADER_SIZE: usize = 4;
const RECORD_KLEN_OFF: usize = 0;
const RECORD_VLEN_OFF: usize = 2;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InternalPageError {
    #[error("page full")]
    PageFull,

    #[error("page corrupt: {0}")]
    Corrupt(String),
}

#[derive(Debug)]
pub struct InternalPage<'a> {
    buf: &'a mut [u8],
}

impl<'a> InternalPage<'a> {
    pub fn init(buf: &'a mut [u8], first_child: u64) -> Result<Self, InternalPageError> {
        if buf.len() > (u16::MAX as usize) {
            return Err(InternalPageError::Corrupt(format!(
                "page too large for u16 offsets: {}",
                buf.len()
            )));
        }
        if buf.len() < HEADER_SIZE {
            return Err(InternalPageError::Corrupt(format!(
                "page too small for header: {}",
                buf.len()
            )));
        }

        buf.fill(0);
        write_u8(buf, HDR_PAGE_TYPE, PAGE_TYPE_INTERNAL)?;
        write_u8(buf, HDR_FLAGS, 0)?;
        write_u16(buf, HDR_SLOT_COUNT, 0)?;
        write_u16(buf, HDR_LOWER, HEADER_SIZE as u16)?;
        write_u16(buf, HDR_UPPER, buf.len() as u16)?;
        write_u64(buf, HDR_FIRST_CHILD, first_child)?;
        Ok(Self { buf })
    }

    pub fn open(buf: &'a mut [u8]) -> Result<Self, InternalPageError> {
        let page = Self { buf };
        page.validate()?;
        Ok(page)
    }

    pub fn first_child(&self) -> Result<u64, InternalPageError> {
        read_u64(self.buf, HDR_FIRST_CHILD)
    }

    pub fn slot_count(&self) -> Result<usize, InternalPageError> {
        Ok(read_u16(self.buf, HDR_SLOT_COUNT)? as usize)
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
        Ok(&self.buf[key_start..key_end])
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
        read_u64(self.buf, child_off)
    }

    /// Route `key` to a child pointer.
    ///
    /// Separator key semantics:
    /// - `first_child` applies when `key < key_at(0)`.
    /// - Each slot i stores (sep_key_i, child_{i+1}); it applies when `key >= sep_key_i` and
    ///   (if i+1 exists) `key < sep_key_{i+1}`.
    pub fn child_for_key(&self, key: &[u8]) -> Result<u64, InternalPageError> {
        // `slot_count` is the number of separator entries stored in this internal node.
        // Each slot points to a record holding `(separator_key, child_pointer)`.
        let n = self.slot_count()?;
        if n == 0 {
            return Ok(self.first_child()?);
        }

        // Find the first separator key > `key` (upper bound), then pick the previous child.
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
            Ok(self.first_child()?)
        } else {
            Ok(self.child_at(lo - 1)?)
        }
    }

    /// Insert a new separator `(key, child)` or replace the child if `key` already exists.
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

        let upper = self.upper()?;
        let new_upper = upper
            .checked_sub(record_len)
            .ok_or_else(|| InternalPageError::Corrupt("upper underflow".into()))?;
        self.write_record(new_upper, key, child)?;
        self.set_upper(new_upper)?;

        let slot_count = self.slot_count()?;
        if idx > slot_count {
            return Err(InternalPageError::Corrupt("insertion index out of bounds".into()));
        }

        if idx < slot_count {
            let slots_start = HEADER_SIZE;
            let src_start = slots_start + idx * SLOT_SIZE;
            let src_end = slots_start + slot_count * SLOT_SIZE;
            let dst_start = slots_start + (idx + 1) * SLOT_SIZE;
            self.buf.copy_within(src_start..src_end, dst_start);
        }

        self.write_slot(idx, new_upper as u16, record_len as u16)?;
        self.set_slot_count((slot_count + 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count + 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    pub fn free_contiguous(&self) -> usize {
        let Ok(lower) = self.lower() else {
            return 0;
        };
        let Ok(upper) = self.upper() else {
            return 0;
        };
        upper.saturating_sub(lower)
    }

    /// Rewrite the page to remove fragmentation, preserving slot order.
    pub fn compact(&mut self) -> Result<(), InternalPageError> {
        self.validate()?;

        let slot_count = self.slot_count()?;
        let page_len = self.buf.len();
        let mut new_buf = vec![0u8; page_len];

        write_u8(&mut new_buf, HDR_PAGE_TYPE, PAGE_TYPE_INTERNAL)?;
        write_u8(&mut new_buf, HDR_FLAGS, 0)?;
        write_u16(&mut new_buf, HDR_SLOT_COUNT, slot_count as u16)?;
        write_u16(
            &mut new_buf,
            HDR_LOWER,
            (HEADER_SIZE + slot_count * SLOT_SIZE) as u16,
        )?;
        write_u64(&mut new_buf, HDR_FIRST_CHILD, self.first_child()?)?;

        let mut upper = page_len;
        for i in (0..slot_count).rev() {
            let (old_off, old_len) = self.slot(i)?;
            let record = self.buf[old_off..(old_off + old_len)].to_vec();
            if old_len == 0 {
                return Err(InternalPageError::Corrupt("zero-length record".into()));
            }
            upper = upper
                .checked_sub(old_len)
                .ok_or_else(|| InternalPageError::Corrupt("compact upper underflow".into()))?;
            new_buf[upper..(upper + old_len)].copy_from_slice(&record);
            write_u16(&mut new_buf, HEADER_SIZE + i * SLOT_SIZE, upper as u16)?;
            write_u16(
                &mut new_buf,
                HEADER_SIZE + i * SLOT_SIZE + 2,
                old_len as u16,
            )?;
        }

        write_u16(&mut new_buf, HDR_UPPER, upper as u16)?;
        self.buf.copy_from_slice(&new_buf);
        Ok(())
    }

    fn validate(&self) -> Result<(), InternalPageError> {
        if self.buf.len() < HEADER_SIZE {
            return Err(InternalPageError::Corrupt("buffer too small".into()));
        }
        let t = read_u8(self.buf, HDR_PAGE_TYPE)?;
        if t != PAGE_TYPE_INTERNAL {
            return Err(InternalPageError::Corrupt(format!(
                "unexpected page type: {t}"
            )));
        }
        let slot_count = self.slot_count()?;
        let lower = self.lower()?;
        let upper = self.upper()?;
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
        if upper > self.buf.len() {
            return Err(InternalPageError::Corrupt(format!(
                "upper out of bounds: upper={upper} len={}",
                self.buf.len()
            )));
        }
        let _ = self.first_child()?;
        Ok(())
    }

    fn lower(&self) -> Result<usize, InternalPageError> {
        Ok(read_u16(self.buf, HDR_LOWER)? as usize)
    }

    fn set_lower(&mut self, v: u16) -> Result<(), InternalPageError> {
        write_u16(self.buf, HDR_LOWER, v)
    }

    fn upper(&self) -> Result<usize, InternalPageError> {
        Ok(read_u16(self.buf, HDR_UPPER)? as usize)
    }

    fn set_upper(&mut self, v: usize) -> Result<(), InternalPageError> {
        if v > (u16::MAX as usize) {
            return Err(InternalPageError::Corrupt(format!("upper too large: {v}")));
        }
        write_u16(self.buf, HDR_UPPER, v as u16)
    }

    fn set_slot_count(&mut self, v: u16) -> Result<(), InternalPageError> {
        write_u16(self.buf, HDR_SLOT_COUNT, v)
    }

    fn write_slot(&mut self, index: usize, off: u16, len: u16) -> Result<(), InternalPageError> {
        let base = HEADER_SIZE + index * SLOT_SIZE;
        write_u16(self.buf, base, off)?;
        write_u16(self.buf, base + 2, len)?;
        Ok(())
    }

    fn slot(&self, index: usize) -> Result<(usize, usize), InternalPageError> {
        let slot_count = self.slot_count()?;
        if index >= slot_count {
            return Err(InternalPageError::Corrupt(format!(
                "slot index out of bounds: {index} (slot_count={slot_count})"
            )));
        }
        let base = HEADER_SIZE + index * SLOT_SIZE;
        let off = read_u16(self.buf, base)? as usize;
        let len = read_u16(self.buf, base + 2)? as usize;
        if off < HEADER_SIZE {
            return Err(InternalPageError::Corrupt(format!("record offset too small: {off}")));
        }
        if off + len > self.buf.len() {
            return Err(InternalPageError::Corrupt(format!(
                "record out of bounds: off={off} len={len} page_len={}",
                self.buf.len()
            )));
        }
        Ok((off, len))
    }

    fn record_header(&self, off: usize, len: usize) -> Result<(usize, usize), InternalPageError> {
        if len < RECORD_HEADER_SIZE {
            return Err(InternalPageError::Corrupt("record too small".into()));
        }
        let klen = read_u16(self.buf, off + RECORD_KLEN_OFF)? as usize;
        let vlen = read_u16(self.buf, off + RECORD_VLEN_OFF)? as usize;
        let expected = RECORD_HEADER_SIZE
            .checked_add(klen)
            .and_then(|x| x.checked_add(vlen))
            .ok_or_else(|| InternalPageError::Corrupt("record length overflow".into()))?;
        if expected != len {
            return Err(InternalPageError::Corrupt(format!(
                "record length mismatch: slot_len={len} expected={expected}"
            )));
        }
        Ok((klen, vlen))
    }

    fn record_key_at(&self, index: usize) -> Result<&[u8], InternalPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, _vlen) = self.record_header(off, len)?;
        let key_start = off + RECORD_HEADER_SIZE;
        let key_end = key_start + klen;
        Ok(&self.buf[key_start..key_end])
    }

    fn find_slot(&self, key: &[u8]) -> Result<(usize, bool), InternalPageError> {
        let n = self.slot_count()?;
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

    fn delete_at(&mut self, index: usize) -> Result<(), InternalPageError> {
        let slot_count = self.slot_count()?;
        if index >= slot_count {
            return Err(InternalPageError::Corrupt("delete index out of bounds".into()));
        }
        if index + 1 < slot_count {
            let slots_start = HEADER_SIZE;
            let src_start = slots_start + (index + 1) * SLOT_SIZE;
            let src_end = slots_start + slot_count * SLOT_SIZE;
            let dst_start = slots_start + index * SLOT_SIZE;
            self.buf.copy_within(src_start..src_end, dst_start);
        }
        self.set_slot_count((slot_count - 1) as u16)?;
        self.set_lower((HEADER_SIZE + (slot_count - 1) * SLOT_SIZE) as u16)?;
        Ok(())
    }

    fn write_record(&mut self, off: usize, key: &[u8], child: u64) -> Result<(), InternalPageError> {
        let klen_u16 = u16::try_from(key.len()).map_err(|_| {
            InternalPageError::Corrupt(format!("key too large: {}", key.len()))
        })?;
        let vlen_u16: u16 = 8;

        if off + RECORD_HEADER_SIZE + key.len() + 8 > self.buf.len() {
            return Err(InternalPageError::Corrupt(
                "write_record out of bounds".into(),
            ));
        }

        {
            let mut w = std::io::Cursor::new(&mut self.buf[off..]);
            w.write_u16::<LittleEndian>(klen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
            w.write_u16::<LittleEndian>(vlen_u16)
                .map_err(|e| InternalPageError::Corrupt(e.to_string()))?;
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let child_start = key_start + key.len();
        self.buf[key_start..child_start].copy_from_slice(key);
        write_u64(self.buf, child_start, child)?;
        Ok(())
    }
}

fn record_len(klen: usize, vlen: usize) -> Result<usize, InternalPageError> {
    let _ = u16::try_from(klen)
        .map_err(|_| InternalPageError::Corrupt(format!("key too large: {klen}")))?;
    let _ = u16::try_from(vlen)
        .map_err(|_| InternalPageError::Corrupt(format!("value too large: {vlen}")))?;
    Ok(RECORD_HEADER_SIZE + klen + vlen)
}

fn read_u8(buf: &[u8], off: usize) -> Result<u8, InternalPageError> {
    buf.get(off)
        .copied()
        .ok_or_else(|| InternalPageError::Corrupt("read_u8 out of bounds".into()))
}

fn write_u8(buf: &mut [u8], off: usize, v: u8) -> Result<(), InternalPageError> {
    let Some(b) = buf.get_mut(off) else {
        return Err(InternalPageError::Corrupt(
            "write_u8 out of bounds".into(),
        ));
    };
    *b = v;
    Ok(())
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, InternalPageError> {
    if off + 2 > buf.len() {
        return Err(InternalPageError::Corrupt(
            "read_u16 out of bounds".into(),
        ));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 2)]);
    rdr.read_u16::<LittleEndian>()
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn write_u16(buf: &mut [u8], off: usize, v: u16) -> Result<(), InternalPageError> {
    if off + 2 > buf.len() {
        return Err(InternalPageError::Corrupt(
            "write_u16 out of bounds".into(),
        ));
    }
    let mut w = std::io::Cursor::new(&mut buf[off..(off + 2)]);
    w.write_u16::<LittleEndian>(v)
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn read_u64(buf: &[u8], off: usize) -> Result<u64, InternalPageError> {
    if off + 8 > buf.len() {
        return Err(InternalPageError::Corrupt(
            "read_u64 out of bounds".into(),
        ));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 8)]);
    rdr.read_u64::<LittleEndian>()
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}

fn write_u64(buf: &mut [u8], off: usize, v: u64) -> Result<(), InternalPageError> {
    if off + 8 > buf.len() {
        return Err(InternalPageError::Corrupt(
            "write_u64 out of bounds".into(),
        ));
    }
    let mut w = std::io::Cursor::new(&mut buf[off..(off + 8)]);
    w.write_u64::<LittleEndian>(v)
        .map_err(|e| InternalPageError::Corrupt(e.to_string()))
}
