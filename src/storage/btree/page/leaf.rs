use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

const PAGE_TYPE_LEAF: u8 = 1;
const HEADER_SIZE: usize = 8;
const SLOT_SIZE: usize = 4;

// Record layout within the packed record area:
//   +0: klen (u16)
//   +2: vlen (u16)
//   +4: key bytes (klen)
//   +4+klen: value bytes (vlen)
const RECORD_KLEN_OFF: usize = 0;
const RECORD_VLEN_OFF: usize = 2;
const RECORD_HEADER_SIZE: usize = 4;

// Header layout (little-endian), offsets in bytes:
//   0: page_type (u8)  - identifies how to interpret the page
//   1: flags (u8)      - reserved for future use
//   2: slot_count (u16)- number of slots (records) stored in the page
//   4: lower (u16)     - end of slot directory (start of free space)
//   6: upper (u16)     - start of packed records (end of free space)
const HDR_PAGE_TYPE: usize = 0;
const HDR_FLAGS: usize = 1;
const HDR_SLOT_COUNT: usize = 2;
const HDR_LOWER: usize = 4;
const HDR_UPPER: usize = 6;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeafPageError {
    #[error("page full")]
    PageFull,

    #[error("page corrupt: {0}")]
    Corrupt(String),
}

#[derive(Debug)]
pub struct LeafPage<'a> {
    buf: &'a mut [u8],
}

impl<'a> LeafPage<'a> {
    /// Initialize `buf` as a brand-new, empty leaf page.
    ///
    /// Algorithmic model:
    /// - The **slot directory** grows forward from the header (`lower` moves up).
    /// - The **record area** grows backward from the end of the page (`upper` moves down).
    /// - Free space is the gap between them (`upper - lower`).
    pub fn init(buf: &'a mut [u8]) -> Result<Self, LeafPageError> {
        if buf.len() > (u16::MAX as usize) {
            return Err(LeafPageError::Corrupt(format!(
                "page too large for u16 offsets: {}",
                buf.len()
            )));
        }
        if buf.len() < HEADER_SIZE {
            return Err(LeafPageError::Corrupt(format!(
                "page too small for header: {}",
                buf.len()
            )));
        }

        buf.fill(0);

        // Initialize an empty leaf page:
        // - no slots yet (`slot_count = 0`)
        // - slot directory ends right after the header (`lower = HEADER_SIZE`)
        // - record area is empty, so it starts at the end of the page (`upper = page_len`)
        write_u8(buf, HDR_PAGE_TYPE, PAGE_TYPE_LEAF)?;
        write_u8(buf, HDR_FLAGS, 0)?;
        write_u16(buf, HDR_SLOT_COUNT, 0)?;
        write_u16(buf, HDR_LOWER, HEADER_SIZE as u16)?;
        write_u16(buf, HDR_UPPER, buf.len() as u16)?;
        Ok(Self { buf })
    }

    pub fn open(buf: &'a mut [u8]) -> Result<Self, LeafPageError> {
        let page = Self { buf };
        page.validate()?;
        Ok(page)
    }

    /// Look up `key` and return a copy of its value (if present).
    ///
    /// Reads are driven by the slot directory: slots are kept sorted by key bytes,
    /// so we can binary-search slots without scanning record bytes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, LeafPageError> {
        let (idx, found) = self.find_slot(key)?;
        if !found {
            return Ok(None);
        }
        Ok(Some(self.value_at(idx)?.to_vec()))
    }

    /// Insert or replace `key -> value`.
    ///
    /// High-level algorithm:
    /// 1) Binary-search the slot directory for the key.
    /// 2) If present, delete the slot (leaving old record bytes as garbage).
    /// 3) Ensure there is enough **contiguous** free space (`upper - lower`):
    ///    - If not, compact (rewrite records tightly to the end of the page).
    ///    - If still not enough, return `PageFull`.
    /// 4) Write the new record at `new_upper = upper - record_len` and update `upper`.
    /// 5) Insert a new slot at the correct index (shift slots right to keep sorted order).
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), LeafPageError> {
        // `idx` is either the position where `key` already lives (found=true),
        // or the insertion point that preserves sorted order (found=false).
        let (idx, found) = self.find_slot(key)?;
        if found {
            self.delete_at(idx)?;
        }

        let record_len = record_len(key.len(), value.len())?;
        let need = record_len + SLOT_SIZE;
        // Deletes don't reclaim space immediately (they only remove slots), so `upper - lower`
        // can be too small even if there is plenty of garbage record bytes. Compact fixes that.
        if self.free_contiguous() < need {
            self.compact()?;
        }
        if self.free_contiguous() < need {
            return Err(LeafPageError::PageFull);
        }

        let upper = self.upper()?;
        let new_upper = upper
            .checked_sub(record_len)
            .ok_or_else(|| LeafPageError::Corrupt("upper underflow".into()))?;
        self.write_record(new_upper, key, value)?;
        self.set_upper(new_upper)?;

        let slot_count = self.slot_count()?;
        if idx > slot_count {
            return Err(LeafPageError::Corrupt(
                "insertion index out of bounds".into(),
            ));
        }

        // Shift slots right to make room for the new slot while preserving sorted-by-key order.
        // This is the "intense shifting" part, but we only move small fixed-size entries.
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

    /// Remove `key` if present.
    ///
    /// This is a *logical delete*: we remove the slot entry (so the record becomes unreachable),
    /// but we do not move/erase record bytes. That means fragmentation is expected until a later
    /// `put()` triggers `compact()`.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, LeafPageError> {
        let (idx, found) = self.find_slot(key)?;
        if !found {
            return Ok(false);
        }
        self.delete_at(idx)?;
        Ok(true)
    }

    pub fn slot_count(&self) -> Result<usize, LeafPageError> {
        Ok(self.slot_count_u16()? as usize)
    }

    pub fn key_at(&self, index: usize) -> Result<&[u8], LeafPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, _vlen) = self.record_header(off, len)?;
        // Key bytes start immediately after the record header (klen/vlen).
        let key_start = off + RECORD_HEADER_SIZE;
        let key_end = key_start + klen;
        Ok(&self.buf[key_start..key_end])
    }

    pub fn value_at(&self, index: usize) -> Result<&[u8], LeafPageError> {
        let (off, len) = self.slot(index)?;
        let (klen, vlen) = self.record_header(off, len)?;
        // Value bytes start immediately after (header + key bytes).
        let val_start = off + RECORD_HEADER_SIZE + klen;
        let val_end = val_start + vlen;
        Ok(&self.buf[val_start..val_end])
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

    /// Rewrite the page to remove fragmentation.
    ///
    /// Algorithm:
    /// - Build a fresh page buffer with the same header + slot directory shape.
    /// - Copy each live record (as referenced by slots) into a tightly packed region at the end.
    /// - Update each slot to point at the record's new offset.
    pub fn compact(&mut self) -> Result<(), LeafPageError> {
        self.validate()?;

        let slot_count = self.slot_count()?;
        let page_len = self.buf.len();
        let mut new_buf = vec![0u8; page_len];

        write_u8(&mut new_buf, 0, PAGE_TYPE_LEAF)?;
        write_u8(&mut new_buf, 1, 0)?;
        write_u16(&mut new_buf, 2, slot_count as u16)?;
        write_u16(
            &mut new_buf,
            4,
            (HEADER_SIZE + slot_count * SLOT_SIZE) as u16,
        )?;

        let mut upper = page_len;
        // Pack records from the end downward. Iterating in reverse lets us keep slot order
        // intact (slot i remains "the i-th smallest key") while laying bytes back-to-front.
        for i in (0..slot_count).rev() {
            let (old_off, old_len) = self.slot(i)?;
            let record = self.buf[old_off..(old_off + old_len)].to_vec();
            if old_len == 0 {
                return Err(LeafPageError::Corrupt("zero-length record".into()));
            }
            upper = upper
                .checked_sub(old_len)
                .ok_or_else(|| LeafPageError::Corrupt("compact upper underflow".into()))?;
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

    fn validate(&self) -> Result<(), LeafPageError> {
        if self.buf.len() < HEADER_SIZE {
            return Err(LeafPageError::Corrupt("buffer too small".into()));
        }
        let t = read_u8(self.buf, 0)?;
        if t != PAGE_TYPE_LEAF {
            return Err(LeafPageError::Corrupt(format!("unexpected page type: {t}")));
        }
        let slot_count = self.slot_count()?;
        let lower = self.lower()?;
        let upper = self.upper()?;
        // Invariant: `lower` is fully determined by the number of slots.
        if lower != HEADER_SIZE + slot_count * SLOT_SIZE {
            return Err(LeafPageError::Corrupt(format!(
                "lower mismatch: lower={lower} expected={}",
                HEADER_SIZE + slot_count * SLOT_SIZE
            )));
        }
        // Invariant: free space is a non-negative gap between slots and records.
        if lower > upper {
            return Err(LeafPageError::Corrupt(format!(
                "invalid free space: lower={lower} > upper={upper}"
            )));
        }
        if upper > self.buf.len() {
            return Err(LeafPageError::Corrupt(format!(
                "upper out of bounds: upper={upper} len={}",
                self.buf.len()
            )));
        }
        Ok(())
    }

    fn slot_count_u16(&self) -> Result<u16, LeafPageError> {
        read_u16(self.buf, HDR_SLOT_COUNT)
    }

    fn set_slot_count(&mut self, v: u16) -> Result<(), LeafPageError> {
        write_u16(self.buf, HDR_SLOT_COUNT, v)
    }

    fn lower(&self) -> Result<usize, LeafPageError> {
        Ok(read_u16(self.buf, HDR_LOWER)? as usize)
    }

    fn set_lower(&mut self, v: u16) -> Result<(), LeafPageError> {
        write_u16(self.buf, HDR_LOWER, v)
    }

    fn upper(&self) -> Result<usize, LeafPageError> {
        Ok(read_u16(self.buf, HDR_UPPER)? as usize)
    }

    fn set_upper(&mut self, v: usize) -> Result<(), LeafPageError> {
        if v > (u16::MAX as usize) {
            return Err(LeafPageError::Corrupt(format!("upper too large: {v}")));
        }
        write_u16(self.buf, HDR_UPPER, v as u16)
    }

    fn slot(&self, index: usize) -> Result<(usize, usize), LeafPageError> {
        let slot_count = self.slot_count()?;
        if index >= slot_count {
            return Err(LeafPageError::Corrupt(format!(
                "slot index out of bounds: {index} (slot_count={slot_count})"
            )));
        }
        let base = HEADER_SIZE + index * SLOT_SIZE;
        let off = read_u16(self.buf, base)? as usize;
        let len = read_u16(self.buf, base + 2)? as usize;
        if off < HEADER_SIZE {
            return Err(LeafPageError::Corrupt(format!(
                "record offset too small: {off}"
            )));
        }
        if off + len > self.buf.len() {
            return Err(LeafPageError::Corrupt(format!(
                "record out of bounds: off={off} len={len} page_len={}",
                self.buf.len()
            )));
        }
        Ok((off, len))
    }

    fn write_slot(&mut self, index: usize, off: u16, len: u16) -> Result<(), LeafPageError> {
        let base = HEADER_SIZE + index * SLOT_SIZE;
        write_u16(self.buf, base, off)?;
        write_u16(self.buf, base + 2, len)?;
        Ok(())
    }

    fn record_header(&self, off: usize, len: usize) -> Result<(usize, usize), LeafPageError> {
        if len < RECORD_HEADER_SIZE {
            return Err(LeafPageError::Corrupt("record too small".into()));
        }
        let klen = read_u16(self.buf, off + RECORD_KLEN_OFF)? as usize;
        let vlen = read_u16(self.buf, off + RECORD_VLEN_OFF)? as usize;
        let expected = RECORD_HEADER_SIZE
            .checked_add(klen)
            .and_then(|x| x.checked_add(vlen))
            .ok_or_else(|| LeafPageError::Corrupt("record length overflow".into()))?;
        if expected != len {
            return Err(LeafPageError::Corrupt(format!(
                "record length mismatch: slot_len={len} expected={expected}"
            )));
        }
        Ok((klen, vlen))
    }

    fn record_key_at(&self, index: usize) -> Result<&[u8], LeafPageError> {
        self.key_at(index)
    }

    fn find_slot(&self, key: &[u8]) -> Result<(usize, bool), LeafPageError> {
        self.validate()?;
        let slot_count = self.slot_count()?;
        // Standard "lower_bound" binary search over sorted slots:
        // - returns (idx, true) if found
        // - returns (insertion_point, false) otherwise
        let mut lo = 0usize;
        let mut hi = slot_count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mid_key = self.record_key_at(mid)?;
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Ok((mid, true)),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok((lo, false))
    }

    pub fn contains_key(&self, key: &[u8]) -> Result<bool, LeafPageError> {
        let (_idx, found) = self.find_slot(key)?;
        Ok(found)
    }

    fn delete_at(&mut self, index: usize) -> Result<(), LeafPageError> {
        self.validate()?;
        let slot_count = self.slot_count()?;
        if index >= slot_count {
            return Err(LeafPageError::Corrupt("delete index out of bounds".into()));
        }
        // Remove slot `index` by shifting later slots left by one.
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

    fn write_record(&mut self, off: usize, key: &[u8], value: &[u8]) -> Result<(), LeafPageError> {
        let klen_u16 = u16::try_from(key.len())
            .map_err(|_| LeafPageError::Corrupt(format!("key too large: {}", key.len())))?;
        let vlen_u16 = u16::try_from(value.len())
            .map_err(|_| LeafPageError::Corrupt(format!("value too large: {}", value.len())))?;

        if off + RECORD_HEADER_SIZE + key.len() + value.len() > self.buf.len() {
            return Err(LeafPageError::Corrupt("write_record out of bounds".into()));
        }

        {
            let mut w = std::io::Cursor::new(&mut self.buf[off..]);
            w.write_u16::<LittleEndian>(klen_u16)
                .map_err(|e| LeafPageError::Corrupt(e.to_string()))?;
            w.write_u16::<LittleEndian>(vlen_u16)
                .map_err(|e| LeafPageError::Corrupt(e.to_string()))?;
        }
        let key_start = off + RECORD_HEADER_SIZE;
        let val_start = key_start + key.len();
        self.buf[key_start..val_start].copy_from_slice(key);
        self.buf[val_start..(val_start + value.len())].copy_from_slice(value);
        Ok(())
    }
}

fn record_len(klen: usize, vlen: usize) -> Result<usize, LeafPageError> {
    let _ = u16::try_from(klen)
        .map_err(|_| LeafPageError::Corrupt(format!("key too large: {klen}")))?;
    let _ = u16::try_from(vlen)
        .map_err(|_| LeafPageError::Corrupt(format!("value too large: {vlen}")))?;
    Ok(RECORD_HEADER_SIZE + klen + vlen)
}

fn read_u8(buf: &[u8], off: usize) -> Result<u8, LeafPageError> {
    buf.get(off)
        .copied()
        .ok_or_else(|| LeafPageError::Corrupt("read_u8 out of bounds".into()))
}

fn write_u8(buf: &mut [u8], off: usize, v: u8) -> Result<(), LeafPageError> {
    let Some(b) = buf.get_mut(off) else {
        return Err(LeafPageError::Corrupt("write_u8 out of bounds".into()));
    };
    *b = v;
    Ok(())
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, LeafPageError> {
    if off + 2 > buf.len() {
        return Err(LeafPageError::Corrupt("read_u16 out of bounds".into()));
    }
    let mut rdr = std::io::Cursor::new(&buf[off..(off + 2)]);
    rdr.read_u16::<LittleEndian>()
        .map_err(|e| LeafPageError::Corrupt(e.to_string()))
}

fn write_u16(buf: &mut [u8], off: usize, v: u16) -> Result<(), LeafPageError> {
    if off + 2 > buf.len() {
        return Err(LeafPageError::Corrupt("write_u16 out of bounds".into()));
    }
    let mut w = std::io::Cursor::new(&mut buf[off..(off + 2)]);
    w.write_u16::<LittleEndian>(v)
        .map_err(|e| LeafPageError::Corrupt(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockFile;
    use tempfile::tempdir;

    #[test]
    fn put_get_delete_roundtrip() {
        let mut buf = vec![0u8; 256];
        let mut page = LeafPage::init(&mut buf).unwrap();

        page.put(b"b", b"two").unwrap();
        page.put(b"a", b"one").unwrap();
        page.put(b"c", b"three").unwrap();

        assert_eq!(page.get(b"a").unwrap(), Some(b"one".to_vec()));
        assert_eq!(page.get(b"b").unwrap(), Some(b"two".to_vec()));
        assert_eq!(page.get(b"c").unwrap(), Some(b"three".to_vec()));
        assert_eq!(page.get(b"nope").unwrap(), None);

        assert_eq!(page.delete(b"b").unwrap(), true);
        assert_eq!(page.get(b"b").unwrap(), None);
        assert_eq!(page.delete(b"b").unwrap(), false);

        page.put(b"b", b"two-again").unwrap();
        assert_eq!(page.get(b"b").unwrap(), Some(b"two-again".to_vec()));
    }

    #[test]
    fn delete_leaves_garbage_but_put_compacts_when_needed() {
        let mut buf = vec![0u8; 128];
        let mut page = LeafPage::init(&mut buf).unwrap();

        page.put(b"a", &vec![b'x'; 30]).unwrap();
        page.put(b"b", &vec![b'y'; 30]).unwrap();
        page.put(b"c", &vec![b'z'; 30]).unwrap();

        page.delete(b"b").unwrap();

        // This should succeed by compacting and reclaiming the deleted record space.
        page.put(b"d", &vec![b'w'; 30]).unwrap();
        assert_eq!(page.get(b"d").unwrap().unwrap().len(), 30);
    }

    #[test]
    fn page_full_is_reported() {
        let mut buf = vec![0u8; 96];
        let mut page = LeafPage::init(&mut buf).unwrap();

        page.put(b"a", &vec![b'x'; 40]).unwrap();
        let err = page.put(b"b", &vec![b'y'; 40]).unwrap_err();
        assert_eq!(err, LeafPageError::PageFull);
    }

    #[test]
    fn persists_via_blockfile_payload() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("leaf.wt");

        let mut bf = BlockFile::create(&path, 512).unwrap();
        let block = bf.allocate_block().unwrap();
        let payload_len = bf.page_payload_len();
        let mut page_bytes = vec![0u8; payload_len];
        {
            let mut page = LeafPage::init(&mut page_bytes).unwrap();
            page.put(b"k1", b"v1").unwrap();
            page.put(b"k2", b"v2").unwrap();
        }
        bf.write_block(block, &page_bytes).unwrap();
        bf.close().unwrap();

        let mut bf2 = BlockFile::open(&path).unwrap();
        let mut read = bf2.read_block(block, true).unwrap();
        let page = LeafPage::open(&mut read).unwrap();
        assert_eq!(page.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(page.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        bf2.close().unwrap();
    }
}
