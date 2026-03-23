use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use super::manager::{BlockManager, Extent, ExtentLists};
use crate::core::errors::{StorageError, WrongoDBError};

// ============================================================================
// Constants
// ============================================================================

pub(in crate::storage) const NONE_BLOCK_ID: u64 = 0;

const CHECKSUM_SIZE: usize = 4;
const DEFAULT_PAGE_SIZE: usize = 4096;
const MAGIC: [u8; 8] = *b"MMWT0001";
const VERSION: u16 = 3;
const CHECKPOINT_SLOT_COUNT: usize = 2;
const CHECKPOINT_SLOT_SIZE: usize = 8 + 8 + 4;
const HEADER_FIXED_SIZE: usize =
    8 + 2 + 4 + 4 + 4 + 4 + (CHECKPOINT_SLOT_COUNT * CHECKPOINT_SLOT_SIZE);
const HEADER_MIN_SIZE: usize = HEADER_FIXED_SIZE;
const EXTENT_ENCODED_SIZE: usize = 8 + 8 + 8;

// ============================================================================
// CheckpointSlot - Checkpoint metadata with CRC validation
// ============================================================================

/// Checkpoint metadata record with CRC32 validation.
///
/// Stores the root block ID and generation number for a checkpoint,
/// with a checksum to detect corruption. Used for crash recovery
/// and checkpoint consistency verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointSlot {
    root_block_id: u64,
    generation: u64,
    crc32: u32,
}

impl CheckpointSlot {
    fn new(root_block_id: u64, generation: u64) -> Self {
        let crc32 = checkpoint_slot_crc(root_block_id, generation);
        Self {
            root_block_id,
            generation,
            crc32,
        }
    }

    fn is_valid(&self) -> bool {
        self.crc32 == checkpoint_slot_crc(self.root_block_id, self.generation)
    }

    fn update(&mut self, root_block_id: u64, generation: u64) {
        self.root_block_id = root_block_id;
        self.generation = generation;
        self.crc32 = checkpoint_slot_crc(root_block_id, generation);
    }
}

// ============================================================================
// FileHeader - On-disk header with checkpoint slots and extent lists
// ============================================================================

/// On-disk file header containing checkpoint metadata and extent allocation lists.
///
/// The header is stored in page 0 and includes:
/// - Magic number and version for file identification
/// - Page size for the file
/// - Count fields for alloc/avail/discard extent lists
/// - Two checkpoint slots for atomic checkpoint updates
///
/// Extent lists track which blocks are allocated (reachable from checkpoint),
/// available for reuse, or discarded (awaiting safe reclaim after checkpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileHeader {
    magic: [u8; 8],
    version: u16,
    page_size: u32,
    alloc_count: u32,
    avail_count: u32,
    discard_count: u32,
    checkpoint_slots: [CheckpointSlot; CHECKPOINT_SLOT_COUNT],
}

impl Default for FileHeader {
    fn default() -> Self {
        let slot0 = CheckpointSlot::new(NONE_BLOCK_ID, 1);
        let slot1 = CheckpointSlot::new(NONE_BLOCK_ID, 0);
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: DEFAULT_PAGE_SIZE as u32,
            alloc_count: 0,
            avail_count: 0,
            discard_count: 0,
            checkpoint_slots: [slot0, slot1],
        }
    }
}

impl FileHeader {
    // ------------------------------------------------------------------------
    // Serialization (pack/unpack)
    // ------------------------------------------------------------------------

    pub(crate) fn unpack_fixed(buf: &[u8]) -> Result<Self, WrongoDBError> {
        if buf.len() < HEADER_MIN_SIZE {
            return Err(StorageError("header buffer too small".into()).into());
        }

        let mut rdr = std::io::Cursor::new(buf);
        let mut magic = [0u8; 8];
        rdr.read_exact(&mut magic)?;
        let version = rdr.read_u16::<LittleEndian>()?;
        let page_size = rdr.read_u32::<LittleEndian>()?;
        let alloc_count = rdr.read_u32::<LittleEndian>()?;
        let avail_count = rdr.read_u32::<LittleEndian>()?;
        let discard_count = rdr.read_u32::<LittleEndian>()?;
        let mut checkpoint_slots = [CheckpointSlot {
            root_block_id: NONE_BLOCK_ID,
            generation: 0,
            crc32: 0,
        }; CHECKPOINT_SLOT_COUNT];
        for slot in &mut checkpoint_slots {
            let root_block_id = rdr.read_u64::<LittleEndian>()?;
            let generation = rdr.read_u64::<LittleEndian>()?;
            let crc32 = rdr.read_u32::<LittleEndian>()?;
            *slot = CheckpointSlot {
                root_block_id,
                generation,
                crc32,
            };
        }

        Ok(Self {
            magic,
            version,
            page_size,
            alloc_count,
            avail_count,
            discard_count,
            checkpoint_slots,
        })
    }

    pub(crate) fn unpack(buf: &[u8]) -> Result<(Self, ExtentLists), WrongoDBError> {
        if buf.len() < HEADER_MIN_SIZE {
            return Err(StorageError("header buffer too small".into()).into());
        }

        let mut rdr = std::io::Cursor::new(buf);
        let mut magic = [0u8; 8];
        rdr.read_exact(&mut magic)?;
        let version = rdr.read_u16::<LittleEndian>()?;
        let page_size = rdr.read_u32::<LittleEndian>()?;
        let alloc_count = rdr.read_u32::<LittleEndian>()?;
        let avail_count = rdr.read_u32::<LittleEndian>()?;
        let discard_count = rdr.read_u32::<LittleEndian>()?;
        let mut checkpoint_slots = [CheckpointSlot {
            root_block_id: NONE_BLOCK_ID,
            generation: 0,
            crc32: 0,
        }; CHECKPOINT_SLOT_COUNT];
        for slot in &mut checkpoint_slots {
            let root_block_id = rdr.read_u64::<LittleEndian>()?;
            let generation = rdr.read_u64::<LittleEndian>()?;
            let crc32 = rdr.read_u32::<LittleEndian>()?;
            *slot = CheckpointSlot {
                root_block_id,
                generation,
                crc32,
            };
        }

        let total_extents = alloc_count
            .checked_add(avail_count)
            .and_then(|v| v.checked_add(discard_count))
            .ok_or_else(|| StorageError("extent count overflow".into()))?;
        let required = HEADER_FIXED_SIZE
            .checked_add((total_extents as usize).saturating_mul(EXTENT_ENCODED_SIZE))
            .ok_or_else(|| StorageError("extent metadata size overflow".into()))?;
        if buf.len() < required {
            return Err(StorageError("header extent list truncated".into()).into());
        }

        let mut lists = ExtentLists::default();
        for _ in 0..alloc_count {
            lists.alloc.push(read_extent(&mut rdr)?);
        }
        for _ in 0..avail_count {
            lists.avail.push(read_extent(&mut rdr)?);
        }
        for _ in 0..discard_count {
            lists.discard.push(read_extent(&mut rdr)?);
        }

        Ok((
            Self {
                magic,
                version,
                page_size,
                alloc_count,
                avail_count,
                discard_count,
                checkpoint_slots,
            },
            lists,
        ))
    }

    pub(crate) fn pack(
        &self,
        extents: &ExtentLists,
        max_payload: usize,
    ) -> Result<Vec<u8>, WrongoDBError> {
        let mut buf = Vec::with_capacity(max_payload);
        buf.extend_from_slice(&self.magic);
        buf.write_u16::<LittleEndian>(self.version)?;
        buf.write_u32::<LittleEndian>(self.page_size)?;
        buf.write_u32::<LittleEndian>(extents.alloc.len() as u32)?;
        buf.write_u32::<LittleEndian>(extents.avail.len() as u32)?;
        buf.write_u32::<LittleEndian>(extents.discard.len() as u32)?;
        for slot in &self.checkpoint_slots {
            buf.write_u64::<LittleEndian>(slot.root_block_id)?;
            buf.write_u64::<LittleEndian>(slot.generation)?;
            buf.write_u32::<LittleEndian>(slot.crc32)?;
        }

        for extent in &extents.alloc {
            write_extent(&mut buf, extent)?;
        }
        for extent in &extents.avail {
            write_extent(&mut buf, extent)?;
        }
        for extent in &extents.discard {
            write_extent(&mut buf, extent)?;
        }

        if buf.len() > max_payload {
            return Err(StorageError("extent metadata exceeds header payload".into()).into());
        }
        buf.resize(max_payload, 0);
        Ok(buf)
    }
}

// ============================================================================
// BlockFile - Fixed-size block storage with extent allocation
// ============================================================================

/// Fixed-size block storage file with checksummed I/O and extent-based allocation.
///
/// `BlockFile` provides the foundation for on-disk storage with:
/// - **Checksummed I/O**: Each block includes a CRC32 checksum for corruption detection
/// - **Extent allocation**: Three-list allocation scheme (alloc/avail/discard) for COW semantics
/// - **Checkpoint slots**: Dual-slot design for atomic checkpoint updates
/// - **Crash recovery**: Header checksum and checkpoint slot validation on open
///
/// Block 0 is reserved for the file header. All other blocks are available
/// for data storage and managed through the extent allocation system.
#[derive(Debug)]
pub(in crate::storage) struct BlockFile {
    file: File,
    header: FileHeader,
    page_size: usize,
    active_checkpoint_slot: usize,
    block_manager: BlockManager,
}

impl BlockFile {
    // ------------------------------------------------------------------------
    // Constructors (highest level of abstraction)
    // ------------------------------------------------------------------------

    /// Create a new block file with the specified page size.
    ///
    /// Creates a new file on disk with a valid header and empty extent lists.
    /// Fails if a non-empty file already exists at the path.
    pub(in crate::storage) fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if path.exists() && path.metadata()?.len() > 0 {
            return Err(StorageError(format!("file already exists: {path:?}")).into());
        }

        if page_size < CHECKSUM_SIZE + HEADER_MIN_SIZE {
            return Err(StorageError("page_size too small for header".into()).into());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let header = FileHeader {
            page_size: page_size as u32,
            ..FileHeader::default()
        };
        let extents = ExtentLists::default();
        let payload = header.pack(&extents, page_size - CHECKSUM_SIZE)?;
        write_header_page(&mut file, &payload)?;
        file.sync_all()?;
        let active_checkpoint_slot = Self::select_checkpoint_slot(&header)?;
        let stable_generation = header.checkpoint_slots[active_checkpoint_slot].generation;
        let block_manager = BlockManager::new(stable_generation, extents);

        Ok(Self {
            file,
            header,
            page_size,
            active_checkpoint_slot,
            block_manager,
        })
    }

    /// Open an existing block file, validating header and checksums.
    ///
    /// Reads the file header, validates the magic number, version, and checksum,
    /// then selects the valid checkpoint slot with the highest generation number.
    /// Fails if the file is corrupted, has an unsupported version, or has no valid checkpoints.
    pub(in crate::storage) fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| StorageError(format!("failed to open {path:?}: {e}")))?;

        let mut prefix = vec![0u8; CHECKSUM_SIZE + HEADER_FIXED_SIZE];
        file.read_exact(&mut prefix)
            .map_err(|_| StorageError("file too small to contain header".into()))?;

        let mut rdr = std::io::Cursor::new(&prefix);
        let stored_checksum = rdr.read_u32::<LittleEndian>()?;
        let mut header_payload = vec![0u8; HEADER_FIXED_SIZE];
        rdr.read_exact(&mut header_payload)?;
        let header = FileHeader::unpack_fixed(&header_payload)?;

        if header.magic != MAGIC {
            return Err(StorageError("invalid file magic".into()).into());
        }
        if header.version != VERSION {
            return Err(StorageError(format!("unsupported version: {}", header.version)).into());
        }
        let page_size = header.page_size as usize;
        if page_size < CHECKSUM_SIZE + HEADER_MIN_SIZE {
            return Err(StorageError("corrupt header page_size".into()).into());
        }

        file.seek(SeekFrom::Start(0))?;
        let mut page0 = vec![0u8; page_size];
        file.read_exact(&mut page0)
            .map_err(|_| StorageError("short read on header page".into()))?;

        let payload0 = &page0[CHECKSUM_SIZE..];
        if crc32(payload0) != stored_checksum {
            return Err(StorageError("header checksum mismatch".into()).into());
        }

        let (header, extents) = FileHeader::unpack(payload0)?;

        let active_checkpoint_slot = Self::select_checkpoint_slot(&header)?;
        let stable_generation = header.checkpoint_slots[active_checkpoint_slot].generation;
        let block_manager = BlockManager::new(stable_generation, extents);
        Ok(Self {
            file,
            header,
            page_size,
            active_checkpoint_slot,
            block_manager,
        })
    }

    // ------------------------------------------------------------------------
    // Lifecycle methods (resource management)
    // ------------------------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn close(self) -> Result<(), WrongoDBError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub(in crate::storage) fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.file.sync_all()?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Checkpoint management
    // ------------------------------------------------------------------------

    /// Return the root block ID from the active checkpoint slot.
    pub(in crate::storage) fn root_block_id(&self) -> u64 {
        self.header.checkpoint_slots[self.active_checkpoint_slot].root_block_id
    }

    /// Update the root block ID in the next checkpoint slot.
    ///
    /// Advances to the next checkpoint slot with an incremented generation number,
    /// writes the updated header to disk, and updates the block manager's stable
    /// generation to enable reclaim of discarded extents.
    pub(in crate::storage) fn set_root_block_id(&mut self, root_block_id: u64) -> Result<(), WrongoDBError> {
        let current_slot = self.active_checkpoint_slot;
        let next_slot = (current_slot + 1) % CHECKPOINT_SLOT_COUNT;
        let current_gen = self.header.checkpoint_slots[current_slot].generation;
        let mut next_gen = current_gen.wrapping_add(1);
        if next_gen == 0 {
            next_gen = 1;
        }
        self.header.checkpoint_slots[next_slot].update(root_block_id, next_gen);
        self.active_checkpoint_slot = next_slot;
        self.block_manager.set_stable_generation(next_gen);
        self.write_header()?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Block allocation
    // ------------------------------------------------------------------------

    /// Allocate a single block from the avail list or grow the file.
    pub(in crate::storage) fn allocate_block(&mut self) -> Result<u64, WrongoDBError> {
        Ok(self.allocate_extent(1)?.offset)
    }

    /// Allocate a contiguous extent of the specified size.
    ///
    /// First tries to satisfy the request from the avail list. If no suitable
    /// extent is available, grows the file to allocate new blocks.
    pub(in crate::storage) fn allocate_extent(&mut self, blocks: u64) -> Result<Extent, WrongoDBError> {
        if blocks == 0 {
            return Err(StorageError("cannot allocate zero-length extent".into()).into());
        }

        if let Some(extent) = self.block_manager.allocate_from_avail(blocks) {
            self.write_header()?;
            return Ok(extent);
        }

        let new_id = self.num_blocks()?;
        let new_len_blocks = new_id
            .checked_add(blocks)
            .ok_or_else(|| StorageError("block count overflow".into()))?;
        let new_len = new_len_blocks
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| StorageError("file length overflow".into()))?;
        self.file.set_len(new_len)?;

        let extent = Extent {
            offset: new_id,
            size: blocks,
            generation: self.block_manager.stable_generation(),
        };
        self.block_manager.add_alloc_extent(extent);
        self.write_header()?;
        Ok(extent)
    }

    /// Preallocate blocks to the file, adding them to the avail list.
    ///
    /// Grows the file and adds the new blocks as a single extent to the avail
    /// list, avoiding fallocate/ftruncate in the allocation hot path.
    pub(in crate::storage) fn preallocate_blocks(&mut self, blocks: u64) -> Result<(), WrongoDBError> {
        if blocks == 0 {
            return Ok(());
        }

        let current_blocks = self.num_blocks()?;
        let new_len_blocks = current_blocks
            .checked_add(blocks)
            .ok_or_else(|| StorageError("block count overflow".into()))?;
        let new_len = new_len_blocks
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| StorageError("file length overflow".into()))?;
        self.file.set_len(new_len)?;

        let extent = Extent {
            offset: current_blocks,
            size: blocks,
            generation: self.block_manager.stable_generation(),
        };
        self.block_manager.add_avail_extent(extent);
        self.write_header()?;
        Ok(())
    }

    /// Free a single block, moving it to the discard list.
    pub(in crate::storage) fn free_block(&mut self, block_id: u64) -> Result<(), WrongoDBError> {
        if block_id == 0 {
            return Err(StorageError("block 0 is reserved for the header".into()).into());
        }

        let current_blocks = self.num_blocks()?;
        if block_id >= current_blocks {
            return Err(StorageError(format!(
                "cannot free unallocated block {block_id} (num_blocks={current_blocks})"
            ))
            .into());
        }
        self.block_manager.free_extent(block_id, 1)?;
        self.write_header()?;
        Ok(())
    }

    /// Reclaim discarded extents by moving them to the avail list.
    pub(in crate::storage) fn reclaim_discarded(&mut self) -> Result<(), WrongoDBError> {
        self.block_manager.reclaim_discarded();
        self.write_header()?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Block I/O
    // ------------------------------------------------------------------------

    /// Read a block from disk, optionally verifying the checksum.
    pub(in crate::storage) fn read_block(&mut self, block_id: u64, verify: bool) -> Result<Vec<u8>, WrongoDBError> {
        let offset = block_id
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| StorageError("block offset overflow".into()))?;

        self.file.seek(SeekFrom::Start(offset))?;
        let mut page = vec![0u8; self.page_size];
        self.file
            .read_exact(&mut page)
            .map_err(|_| StorageError(format!("short read for block {block_id}")))?;

        let mut rdr = std::io::Cursor::new(&page);
        let stored_checksum = rdr.read_u32::<LittleEndian>()?;
        let payload = &page[CHECKSUM_SIZE..];

        if verify && crc32(payload) != stored_checksum {
            return Err(StorageError(format!("checksum mismatch for block {block_id}")).into());
        }

        Ok(payload.to_vec())
    }

    /// Write a payload to a block, computing and storing the checksum.
    pub(in crate::storage) fn write_block(&mut self, block_id: u64, payload: &[u8]) -> Result<(), WrongoDBError> {
        if block_id == 0 {
            return Err(StorageError("block 0 is reserved for the header".into()).into());
        }

        let current_blocks = self.num_blocks()?;
        if block_id >= current_blocks {
            return Err(StorageError(format!(
                "block {block_id} not allocated (num_blocks={current_blocks}); call allocate_block() first"
            ))
            .into());
        }

        let max_payload = self.page_size - CHECKSUM_SIZE;
        if payload.len() > max_payload {
            return Err(StorageError(format!(
                "payload too large for page (max {max_payload} bytes)"
            ))
            .into());
        }

        let offset = block_id
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| StorageError("block offset overflow".into()))?;

        let mut padded = vec![0u8; max_payload];
        padded[..payload.len()].copy_from_slice(payload);
        let checksum = crc32(&padded);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_u32::<LittleEndian>(checksum)?;
        self.file.write_all(&padded)?;
        self.file.flush()?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private helpers (lowest level of abstraction)
    // ------------------------------------------------------------------------

    fn write_header(&mut self) -> Result<(), WrongoDBError> {
        let extents = self.block_manager.extent_lists();
        self.header.alloc_count = extents.alloc.len() as u32;
        self.header.avail_count = extents.avail.len() as u32;
        self.header.discard_count = extents.discard.len() as u32;
        let payload = self.header.pack(&extents, self.page_size - CHECKSUM_SIZE)?;
        write_header_page(&mut self.file, &payload)?;
        Ok(())
    }

    fn num_blocks(&mut self) -> Result<u64, WrongoDBError> {
        let size = self.file.metadata()?.len();
        Ok(size / (self.page_size as u64))
    }

    pub(crate) fn page_payload_len(&self) -> usize {
        self.page_size - CHECKSUM_SIZE
    }

    fn select_checkpoint_slot(header: &FileHeader) -> Result<usize, WrongoDBError> {
        let mut best_idx: Option<usize> = None;
        let mut best_gen = 0u64;
        for (idx, slot) in header.checkpoint_slots.iter().enumerate() {
            if slot.is_valid() && (best_idx.is_none() || slot.generation > best_gen) {
                best_idx = Some(idx);
                best_gen = slot.generation;
            }
        }
        best_idx.ok_or_else(|| StorageError("no valid checkpoint slots found".into()).into())
    }
}

// ============================================================================
// Helper functions (serialization and checksums)
// ============================================================================

fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn checkpoint_slot_crc(root_block_id: u64, generation: u64) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&root_block_id.to_le_bytes());
    hasher.update(&generation.to_le_bytes());
    hasher.finalize()
}

fn write_extent(buf: &mut Vec<u8>, extent: &Extent) -> Result<(), WrongoDBError> {
    buf.write_u64::<LittleEndian>(extent.offset)?;
    buf.write_u64::<LittleEndian>(extent.size)?;
    buf.write_u64::<LittleEndian>(extent.generation)?;
    Ok(())
}

fn read_extent(rdr: &mut std::io::Cursor<&[u8]>) -> Result<Extent, WrongoDBError> {
    let offset = rdr.read_u64::<LittleEndian>()?;
    let size = rdr.read_u64::<LittleEndian>()?;
    let generation = rdr.read_u64::<LittleEndian>()?;
    Ok(Extent {
        offset,
        size,
        generation,
    })
}

fn write_header_page(file: &mut File, payload: &[u8]) -> Result<(), WrongoDBError> {
    let checksum = crc32(payload);

    file.seek(SeekFrom::Start(0))?;
    let mut writer = std::io::BufWriter::new(file);
    writer.write_u32::<LittleEndian>(checksum)?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}
