use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use crate::errors::StorageError;
use crate::WrongoDBError;

const CHECKSUM_SIZE: usize = 4;
const DEFAULT_PAGE_SIZE: usize = 4096;
const MAGIC: [u8; 8] = *b"MMWT0001";
const VERSION: u16 = 1;
const HEADER_PAD_SIZE: usize = 64;
pub const NONE_BLOCK_ID: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub page_size: u32,
    pub root_block_id: u64,
    pub free_list_head: u64,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: DEFAULT_PAGE_SIZE as u32,
            root_block_id: NONE_BLOCK_ID,
            free_list_head: NONE_BLOCK_ID,
        }
    }
}

impl FileHeader {
    pub fn pack(&self) -> Result<[u8; HEADER_PAD_SIZE], WrongoDBError> {
        let mut buf = Vec::with_capacity(HEADER_PAD_SIZE);
        buf.extend_from_slice(&self.magic);
        buf.write_u16::<LittleEndian>(self.version)?;
        buf.write_u32::<LittleEndian>(self.page_size)?;
        buf.write_u64::<LittleEndian>(self.root_block_id)?;
        buf.write_u64::<LittleEndian>(self.free_list_head)?;
        if buf.len() > HEADER_PAD_SIZE {
            return Err(StorageError("header struct too large".into()).into());
        }
        buf.resize(HEADER_PAD_SIZE, 0);
        Ok(buf.try_into().expect("resized to exactly HEADER_PAD_SIZE"))
    }

    pub fn unpack(buf: &[u8]) -> Result<Self, WrongoDBError> {
        if buf.len() < 8 + 2 + 4 + 8 + 8 {
            return Err(StorageError("header buffer too small".into()).into());
        }

        let mut rdr = std::io::Cursor::new(buf);
        let mut magic = [0u8; 8];
        rdr.read_exact(&mut magic)?;
        let version = rdr.read_u16::<LittleEndian>()?;
        let page_size = rdr.read_u32::<LittleEndian>()?;
        let root_block_id = rdr.read_u64::<LittleEndian>()?;
        let free_list_head = rdr.read_u64::<LittleEndian>()?;
        Ok(Self {
            magic,
            version,
            page_size,
            root_block_id,
            free_list_head,
        })
    }
}

#[derive(Debug)]
pub struct BlockFile {
    pub path: PathBuf,
    file: File,
    pub header: FileHeader,
    pub page_size: usize,
}

impl BlockFile {
    pub(crate) fn page_payload_len(&self) -> usize {
        self.page_size - CHECKSUM_SIZE
    }

    pub fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if path.exists() && path.metadata()?.len() > 0 {
            return Err(StorageError(format!("file already exists: {path:?}")).into());
        }

        if page_size < CHECKSUM_SIZE + HEADER_PAD_SIZE {
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

        write_header_page(&mut file, &header, page_size)?;
        file.sync_all()?;

        Ok(Self {
            path,
            file,
            header,
            page_size,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Err(StorageError(format!("file not found: {path:?}")).into());
        }

        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut prefix = vec![0u8; CHECKSUM_SIZE + HEADER_PAD_SIZE];
        file.read_exact(&mut prefix)
            .map_err(|_| StorageError("file too small to contain header".into()))?;

        let mut rdr = std::io::Cursor::new(&prefix);
        let stored_checksum = rdr.read_u32::<LittleEndian>()?;
        let mut header_payload = [0u8; HEADER_PAD_SIZE];
        rdr.read_exact(&mut header_payload)?;
        let header = FileHeader::unpack(&header_payload)?;

        if header.magic != MAGIC {
            return Err(StorageError("invalid file magic".into()).into());
        }
        if header.version != VERSION {
            return Err(StorageError(format!("unsupported version: {}", header.version)).into());
        }
        let page_size = header.page_size as usize;
        if page_size < CHECKSUM_SIZE + HEADER_PAD_SIZE {
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

        Ok(Self {
            path,
            file,
            header,
            page_size,
        })
    }

    fn write_header(&mut self) -> Result<(), WrongoDBError> {
        write_header_page(&mut self.file, &self.header, self.page_size)?;
        Ok(())
    }

    pub fn set_root_block_id(&mut self, root_block_id: u64) -> Result<(), WrongoDBError> {
        self.header.root_block_id = root_block_id;
        self.write_header()?;
        Ok(())
    }

    pub fn allocate_block(&mut self) -> Result<u64, WrongoDBError> {
        let head = self.header.free_list_head;
        if head != NONE_BLOCK_ID {
            let block_id: u64 = head;
            let current_blocks = self.num_blocks()?;
            if block_id == 0 || block_id >= current_blocks {
                return Err(StorageError(format!(
                    "corrupt free_list_head={block_id} (num_blocks={current_blocks})"
                ))
                .into());
            }
            let payload = self.read_block(block_id, true)?;
            if payload.len() < 8 {
                return Err(StorageError(format!(
                    "free list block {block_id} too small for next pointer"
                ))
                .into());
            }
            let mut rdr = std::io::Cursor::new(&payload);
            let next = rdr.read_u64::<LittleEndian>()?;
            if next != NONE_BLOCK_ID {
                let current_blocks = self.num_blocks()?;
                if next == 0 || next >= current_blocks {
                    return Err(StorageError(format!(
                        "corrupt free list next={next} in block {block_id} (num_blocks={current_blocks})"
                    ))
                    .into());
                }
            }
            self.header.free_list_head = next;
            self.write_header()?;
            return Ok(block_id);
        }

        let new_id = self.num_blocks()?;
        let new_len = (new_id + 1)
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| StorageError("file length overflow".into()))?;
        self.file.set_len(new_len)?;
        Ok(new_id)
    }

    pub fn write_new_block(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError> {
        let block_id = self.allocate_block()?;
        self.write_block(block_id, payload)?;
        Ok(block_id)
    }

    pub fn free_block(&mut self, block_id: u64) -> Result<(), WrongoDBError> {
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

        let next = self.header.free_list_head;
        let mut payload = vec![0u8; 8];
        {
            let mut w = std::io::Cursor::new(&mut payload);
            w.write_u64::<LittleEndian>(next)?;
        }

        self.write_block(block_id, &payload)?;
        self.header.free_list_head = block_id;
        self.write_header()?;
        Ok(())
    }

    pub fn close(self) -> Result<(), WrongoDBError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn sync_data(&mut self) -> Result<(), WrongoDBError> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn read_block(&mut self, block_id: u64, verify: bool) -> Result<Vec<u8>, WrongoDBError> {
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

    pub fn write_block(&mut self, block_id: u64, payload: &[u8]) -> Result<(), WrongoDBError> {
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

    pub fn num_blocks(&mut self) -> Result<u64, WrongoDBError> {
        let size = self.file.metadata()?.len();
        Ok(size / (self.page_size as u64))
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn write_header_page(
    file: &mut File,
    header: &FileHeader,
    page_size: usize,
) -> Result<(), WrongoDBError> {
    let max_payload = page_size - CHECKSUM_SIZE;
    let packed = header.pack()?;
    if packed.len() > max_payload {
        return Err(StorageError("header does not fit in page".into()).into());
    }

    let mut padded = vec![0u8; max_payload];
    padded[..packed.len()].copy_from_slice(&packed);
    let checksum = crc32(&padded);

    file.seek(SeekFrom::Start(0))?;
    let mut writer = std::io::BufWriter::new(file);
    writer.write_u32::<LittleEndian>(checksum)?;
    writer.write_all(&padded)?;
    writer.flush()?;
    Ok(())
}
