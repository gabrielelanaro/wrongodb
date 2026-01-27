//! Write-Ahead Logging (WAL) for crash recovery.
//!
//! Uses change vector logging (WiredTiger-style): operations are logged,
//! not full page images. This provides ~100x smaller WAL size.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;

use crate::{StorageError, WrongoDBError};

/// WAL file magic bytes (8 bytes)
const WAL_MAGIC: &[u8; 8] = b"WAL0001\0";
/// WAL file format version
const WAL_VERSION: u16 = 2;
/// WAL file header size (512 bytes)
const WAL_HEADER_SIZE: usize = 512;
/// WAL record header size (32 bytes)
/// Format: record_type(1) + flags(1) + payload_len(2) + reserved(2) + lsn(12) + prev_lsn(12) + crc32(4) = 34 bytes
/// But we'll use 32 bytes for simplicity, with lsn/prev_lsn using just offset (8 bytes each)
const RECORD_HEADER_SIZE: usize = 32;
/// Minimum WAL record size (for alignment)
const MIN_RECORD_SIZE: usize = 128;

/// Log Sequence Number - uniquely identifies a position in the WAL.
///
/// Combines file_id (for future log rotation) and byte offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    pub file_id: u32,
    pub offset: u64,
}

impl Lsn {
    pub fn new(file_id: u32, offset: u64) -> Self {
        Self { file_id, offset }
    }

    pub fn is_valid(&self) -> bool {
        self.file_id != 0 || self.offset != 0
    }
}

/// WAL record type - identifies the kind of operation logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    Put = 1,
    Delete = 2,
    Checkpoint = 3,
}

impl TryFrom<u8> for WalRecordType {
    type Error = WrongoDBError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Checkpoint),
            _ => Err(StorageError(format!("invalid WAL record type: {}", value)).into()),
        }
    }
}

/// A WAL log record containing operation data.
///
/// Uses change vectors: compact descriptions of operations, not full pages.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Checkpoint {
        root_block_id: u64,
        generation: u64,
    },
}

impl WalRecord {
    /// Get the record type for this WAL record.
    pub fn record_type(&self) -> WalRecordType {
        match self {
            WalRecord::Put { .. } => WalRecordType::Put,
            WalRecord::Delete { .. } => WalRecordType::Delete,
            WalRecord::Checkpoint { .. } => WalRecordType::Checkpoint,
        }
    }

    /// Serialize the record payload to bytes.
    fn serialize_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            WalRecord::Put { key, value } => {
                // key_len (4 bytes) + key
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                // value_len (4 bytes) + value
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }

            WalRecord::Delete { key } => {
                // key_len (4 bytes) + key
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }

            WalRecord::Checkpoint {
                root_block_id,
                generation,
            } => {
                // root_block_id (8 bytes)
                buf.extend_from_slice(&root_block_id.to_le_bytes());
                // generation (8 bytes)
                buf.extend_from_slice(&generation.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize a record payload from bytes.
    fn deserialize_payload(
        record_type: WalRecordType,
        data: &[u8],
    ) -> Result<Self, WrongoDBError> {
        let mut cursor = 0;

        let read_u64 = |data: &[u8], cur: &mut usize| -> Result<u64, WrongoDBError> {
            if *cur + 8 > data.len() {
                return Err(StorageError("unexpected EOF reading u64".into()).into());
            }
            let val = u64::from_le_bytes(data[*cur..*cur + 8].try_into().unwrap());
            *cur += 8;
            Ok(val)
        };

        let read_u32 = |data: &[u8], cur: &mut usize| -> Result<u32, WrongoDBError> {
            if *cur + 4 > data.len() {
                return Err(StorageError("unexpected EOF reading u32".into()).into());
            }
            let val = u32::from_le_bytes(data[*cur..*cur + 4].try_into().unwrap());
            *cur += 4;
            Ok(val)
        };

        let read_bytes = |data: &[u8], cur: &mut usize, len: usize| -> Result<Vec<u8>, WrongoDBError> {
            if *cur + len > data.len() {
                return Err(StorageError("unexpected EOF reading bytes".into()).into());
            }
            let val = data[*cur..*cur + len].to_vec();
            *cur += len;
            Ok(val)
        };

        match record_type {
            WalRecordType::Put => {
                let key_len = read_u32(data, &mut cursor)? as usize;
                let key = read_bytes(data, &mut cursor, key_len)?;
                let value_len = read_u32(data, &mut cursor)? as usize;
                let value = read_bytes(data, &mut cursor, value_len)?;

                Ok(WalRecord::Put { key, value })
            }

            WalRecordType::Delete => {
                let key_len = read_u32(data, &mut cursor)? as usize;
                let key = read_bytes(data, &mut cursor, key_len)?;

                Ok(WalRecord::Delete { key })
            }

            WalRecordType::Checkpoint => {
                let root_block_id = read_u64(data, &mut cursor)?;
                let generation = read_u64(data, &mut cursor)?;

                Ok(WalRecord::Checkpoint {
                    root_block_id,
                    generation,
                })
            }
        }
    }
}

/// WAL file header - written at the start of the WAL file.
#[derive(Debug, Clone)]
struct WalFileHeader {
    magic: [u8; 8],
    version: u16,
    page_size: u32,
    last_lsn: Lsn,
    checkpoint_lsn: Lsn,
    crc32: u32,
}

impl WalFileHeader {
    fn new(page_size: u32) -> Self {
        let mut header = Self {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            page_size,
            last_lsn: Lsn::new(0, 0),  // Invalid LSN initially (no records written)
            checkpoint_lsn: Lsn::new(0, 0),
            crc32: 0,
        };
        header.crc32 = header.compute_crc32();
        header
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        let mut cursor = 0;

        // Magic (8 bytes)
        buf[cursor..cursor + 8].copy_from_slice(&self.magic);
        cursor += 8;

        // Version (2 bytes)
        buf[cursor..cursor + 2].copy_from_slice(&self.version.to_le_bytes());
        cursor += 2;

        // Reserved (2 bytes)
        cursor += 2;

        // Page size (4 bytes)
        buf[cursor..cursor + 4].copy_from_slice(&self.page_size.to_le_bytes());
        cursor += 4;

        // Reserved (4 bytes)
        cursor += 4;

        // Last LSN: file_id (4 bytes)
        buf[cursor..cursor + 4].copy_from_slice(&self.last_lsn.file_id.to_le_bytes());
        cursor += 4;

        // Last LSN: offset (8 bytes)
        buf[cursor..cursor + 8].copy_from_slice(&self.last_lsn.offset.to_le_bytes());
        cursor += 8;

        // Checkpoint LSN: file_id (4 bytes)
        buf[cursor..cursor + 4].copy_from_slice(&self.checkpoint_lsn.file_id.to_le_bytes());
        cursor += 4;

        // Checkpoint LSN: offset (8 bytes)
        buf[cursor..cursor + 8].copy_from_slice(&self.checkpoint_lsn.offset.to_le_bytes());
        cursor += 8;

        // CRC32 (4 bytes)
        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        // Remainder is padding (already zeroed)

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(StorageError("WAL header too short".into()).into());
        }

        let mut cursor = 0;

        // Magic
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[cursor..cursor + 8]);
        cursor += 8;

        if magic != *WAL_MAGIC {
            return Err(StorageError(format!("invalid WAL magic: {:?}", magic)).into());
        }

        // Version
        let version = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap());
        if version != WAL_VERSION {
            return Err(StorageError(format!("unsupported WAL version: {}", version)).into());
        }
        cursor += 4; // Skip reserved

        // Page size
        let page_size = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 8; // Skip reserved

        // Last LSN
        let last_lsn_file_id = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let last_lsn_offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let last_lsn = Lsn::new(last_lsn_file_id, last_lsn_offset);

        // Checkpoint LSN
        let ckpt_lsn_file_id = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let ckpt_lsn_offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let checkpoint_lsn = Lsn::new(ckpt_lsn_file_id, ckpt_lsn_offset);

        // CRC32
        let crc32 = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());

        Ok(Self {
            magic,
            version,
            page_size,
            last_lsn,
            checkpoint_lsn,
            crc32,
        })
    }

    fn compute_crc32(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&[0u8, 0u8]); // Reserved
        hasher.update(&self.page_size.to_le_bytes());
        hasher.update(&[0u8, 0u8, 0u8, 0u8]); // Reserved
        hasher.update(&self.last_lsn.file_id.to_le_bytes());
        hasher.update(&self.last_lsn.offset.to_le_bytes());
        hasher.update(&self.checkpoint_lsn.file_id.to_le_bytes());
        hasher.update(&self.checkpoint_lsn.offset.to_le_bytes());
        hasher.finalize()
    }

    fn validate_crc(&self) -> bool {
        self.compute_crc32() == self.crc32
    }
}

/// WAL record header - precedes each record in the WAL.
#[derive(Debug, Clone)]
pub struct WalRecordHeader {
    pub record_type: u8,
    pub flags: u8,
    pub payload_len: u16,
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub crc32: u32,
}

impl WalRecordHeader {
    fn serialize(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        let mut cursor = 0;

        buf[cursor] = self.record_type;
        cursor += 1;
        buf[cursor] = self.flags;
        cursor += 1;
        buf[cursor..cursor + 2].copy_from_slice(&self.payload_len.to_le_bytes());
        cursor += 2;
        // Reserved (2 bytes)
        cursor += 2;

        // LSN offset (8 bytes) - simplified, file_id assumed to be 0
        buf[cursor..cursor + 8].copy_from_slice(&self.lsn.offset.to_le_bytes());
        cursor += 8;

        // Previous LSN offset (8 bytes) - simplified
        buf[cursor..cursor + 8].copy_from_slice(&self.prev_lsn.offset.to_le_bytes());
        cursor += 8;

        // CRC32 (4 bytes)
        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < RECORD_HEADER_SIZE {
            return Err(StorageError("WAL record header too short".into()).into());
        }

        let mut cursor = 0;

        let record_type = data[cursor];
        cursor += 1;
        let flags = data[cursor];
        cursor += 1;
        let payload_len = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        // Skip reserved (2 bytes)
        cursor += 2;

        // LSN offset (8 bytes)
        let lsn_offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let lsn = Lsn::new(0, lsn_offset);

        // Previous LSN offset (8 bytes)
        let prev_lsn_offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let prev_lsn = Lsn::new(0, prev_lsn_offset);

        // CRC32 (4 bytes)
        let crc32 = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());

        Ok(Self {
            record_type,
            flags,
            payload_len,
            lsn,
            prev_lsn,
            crc32,
        })
    }
}

/// WAL file - manages the write-ahead log.
///
/// Change vector logging: operations are logged as compact descriptions,
/// not full page images. This matches WiredTiger's production architecture.
#[derive(Debug)]
pub struct WalFile {
    path: PathBuf,
    file: File,
    header: WalFileHeader,
    write_buffer: Vec<u8>,
    buffer_capacity: usize,
    last_lsn: Lsn,
}

impl WalFile {
    /// Buffer capacity for WAL writes (64KB)
    const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;

    /// Create a new WAL file.
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let mut file = File::create(path)?;

        let header = WalFileHeader::new(page_size);
        let header_bytes = header.serialize();
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn: Lsn::new(0, WAL_HEADER_SIZE as u64),  // Next write position
        })
    }

    /// Open an existing WAL file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read and validate header
        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = WalFileHeader::deserialize(&header_bytes)?;

        if !header.validate_crc() {
            return Err(StorageError("WAL header CRC32 mismatch".into()).into());
        }

        let file_len = file.metadata()?.len();
        let last_lsn = Lsn::new(0, file_len);
        file.seek(SeekFrom::Start(last_lsn.offset))?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn,
        })
    }

    /// Get the checkpoint LSN - recovery should replay from this point.
    pub fn checkpoint_lsn(&self) -> Lsn {
        self.header.checkpoint_lsn
    }

    /// Update the checkpoint LSN in the WAL header.
    pub fn set_checkpoint_lsn(&mut self, lsn: Lsn) -> Result<(), WrongoDBError> {
        self.header.checkpoint_lsn = lsn;

        // Recompute CRC32 after modifying the header
        self.header.crc32 = self.header.compute_crc32();

        // Rewrite header to disk
        let header_bytes = self.header.serialize();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;

        Ok(())
    }

    /// Sync the WAL file to disk.
    pub fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.flush_buffer()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Log a logical put (upsert) operation.
    pub fn log_put(&mut self, key: &[u8], value: &[u8]) -> Result<Lsn, WrongoDBError> {
        let record = WalRecord::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.append_record(record)
    }

    /// Log a logical delete operation.
    pub fn log_delete(&mut self, key: &[u8]) -> Result<Lsn, WrongoDBError> {
        let record = WalRecord::Delete { key: key.to_vec() };
        self.append_record(record)
    }

    /// Log a checkpoint operation.
    pub fn log_checkpoint(&mut self, root_block_id: u64, generation: u64) -> Result<Lsn, WrongoDBError> {
        let record = WalRecord::Checkpoint {
            root_block_id,
            generation,
        };
        self.append_record(record)?;

        // Return the LSN AFTER the checkpoint record
        // Recovery should start reading from here, skipping the checkpoint record itself
        Ok(self.last_lsn)
    }

    /// Append a WAL record to the log.
    fn append_record(&mut self, record: WalRecord) -> Result<Lsn, WrongoDBError> {
        let payload = record.serialize_payload();
        let record_type = record.record_type() as u8;
        let payload_len = payload.len() as u16;

        let lsn = self.last_lsn;
        let prev_lsn = self.header.last_lsn;

        // Compute CRC32 of header + payload
        // Header format (32 bytes):
        // - record_type (1) + flags (1) + payload_len (2) + reserved (2) = 6 bytes
        // - lsn_offset (8) = 8 bytes
        // - prev_lsn_offset (8) = 8 bytes
        // - crc32 (4) = 4 bytes
        let mut hasher = Hasher::new();
        hasher.update(&[record_type, 0]); // record_type + flags
        hasher.update(&payload_len.to_le_bytes());
        hasher.update(&[0u8, 0u8]); // reserved (2 bytes)
        hasher.update(&lsn.offset.to_le_bytes()); // lsn_offset (8 bytes)
        hasher.update(&prev_lsn.offset.to_le_bytes()); // prev_lsn_offset (8 bytes)
        hasher.update(&payload);
        let crc32 = hasher.finalize();

        // Build record header
        let header = WalRecordHeader {
            record_type,
            flags: 0,
            payload_len,
            lsn,
            prev_lsn,
            crc32,
        };

        // Write record header + payload
        let header_bytes = header.serialize();
        self.write_buffered(&header_bytes)?;
        self.write_buffered(&payload)?;

        // Update LSN tracking
        let record_size = RECORD_HEADER_SIZE + payload.len();
        self.last_lsn = Lsn::new(
            self.last_lsn.file_id,
            self.last_lsn.offset + record_size as u64,
        );
        // Update header.last_lsn to point to this record (the last written record)
        self.header.last_lsn = lsn;

        Ok(lsn)
    }

    /// Flush the write buffer to disk.
    fn flush_buffer(&mut self) -> Result<(), WrongoDBError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        self.file.write_all(&self.write_buffer)?;
        self.write_buffer.clear();
        Ok(())
    }

    /// Write data to the WAL buffer.
    fn write_buffered(&mut self, data: &[u8]) -> Result<(), WrongoDBError> {
        if self.write_buffer.len() + data.len() > self.buffer_capacity {
            self.flush_buffer()?;
        }

        self.write_buffer.extend_from_slice(data);
        Ok(())
    }

    /// Close the WAL file and delete it (after successful recovery).
    pub fn close_and_delete(mut self) -> Result<(), WrongoDBError> {
        // Flush any remaining data
        let _ = self.flush_buffer();

        // Get the path before self is dropped
        let path = self.path.clone();

        // File will be closed when self is dropped
        drop(self);

        // Delete the file
        std::fs::remove_file(&path)?;
        Ok(())
    }
}

impl Drop for WalFile {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}

/// Helper to generate WAL file path from data file path.
pub fn wal_path_from_data_path(data_path: &Path) -> PathBuf {
    let mut wal_path = data_path.as_os_str().to_owned();
    wal_path.push(".wal");
    PathBuf::from(wal_path)
}

/// Recovery-specific error type for WAL operations.
#[derive(Debug)]
pub enum RecoveryError {
    /// Checksum mismatch in WAL record
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    /// Broken LSN chain (gap in sequence)
    BrokenLsnChain {
        current_lsn: Lsn,
        expected_prev: Lsn,
        actual_prev: Lsn,
    },
    /// Corrupt record header
    CorruptRecordHeader {
        offset: u64,
        details: String,
    },
    /// Corrupt record payload
    CorruptRecordPayload {
        record_type: WalRecordType,
        offset: u64,
        details: String,
    },
    /// Invalid WAL file
    InvalidWalFile(String),
    /// IO error during recovery
    Io(std::io::Error),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryError::ChecksumMismatch { offset, expected, actual } => {
                write!(f, "WAL checksum mismatch at offset {}: expected {:08x}, got {:08x}",
                       offset, expected, actual)
            }
            RecoveryError::BrokenLsnChain { current_lsn, expected_prev, actual_prev } => {
                write!(f, "WAL LSN chain broken at LSN {:?}: expected prev LSN {:?}, got {:?}",
                       current_lsn, expected_prev, actual_prev)
            }
            RecoveryError::CorruptRecordHeader { offset, details } => {
                write!(f, "WAL corrupt record header at offset {}: {}", offset, details)
            }
            RecoveryError::CorruptRecordPayload { record_type, offset, details } => {
                write!(f, "WAL corrupt payload for record type {:?} at offset {}: {}",
                       record_type, offset, details)
            }
            RecoveryError::InvalidWalFile(msg) => {
                write!(f, "Invalid WAL file: {}", msg)
            }
            RecoveryError::Io(err) => {
                write!(f, "WAL IO error: {}", err)
            }
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<std::io::Error> for RecoveryError {
    fn from(err: std::io::Error) -> Self {
        RecoveryError::Io(err)
    }
}

/// WAL reader for sequential reading during recovery.
///
/// Reads WAL records from the checkpoint LSN to the end of the file.
/// Validates checksums and LSN chains.
pub struct WalReader {
    file: File,
    header: WalFileHeader,
    current_offset: u64,
    last_valid_lsn: Lsn,
}

impl WalReader {
    /// Open a WAL file for reading.
    ///
    /// Returns an error if the WAL file doesn't exist or has an invalid header.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RecoveryError> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(RecoveryError::InvalidWalFile(
                format!("WAL file does not exist: {}", path.display())
            ));
        }

        let mut file = File::open(path)?;

        // Read and validate header
        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = WalFileHeader::deserialize(&header_bytes)
            .map_err(|e| RecoveryError::InvalidWalFile(format!("invalid header: {}", e)))?;

        if !header.validate_crc() {
            return Err(RecoveryError::InvalidWalFile(
                "WAL header CRC32 mismatch".to_string()
            ));
        }

        // Start reading from checkpoint LSN (or beginning if 0)
        let current_offset = if header.checkpoint_lsn.is_valid() {
            header.checkpoint_lsn.offset
        } else {
            WAL_HEADER_SIZE as u64
        };

        Ok(Self {
            file,
            header,
            current_offset,
            last_valid_lsn: Lsn::new(0, 0),
        })
    }

    /// Read the next WAL record from the log.
    ///
    /// Returns `Ok(None)` when reaching the end of the file or a partial record.
    /// Returns `Ok(Some((header, record)))` for successfully read records.
    /// Returns `Err` for unrecoverable errors (corruption).
    pub fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError> {
        // Seek to current offset
        self.file.seek(SeekFrom::Start(self.current_offset))?;

        // Try to read record header
        let mut header_bytes = vec![0u8; RECORD_HEADER_SIZE];
        let bytes_read = self.file.read(&mut header_bytes)?;

        // EOF check
        if bytes_read == 0 {
            return Ok(None);
        }

        // Partial header at EOF
        if bytes_read < RECORD_HEADER_SIZE {
            return Ok(None);
        }

        // Deserialize header
        let header = WalRecordHeader::deserialize(&header_bytes)
            .map_err(|e| RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: e.to_string(),
            })?;

        // Read payload
        let payload_len = header.payload_len as usize;
        let mut payload = vec![0u8; payload_len];
        let payload_bytes_read = self.file.read(&mut payload)?;

        // Partial payload at EOF
        if payload_bytes_read < payload_len {
            return Ok(None);
        }

        // Validate checksum
        // Header format for CRC (same as during write):
        // - record_type (1) + flags (1) + payload_len (2) + reserved (2) = 6 bytes
        // - lsn_offset (8) + prev_lsn_offset (8) = 16 bytes
        // Total = 22 bytes (not including CRC itself or padding)
        let crc_header_len = 1 + 1 + 2 + 2 + 8 + 8; // 22 bytes
        let mut hasher = Hasher::new();
        hasher.update(&header_bytes[0..crc_header_len]);
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if computed_crc != header.crc32 {
            return Err(RecoveryError::ChecksumMismatch {
                offset: self.current_offset,
                expected: header.crc32,
                actual: computed_crc,
            });
        }

        // Validate LSN chain
        if self.last_valid_lsn.is_valid() && header.prev_lsn != self.last_valid_lsn {
            return Err(RecoveryError::BrokenLsnChain {
                current_lsn: header.lsn,
                expected_prev: self.last_valid_lsn,
                actual_prev: header.prev_lsn,
            });
        }

        // Deserialize payload
        let record_type = WalRecordType::try_from(header.record_type)
            .map_err(|_| RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: format!("invalid record type: {}", header.record_type),
            })?;

        let record = WalRecord::deserialize_payload(record_type, &payload)
            .map_err(|e| RecoveryError::CorruptRecordPayload {
                record_type,
                offset: self.current_offset + RECORD_HEADER_SIZE as u64,
                details: e.to_string(),
            })?;

        // Update state
        self.last_valid_lsn = header.lsn;
        self.current_offset += (RECORD_HEADER_SIZE + payload_len) as u64;

        Ok(Some((header, record)))
    }

    /// Get the checkpoint LSN from the WAL header.
    pub fn checkpoint_lsn(&self) -> Lsn {
        self.header.checkpoint_lsn
    }

    /// Check if we've reached the end of the WAL file.
    pub fn is_eof(&mut self) -> Result<bool, RecoveryError> {
        let current_pos = self.file.seek(SeekFrom::Current(0))?;
        let file_len = self.file.metadata()?.len();
        Ok(current_pos >= file_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn lsn_creation_and_validation() {
        let lsn = Lsn::new(1, 100);
        assert!(lsn.is_valid());

        let invalid = Lsn::new(0, 0);
        assert!(!invalid.is_valid());
    }

    #[test]
    fn lsn_ordering() {
        let lsn1 = Lsn::new(0, 100);
        let lsn2 = Lsn::new(0, 200);
        assert!(lsn1 < lsn2);

        let lsn3 = Lsn::new(1, 50);
        assert!(lsn2 < lsn3); // Different file_id
    }

    #[test]
    fn wal_header_round_trip() {
        let header = WalFileHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            page_size: 4096,
            last_lsn: Lsn::new(0, 512),
            checkpoint_lsn: Lsn::new(0, 0),
            crc32: 0,
        };

        let crc = header.compute_crc32();
        let header_with_crc = WalFileHeader { crc32: crc, ..header };

        let serialized = header_with_crc.serialize();
        let deserialized = WalFileHeader::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.magic, header.magic);
        assert_eq!(deserialized.version, header.version);
        assert_eq!(deserialized.page_size, header.page_size);
        assert_eq!(deserialized.last_lsn, header.last_lsn);
        assert_eq!(deserialized.checkpoint_lsn, header.checkpoint_lsn);
        assert!(deserialized.validate_crc());
    }

    #[test]
    fn wal_header_crc_validation() {
        let mut header = WalFileHeader::new(4096);

        // Valid CRC should pass
        header.crc32 = header.compute_crc32();
        assert!(header.validate_crc());

        // Invalid CRC should fail
        header.crc32 = 12345;
        assert!(!header.validate_crc());
    }

    #[test]
    fn wal_file_create_and_open() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.wal");

        // Create
        let wal = WalFile::create(&path, 4096).unwrap();
        assert_eq!(wal.last_lsn.offset, WAL_HEADER_SIZE as u64);

        drop(wal);

        // Open
        let wal2 = WalFile::open(&path).unwrap();
        assert_eq!(wal2.header.page_size, 4096);
        assert!(wal2.header.validate_crc());
    }

    #[test]
    fn wal_path_helper() {
        let data_path = Path::new("/tmp/test.wt");
        let wal_path = wal_path_from_data_path(data_path);
        assert_eq!(wal_path, PathBuf::from("/tmp/test.wt.wal"));
    }

    #[test]
    fn wal_record_serialize_put() {
        let record = WalRecord::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let payload = record.serialize_payload();
        let deserialized =
            WalRecord::deserialize_payload(WalRecordType::Put, &payload).unwrap();

        assert_eq!(record, deserialized);
    }

    #[test]
    fn wal_record_serialize_delete() {
        let record = WalRecord::Delete {
            key: b"test_key".to_vec(),
        };

        let payload = record.serialize_payload();
        let deserialized =
            WalRecord::deserialize_payload(WalRecordType::Delete, &payload).unwrap();

        assert_eq!(record, deserialized);
    }

    #[test]
    fn wal_record_serialize_checkpoint() {
        let record = WalRecord::Checkpoint {
            root_block_id: 123,
            generation: 5,
        };

        let payload = record.serialize_payload();
        let deserialized =
            WalRecord::deserialize_payload(WalRecordType::Checkpoint, &payload).unwrap();

        assert_eq!(record, deserialized);
    }

    #[test]
    fn wal_file_log_and_read_put() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.wal");

        let mut wal = WalFile::create(&path, 4096).unwrap();

        // Log a put
        let lsn = wal.log_put(b"my_key", b"my_value").unwrap();

        assert_eq!(lsn.offset, WAL_HEADER_SIZE as u64);

        // Sync and close
        wal.sync().unwrap();
        drop(wal);

        // TODO: Add record reading functionality in Phase 4
        // For now, just verify the file was written
        assert!(path.exists());
    }

    #[test]
    fn wal_file_buffering() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test_buffer.wal");

        let mut wal = WalFile::create(&path, 4096).unwrap();

        // Write multiple records - should buffer
        wal.log_put(b"key1", b"value1").unwrap();
        wal.log_put(b"key2", b"value2").unwrap();
        wal.log_put(b"key3", b"value3").unwrap();

        // Now sync - should flush buffer
        wal.sync().unwrap();

        drop(wal);
        assert!(path.exists());
    }

    #[test]
    fn wal_lsn_progression() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test_lsn.wal");

        let mut wal = WalFile::create(&path, 4096).unwrap();

        let lsn1 = wal.log_put(b"key1", b"value1").unwrap();
        let lsn2 = wal.log_put(b"key2", b"value2").unwrap();
        let lsn3 = wal.log_put(b"key3", b"value3").unwrap();

        // LSNs should be sequential
        assert!(lsn1.offset < lsn2.offset);
        assert!(lsn2.offset < lsn3.offset);
    }
}
