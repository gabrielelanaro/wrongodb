use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crc32fast::Hasher;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::txn::{Timestamp, TxnId, TxnLogOp};

// ============================================================================
// Constants
// ============================================================================

#[cfg(test)]
const GLOBAL_WAL_FILE_NAME: &str = "global.wal";
const WAL_MAGIC: &[u8; 8] = b"WALG001\0";
const WAL_VERSION: u16 = 6;
const WAL_HEADER_SIZE: usize = 512;
const RECORD_HEADER_SIZE: usize = 26;

// ============================================================================
// Public Types
// ============================================================================

/// Log Sequence Number that identifies one position in the WAL.
///
/// The current implementation keeps a single log file, so `file_id` stays `0`.
/// The type still carries both fields because record headers and recovery code
/// already speak in terms of full LSNs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    /// Log file identifier for the record position.
    pub file_id: u32,
    /// Byte offset within the log file.
    pub offset: u64,
}

/// WAL record discriminator stored in each record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// One committed transaction record containing all logical operations.
    TxnCommit = 1,
    /// One checkpoint marker record.
    Checkpoint = 2,
}

/// WAL record payload used for recovery replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// Committed transaction payload.
    TxnCommit {
        /// Transaction identifier that produced the committed operations.
        txn_id: TxnId,
        /// Commit timestamp assigned when the transaction became durable.
        commit_ts: Timestamp,
        /// Logical operations replayed during recovery.
        ops: Vec<TxnLogOp>,
    },
    /// Checkpoint marker record.
    Checkpoint,
}

/// Fixed-size header that precedes each WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordHeader {
    /// Encoded [`WalRecordType`] discriminator.
    pub record_type: u8,
    /// Reserved record flags.
    pub flags: u8,
    /// Payload length in bytes.
    pub payload_len: u16,
    /// LSN assigned to this record.
    pub lsn: Lsn,
    /// LSN of the previous valid record in the chain.
    pub prev_lsn: Lsn,
    /// CRC32 over the header prefix plus payload bytes.
    pub crc32: u32,
}

/// Recovery-specific error type for sequential WAL replay.
#[derive(Debug)]
pub enum RecoveryError {
    /// Record bytes were read successfully but failed the CRC check.
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    /// Record chaining no longer matches the previous valid LSN.
    BrokenLsnChain {
        current_lsn: Lsn,
        expected_prev: Lsn,
        actual_prev: Lsn,
    },
    /// The fixed-size header is malformed.
    CorruptRecordHeader { offset: u64, details: String },
    /// The payload bytes are malformed for the decoded record type.
    CorruptRecordPayload {
        record_type: WalRecordType,
        offset: u64,
        details: String,
    },
    /// The file is not a usable WAL at all.
    InvalidWalFile(String),
    /// Underlying I/O failure while opening or reading the WAL.
    Io(std::io::Error),
}

/// Reader interface for sequential WAL replay.
pub trait WalReader {
    /// Read the next valid record from the current reader position.
    ///
    /// Returns `Ok(None)` on clean EOF and on a truncated tail where a full
    /// header or payload is no longer available.
    fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError>;
}

/// File-backed WAL reader used during recovery.
///
/// The reader starts at the checkpoint LSN recorded in the file header when
/// present, otherwise at the first record after the file header.
pub struct WalFileReader {
    file: File,
    current_offset: u64,
    last_valid_lsn: Lsn,
}

// ============================================================================
// Internal Types
// ============================================================================

/// WAL file header - written at the start of the WAL file.
#[derive(Debug, Clone)]
struct LogFileHeader {
    magic: [u8; 8],
    version: u16,
    last_lsn: Lsn,
    checkpoint_lsn: Lsn,
    crc32: u32,
}

#[derive(Debug)]
pub(crate) struct LogFile {
    file: File,
    header: LogFileHeader,
    write_buffer: Vec<u8>,
    buffer_capacity: usize,
    last_lsn: Lsn,
    last_record_lsn: Lsn,
}

// ============================================================================
// Lsn Implementation
// ============================================================================

impl Lsn {
    /// Construct one LSN from a log file identifier and byte offset.
    pub fn new(file_id: u32, offset: u64) -> Self {
        Self { file_id, offset }
    }

    /// Whether this LSN points at a real record position.
    pub fn is_valid(&self) -> bool {
        self.file_id != 0 || self.offset != 0
    }
}

// ============================================================================
// WalRecordType Implementation
// ============================================================================

impl TryFrom<u8> for WalRecordType {
    type Error = WrongoDBError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::TxnCommit),
            2 => Ok(Self::Checkpoint),
            _ => Err(StorageError(format!("invalid WAL record type: {value}")).into()),
        }
    }
}

// ============================================================================
// WalRecord Implementation
// ============================================================================

impl WalRecord {
    /// Return the discriminator stored in the corresponding record header.
    pub fn record_type(&self) -> WalRecordType {
        match self {
            WalRecord::TxnCommit { .. } => WalRecordType::TxnCommit,
            WalRecord::Checkpoint => WalRecordType::Checkpoint,
        }
    }

    fn serialize_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            WalRecord::TxnCommit {
                txn_id,
                commit_ts,
                ops,
            } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&commit_ts.to_le_bytes());
                buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
                for op in ops {
                    serialize_log_op(&mut buf, op);
                }
            }
            WalRecord::Checkpoint => {}
        }

        buf
    }

    fn deserialize_payload(record_type: WalRecordType, data: &[u8]) -> Result<Self, WrongoDBError> {
        let mut cursor = 0usize;

        let read_u64 = |data: &[u8], cursor: &mut usize| -> Result<u64, WrongoDBError> {
            if *cursor + 8 > data.len() {
                return Err(StorageError("unexpected EOF reading u64".into()).into());
            }
            let out = u64::from_le_bytes(
                data[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| StorageError("invalid u64 encoding".into()))?,
            );
            *cursor += 8;
            Ok(out)
        };

        match record_type {
            WalRecordType::TxnCommit => {
                let txn_id = read_u64(data, &mut cursor)?;
                let commit_ts = read_u64(data, &mut cursor)?;
                let ops_len = read_u32(data, &mut cursor)? as usize;
                let mut ops = Vec::with_capacity(ops_len);
                for _ in 0..ops_len {
                    ops.push(deserialize_log_op(data, &mut cursor)?);
                }
                Ok(WalRecord::TxnCommit {
                    txn_id,
                    commit_ts,
                    ops,
                })
            }
            WalRecordType::Checkpoint => Ok(WalRecord::Checkpoint),
        }
    }
}

// ============================================================================
// WalRecordHeader Implementation
// ============================================================================

impl WalRecordHeader {
    fn serialize(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        let mut cursor = 0usize;

        buf[cursor] = self.record_type;
        cursor += 1;
        buf[cursor] = self.flags;
        cursor += 1;
        buf[cursor..cursor + 2].copy_from_slice(&self.payload_len.to_le_bytes());
        cursor += 2;

        // reserved
        cursor += 2;

        buf[cursor..cursor + 8].copy_from_slice(&self.lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 8].copy_from_slice(&self.prev_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < RECORD_HEADER_SIZE {
            return Err(StorageError("WAL record header too short".into()).into());
        }

        let mut cursor = 0usize;

        let record_type = data[cursor];
        cursor += 1;
        let flags = data[cursor];
        cursor += 1;
        let payload_len = u16::from_le_bytes(
            data[cursor..cursor + 2]
                .try_into()
                .map_err(|_| StorageError("invalid payload length".into()))?,
        );
        cursor += 2;

        // reserved
        cursor += 2;

        let lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid record lsn".into()))?,
        );
        cursor += 8;

        let prev_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid prev lsn".into()))?,
        );
        cursor += 8;

        let crc32 = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid record crc".into()))?,
        );

        Ok(Self {
            record_type,
            flags,
            payload_len,
            lsn: Lsn::new(0, lsn_offset),
            prev_lsn: Lsn::new(0, prev_lsn_offset),
            crc32,
        })
    }
}

// ============================================================================
// RecoveryError Implementation
// ============================================================================

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryError::ChecksumMismatch {
                offset,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "WAL checksum mismatch at offset {offset}: expected {expected:08x}, got {actual:08x}"
                )
            }
            RecoveryError::BrokenLsnChain {
                current_lsn,
                expected_prev,
                actual_prev,
            } => {
                write!(
                    f,
                    "WAL LSN chain broken at LSN {current_lsn:?}: expected prev LSN {expected_prev:?}, got {actual_prev:?}"
                )
            }
            RecoveryError::CorruptRecordHeader { offset, details } => {
                write!(f, "WAL corrupt record header at offset {offset}: {details}")
            }
            RecoveryError::CorruptRecordPayload {
                record_type,
                offset,
                details,
            } => {
                write!(
                    f,
                    "WAL corrupt payload for record type {record_type:?} at offset {offset}: {details}"
                )
            }
            RecoveryError::InvalidWalFile(msg) => write!(f, "Invalid WAL file: {msg}"),
            RecoveryError::Io(err) => write!(f, "WAL IO error: {err}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<std::io::Error> for RecoveryError {
    fn from(value: std::io::Error) -> Self {
        RecoveryError::Io(value)
    }
}

// ============================================================================
// WalFileReader Implementation
// ============================================================================

impl WalFileReader {
    /// Open one WAL file for sequential recovery replay.
    ///
    /// The file header is validated before any record is read. If the header
    /// contains a checkpoint LSN, the reader starts from that offset so replay
    /// skips records older than the last completed checkpoint.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RecoveryError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(RecoveryError::InvalidWalFile(format!(
                "WAL file does not exist: {}",
                path.display()
            )));
        }

        let mut file = File::open(path)?;

        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = LogFileHeader::deserialize(&header_bytes)
            .map_err(|e| RecoveryError::InvalidWalFile(format!("invalid header: {e}")))?;

        if !header.validate_crc() {
            return Err(RecoveryError::InvalidWalFile(
                "WAL header CRC32 mismatch".to_string(),
            ));
        }

        let current_offset = if header.checkpoint_lsn.is_valid() {
            header.checkpoint_lsn.offset
        } else {
            WAL_HEADER_SIZE as u64
        };

        Ok(Self {
            file,
            current_offset,
            last_valid_lsn: Lsn::new(0, 0),
        })
    }
}

impl WalReader for WalFileReader {
    fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError> {
        self.file.seek(SeekFrom::Start(self.current_offset))?;

        let mut header_bytes = vec![0u8; RECORD_HEADER_SIZE];
        let bytes_read = self.file.read(&mut header_bytes)?;

        if bytes_read == 0 {
            return Ok(None);
        }

        if bytes_read < RECORD_HEADER_SIZE {
            return Ok(None);
        }

        let header = WalRecordHeader::deserialize(&header_bytes).map_err(|e| {
            RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: e.to_string(),
            }
        })?;

        let payload_len = header.payload_len as usize;
        let mut payload = vec![0u8; payload_len];
        let payload_bytes_read = self.file.read(&mut payload)?;

        if payload_bytes_read < payload_len {
            return Ok(None);
        }

        let crc_header_len = 1 + 1 + 2 + 2 + 8 + 8;
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

        if self.last_valid_lsn.is_valid() && header.prev_lsn != self.last_valid_lsn {
            return Err(RecoveryError::BrokenLsnChain {
                current_lsn: header.lsn,
                expected_prev: self.last_valid_lsn,
                actual_prev: header.prev_lsn,
            });
        }

        let record_type = WalRecordType::try_from(header.record_type).map_err(|_| {
            RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: format!("invalid record type: {}", header.record_type),
            }
        })?;

        let record = WalRecord::deserialize_payload(record_type, &payload).map_err(|e| {
            RecoveryError::CorruptRecordPayload {
                record_type,
                offset: self.current_offset + RECORD_HEADER_SIZE as u64,
                details: e.to_string(),
            }
        })?;

        self.last_valid_lsn = header.lsn;
        self.current_offset += (RECORD_HEADER_SIZE + payload_len) as u64;

        Ok(Some((header, record)))
    }
}

// ============================================================================
// LogFileHeader Implementation
// ============================================================================

impl LogFileHeader {
    fn new() -> Self {
        let mut header = Self {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_lsn: Lsn::new(0, 0),
            checkpoint_lsn: Lsn::new(0, 0),
            crc32: 0,
        };
        header.crc32 = header.compute_crc32();
        header
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        let mut cursor = 0usize;

        buf[cursor..cursor + 8].copy_from_slice(&self.magic);
        cursor += 8;

        buf[cursor..cursor + 2].copy_from_slice(&self.version.to_le_bytes());
        cursor += 2;

        // reserved
        cursor += 2;

        // reserved
        cursor += 4;

        buf[cursor..cursor + 4].copy_from_slice(&self.last_lsn.file_id.to_le_bytes());
        cursor += 4;
        buf[cursor..cursor + 8].copy_from_slice(&self.last_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.checkpoint_lsn.file_id.to_le_bytes());
        cursor += 4;
        buf[cursor..cursor + 8].copy_from_slice(&self.checkpoint_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(StorageError("WAL header too short".into()).into());
        }

        let mut cursor = 0usize;

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[cursor..cursor + 8]);
        cursor += 8;

        if magic != *WAL_MAGIC {
            return Err(StorageError(format!("invalid WAL magic: {magic:?}")).into());
        }

        let version = u16::from_le_bytes(
            data[cursor..cursor + 2]
                .try_into()
                .map_err(|_| StorageError("invalid WAL version".into()))?,
        );
        if version != WAL_VERSION {
            return Err(StorageError(format!("unsupported WAL version: {version}")).into());
        }

        cursor += 4; // version + reserved
        cursor += 4; // reserved

        let last_lsn_file_id = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid last_lsn file_id".into()))?,
        );
        cursor += 4;
        let last_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid last_lsn offset".into()))?,
        );
        cursor += 8;

        let checkpoint_lsn_file_id = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid checkpoint_lsn file_id".into()))?,
        );
        cursor += 4;
        let checkpoint_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid checkpoint_lsn offset".into()))?,
        );
        cursor += 8;

        let crc32 = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid WAL header checksum".into()))?,
        );

        Ok(Self {
            magic,
            version,
            last_lsn: Lsn::new(last_lsn_file_id, last_lsn_offset),
            checkpoint_lsn: Lsn::new(checkpoint_lsn_file_id, checkpoint_lsn_offset),
            crc32,
        })
    }

    fn compute_crc32(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&[0u8, 0u8]);
        hasher.update(&[0u8; 4]);
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

// ============================================================================
// LogFile Implementation
// ============================================================================

impl LogFile {
    const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;

    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        if path.exists() {
            return Self::open(path);
        }

        Self::create(path)
    }

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let mut file = File::create(path)?;

        let header = LogFileHeader::new();
        let header_bytes = header.serialize();
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(Self {
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn: Lsn::new(0, WAL_HEADER_SIZE as u64),
            last_record_lsn: Lsn::new(0, 0),
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let mut header = LogFileHeader::deserialize(&header_bytes)?;

        if !header.validate_crc() {
            return Err(StorageError("WAL header CRC32 mismatch".into()).into());
        }

        let (last_record_lsn, mut write_offset, truncate_reason) = scan_wal_tail(path)?;
        let file_len = file.metadata()?.len();
        if write_offset > file_len {
            write_offset = file_len;
        }

        if write_offset < file_len {
            if let Some(reason) = truncate_reason {
                eprintln!("Truncating invalid WAL tail from offset {write_offset}: {reason}");
            }
            file.set_len(write_offset)?;
        }

        if header.last_lsn != last_record_lsn {
            header.last_lsn = last_record_lsn;
            header.crc32 = header.compute_crc32();
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header.serialize())?;
        }

        file.seek(SeekFrom::Start(write_offset))?;

        Ok(Self {
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn: Lsn::new(0, write_offset),
            last_record_lsn,
        })
    }

    // ------------------------------------------------------------------------
    // Checkpoint Operations
    // ------------------------------------------------------------------------

    #[cfg(test)]
    fn set_checkpoint_lsn(&mut self, lsn: Lsn) -> Result<(), WrongoDBError> {
        self.header.checkpoint_lsn = lsn;
        self.header.crc32 = self.header.compute_crc32();

        let header_bytes = self.header.serialize();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub(crate) fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.sync()?;
        self.file.set_len(WAL_HEADER_SIZE as u64)?;

        self.last_lsn = Lsn::new(0, WAL_HEADER_SIZE as u64);
        self.last_record_lsn = Lsn::new(0, 0);
        self.header.last_lsn = Lsn::new(0, 0);
        self.header.checkpoint_lsn = Lsn::new(0, 0);
        self.header.crc32 = self.header.compute_crc32();
        let header_bytes = self.header.serialize();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Sync
    // ------------------------------------------------------------------------

    pub(crate) fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.flush_buffer()?;
        self.file.sync_all()?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Logging Operations
    // ------------------------------------------------------------------------

    pub(crate) fn log_txn_commit(
        &mut self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        ops: &[TxnLogOp],
    ) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::TxnCommit {
            txn_id,
            commit_ts,
            ops: ops.to_vec(),
        })
    }

    pub(crate) fn log_checkpoint(&mut self) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::Checkpoint)
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn append_record(&mut self, record: WalRecord) -> Result<Lsn, WrongoDBError> {
        let payload = record.serialize_payload();
        let record_type = record.record_type() as u8;
        let payload_len = payload.len() as u16;

        let lsn = self.last_lsn;
        let prev_lsn = self.last_record_lsn;

        let mut hasher = Hasher::new();
        hasher.update(&[record_type, 0]);
        hasher.update(&payload_len.to_le_bytes());
        hasher.update(&[0u8, 0u8]);
        hasher.update(&lsn.offset.to_le_bytes());
        hasher.update(&prev_lsn.offset.to_le_bytes());
        hasher.update(&payload);
        let crc32 = hasher.finalize();

        let header = WalRecordHeader {
            record_type,
            flags: 0,
            payload_len,
            lsn,
            prev_lsn,
            crc32,
        };

        self.write_buffered(&header.serialize())?;
        self.write_buffered(&payload)?;

        let record_size = RECORD_HEADER_SIZE + payload.len();
        self.last_lsn = Lsn::new(
            self.last_lsn.file_id,
            self.last_lsn.offset + record_size as u64,
        );
        self.last_record_lsn = lsn;
        self.header.last_lsn = lsn;

        Ok(lsn)
    }

    fn flush_buffer(&mut self) -> Result<(), WrongoDBError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        self.file.write_all(&self.write_buffer)?;
        self.write_buffer.clear();
        Ok(())
    }

    fn write_buffered(&mut self, data: &[u8]) -> Result<(), WrongoDBError> {
        if self.write_buffer.len() + data.len() > self.buffer_capacity {
            self.flush_buffer()?;
        }
        self.write_buffer.extend_from_slice(data);
        Ok(())
    }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn serialize_log_op(buf: &mut Vec<u8>, op: &TxnLogOp) {
    match op {
        TxnLogOp::Put {
            store_id,
            key,
            value,
        } => {
            buf.push(1);
            buf.extend_from_slice(&store_id.to_le_bytes());
            serialize_bytes(buf, key);
            serialize_bytes(buf, value);
        }
        TxnLogOp::Delete { store_id, key } => {
            buf.push(2);
            buf.extend_from_slice(&store_id.to_le_bytes());
            serialize_bytes(buf, key);
        }
    }
}

fn serialize_bytes(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32, WrongoDBError> {
    if *cursor + 4 > data.len() {
        return Err(StorageError("unexpected EOF reading u32".into()).into());
    }
    let out = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError("invalid u32 encoding".into()))?,
    );
    *cursor += 4;
    Ok(out)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64, WrongoDBError> {
    if *cursor + 8 > data.len() {
        return Err(StorageError("unexpected EOF reading u64".into()).into());
    }
    let out = u64::from_le_bytes(
        data[*cursor..*cursor + 8]
            .try_into()
            .map_err(|_| StorageError("invalid u64 encoding".into()))?,
    );
    *cursor += 8;
    Ok(out)
}

fn deserialize_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>, WrongoDBError> {
    if *cursor + 4 > data.len() {
        return Err(StorageError("unexpected EOF reading length".into()).into());
    }
    let len = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError("invalid length encoding".into()))?,
    ) as usize;
    *cursor += 4;
    if *cursor + len > data.len() {
        return Err(StorageError("unexpected EOF reading bytes".into()).into());
    }
    let out = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(out)
}

fn deserialize_log_op(data: &[u8], cursor: &mut usize) -> Result<TxnLogOp, WrongoDBError> {
    if *cursor >= data.len() {
        return Err(StorageError("unexpected EOF reading txn log op type".into()).into());
    }

    let op_type = data[*cursor];
    *cursor += 1;

    match op_type {
        1 => {
            let store_id = read_u64(data, cursor)?;
            let key = deserialize_bytes(data, cursor)?;
            let value = deserialize_bytes(data, cursor)?;
            Ok(TxnLogOp::Put {
                store_id,
                key,
                value,
            })
        }
        2 => {
            let store_id = read_u64(data, cursor)?;
            let key = deserialize_bytes(data, cursor)?;
            Ok(TxnLogOp::Delete { store_id, key })
        }
        _ => Err(StorageError(format!("invalid txn log op type: {op_type}")).into()),
    }
}

fn scan_wal_tail(path: &Path) -> Result<(Lsn, u64, Option<String>), WrongoDBError> {
    let mut reader = WalFileReader::open(path).map_err(|e| {
        StorageError(format!(
            "failed to open WAL reader while scanning tail: {e}"
        ))
    })?;

    let mut last_record_lsn = Lsn::new(0, 0);
    let mut truncate_reason = None;

    let write_offset = loop {
        match reader.read_record() {
            Ok(Some((header, _record))) => {
                last_record_lsn = header.lsn;
            }
            Ok(None) => {
                break reader.current_offset;
            }
            Err(
                err @ (RecoveryError::ChecksumMismatch { .. }
                | RecoveryError::BrokenLsnChain { .. }
                | RecoveryError::CorruptRecordHeader { .. }
                | RecoveryError::CorruptRecordPayload { .. }),
            ) => {
                truncate_reason = Some(err.to_string());
                break reader.current_offset;
            }
            Err(err) => {
                return Err(StorageError(format!("failed while scanning WAL tail: {err}")).into());
            }
        }
    };

    Ok((last_record_lsn, write_offset, truncate_reason))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;
    use crate::storage::reserved_store::{FIRST_DYNAMIC_STORE_ID, METADATA_STORE_ID};

    const TEST_STORE_ID: u64 = FIRST_DYNAMIC_STORE_ID;

    fn read_header(path: &Path) -> LogFileHeader {
        let mut file = File::open(path).unwrap();
        let mut bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut bytes).unwrap();
        LogFileHeader::deserialize(&bytes).unwrap()
    }

    fn wal_path(dir: &Path) -> std::path::PathBuf {
        dir.join(GLOBAL_WAL_FILE_NAME)
    }

    #[test]
    fn wal_record_round_trip() {
        let input = WalRecord::TxnCommit {
            txn_id: 42,
            commit_ts: 42,
            ops: vec![TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        };

        let payload = input.serialize_payload();
        let out = WalRecord::deserialize_payload(WalRecordType::TxnCommit, &payload).unwrap();

        assert_eq!(input, out);
    }

    #[test]
    fn wal_record_round_trip_reserved_metadata_uri() {
        let input = WalRecord::TxnCommit {
            txn_id: 42,
            commit_ts: 42,
            ops: vec![TxnLogOp::Put {
                store_id: METADATA_STORE_ID,
                key: b"table:users".to_vec(),
                value: b"metadata-row".to_vec(),
            }],
        };

        let payload = input.serialize_payload();
        let out = WalRecord::deserialize_payload(WalRecordType::TxnCommit, &payload).unwrap();

        assert_eq!(input, out);
    }

    #[test]
    fn wal_record_header_round_trip_preserves_lsn_chain() {
        let input = WalRecordHeader {
            record_type: WalRecordType::TxnCommit as u8,
            flags: 1,
            payload_len: 77,
            lsn: Lsn::new(0, 4096),
            prev_lsn: Lsn::new(0, 2048),
            crc32: 0x11223344,
        };

        let encoded = input.serialize();
        let out = WalRecordHeader::deserialize(&encoded).unwrap();

        assert_eq!(input, out);
    }

    #[test]
    fn create_log_and_read() {
        let dir = tempdir().unwrap();
        let wal_path = wal_path(dir.path());
        let mut wal = LogFile::open_or_create(&wal_path).unwrap();

        wal.log_txn_commit(
            1,
            1,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        )
        .unwrap();
        wal.sync().unwrap();

        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let (header, record) = reader.read_record().unwrap().unwrap();
        assert_eq!(header.lsn, Lsn::new(0, WAL_HEADER_SIZE as u64));
        assert_eq!(header.prev_lsn, Lsn::new(0, 0));

        match record {
            WalRecord::TxnCommit {
                txn_id,
                commit_ts,
                ops,
            } => {
                assert_eq!(txn_id, 1);
                assert_eq!(commit_ts, 1);
                assert_eq!(
                    ops,
                    vec![TxnLogOp::Put {
                        store_id: TEST_STORE_ID,
                        key: b"k1".to_vec(),
                        value: b"v1".to_vec(),
                    }]
                );
            }
            other => panic!("unexpected record: {other:?}"),
        }
    }

    #[test]
    fn wal_crc_mismatch_when_header_bytes_are_corrupted() {
        let dir = tempdir().unwrap();
        let wal_path = wal_path(dir.path());
        let mut wal = LogFile::open_or_create(&wal_path).unwrap();
        wal.log_txn_commit(
            1,
            1,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        )
        .unwrap();
        wal.sync().unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        let header_byte_offset = WAL_HEADER_SIZE as u64 + 6;
        file.seek(SeekFrom::Start(header_byte_offset)).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();

        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, RecoveryError::ChecksumMismatch { .. }));
    }

    #[test]
    fn lsn_offsets_advance_monotonically() {
        let dir = tempdir().unwrap();
        let wal_path = wal_path(dir.path());
        let mut wal = LogFile::open_or_create(&wal_path).unwrap();

        wal.log_txn_commit(
            1,
            1,
            &[
                TxnLogOp::Put {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                TxnLogOp::Delete {
                    store_id: TEST_STORE_ID,
                    key: b"k1".to_vec(),
                },
            ],
        )
        .unwrap();
        wal.log_checkpoint().unwrap();
        wal.sync().unwrap();

        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let mut headers = Vec::new();
        while let Some((header, _)) = reader.read_record().unwrap() {
            headers.push(header);
        }

        assert_eq!(headers.len(), 2);
        assert!(headers[1].lsn.offset > headers[0].lsn.offset);
        assert_eq!(headers[1].prev_lsn, headers[0].lsn);
    }

    #[test]
    fn truncate_then_append_restarts_record_sequence_after_checkpoint() {
        let dir = tempdir().unwrap();
        let wal_path = wal_path(dir.path());
        let mut wal = LogFile::open_or_create(&wal_path).unwrap();

        wal.log_txn_commit(
            1,
            1,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        )
        .unwrap();
        let checkpoint_lsn = wal.log_checkpoint().unwrap();
        wal.set_checkpoint_lsn(checkpoint_lsn).unwrap();
        wal.sync().unwrap();
        wal.truncate_to_checkpoint().unwrap();

        wal.log_txn_commit(
            2,
            2,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            }],
        )
        .unwrap();
        wal.sync().unwrap();

        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let (header, _) = reader.read_record().unwrap().unwrap();
        assert_eq!(header.lsn, Lsn::new(0, WAL_HEADER_SIZE as u64));
        assert_eq!(header.prev_lsn, Lsn::new(0, 0));
    }

    #[test]
    fn truncate_clears_checkpoint_and_last_lsn_metadata() {
        let dir = tempdir().unwrap();
        let wal_path = wal_path(dir.path());
        let mut wal = LogFile::open_or_create(&wal_path).unwrap();

        wal.log_txn_commit(
            1,
            1,
            &[TxnLogOp::Put {
                store_id: TEST_STORE_ID,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        )
        .unwrap();
        let checkpoint_lsn = wal.log_checkpoint().unwrap();
        wal.set_checkpoint_lsn(checkpoint_lsn).unwrap();
        wal.sync().unwrap();
        wal.truncate_to_checkpoint().unwrap();

        let header = read_header(&wal_path);
        assert_eq!(header.last_lsn, Lsn::new(0, 0));
        assert_eq!(header.checkpoint_lsn, Lsn::new(0, 0));
    }

    #[test]
    fn wal_v1_header_is_rejected() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("legacy.wal");

        let mut header = LogFileHeader {
            magic: *WAL_MAGIC,
            version: 1,
            last_lsn: Lsn::new(0, 0),
            checkpoint_lsn: Lsn::new(0, 0),
            crc32: 0,
        };
        header.crc32 = header.compute_crc32();
        fs::write(&wal_path, header.serialize()).unwrap();

        let err = match WalFileReader::open(&wal_path) {
            Ok(_) => panic!("expected v1 header to be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("unsupported WAL version: 1"));
    }
}
