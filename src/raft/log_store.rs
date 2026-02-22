use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::core::errors::StorageError;
use crate::raft::protocol::ProtocolLogEntry;
use crate::WrongoDBError;

const RAFT_LOG_FILE: &str = "raft_log.bin";
const RAFT_LOG_MAGIC: &[u8; 8] = b"RFTLOG01";
const RAFT_LOG_VERSION: u16 = 1;
const RAFT_LOG_HEADER_SIZE: usize = 16;
const RAFT_LOG_RECORD_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub(crate) struct RaftLogStore {
    file: std::fs::File,
    entries: Vec<ProtocolLogEntry>,
    entry_offsets: Vec<u64>,
}

impl RaftLogStore {
    pub(crate) fn path_for_db<P: AsRef<Path>>(db_dir: P) -> PathBuf {
        db_dir.as_ref().join(RAFT_LOG_FILE)
    }

    pub(crate) fn open_or_create<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let path = Self::path_for_db(db_dir);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        if file.metadata()?.len() == 0 {
            write_header(&mut file)?;
            file.sync_all()?;
        }

        read_and_validate_header(&mut file)?;

        let (entries, entry_offsets, truncate_to_offset) = scan_entries(&mut file)?;
        if let Some(offset) = truncate_to_offset {
            file.set_len(offset)?;
        }

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            entries,
            entry_offsets,
        })
    }

    pub(crate) fn entries(&self) -> &[ProtocolLogEntry] {
        &self.entries
    }

    pub(crate) fn last_log_index_term(&self) -> (u64, u64) {
        let index = self.entries.len() as u64;
        let term = self.entries.last().map(|entry| entry.term).unwrap_or(0);
        (index, term)
    }

    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }

        let zero_based = usize::try_from(index - 1).ok()?;
        self.entries.get(zero_based).map(|entry| entry.term)
    }

    pub(crate) fn truncate_from(&mut self, index: u64) -> Result<(), WrongoDBError> {
        let truncate_offset = if index == 0 {
            RAFT_LOG_HEADER_SIZE as u64
        } else {
            let Some(zero_based) = usize::try_from(index - 1).ok() else {
                return Ok(());
            };

            if zero_based >= self.entries.len() {
                return Ok(());
            }

            self.entry_offsets[zero_based]
        };

        self.file.set_len(truncate_offset)?;
        self.file.seek(SeekFrom::Start(truncate_offset))?;

        if index == 0 {
            self.entries.clear();
            self.entry_offsets.clear();
        } else {
            let keep = usize::try_from(index - 1).unwrap_or(0);
            self.entries.truncate(keep);
            self.entry_offsets.truncate(keep);
        }

        Ok(())
    }

    pub(crate) fn append_entries(
        &mut self,
        entries: &[ProtocolLogEntry],
    ) -> Result<(), WrongoDBError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut current_offset = self.file.seek(SeekFrom::End(0))?;
        for entry in entries {
            let payload_len = entry.payload.len() as u32;
            let crc32 = record_crc32(entry.term, payload_len, &entry.payload);

            let mut header = [0u8; RAFT_LOG_RECORD_HEADER_SIZE];
            let mut cursor = 0usize;
            header[cursor..cursor + 8].copy_from_slice(&entry.term.to_le_bytes());
            cursor += 8;
            header[cursor..cursor + 4].copy_from_slice(&payload_len.to_le_bytes());
            cursor += 4;
            header[cursor..cursor + 4].copy_from_slice(&crc32.to_le_bytes());

            self.file.write_all(&header)?;
            self.file.write_all(&entry.payload)?;

            self.entry_offsets.push(current_offset);
            self.entries.push(entry.clone());
            current_offset += RAFT_LOG_RECORD_HEADER_SIZE as u64 + payload_len as u64;
        }

        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.file.sync_all()?;
        Ok(())
    }
}

fn write_header(file: &mut std::fs::File) -> Result<(), WrongoDBError> {
    let mut header = [0u8; RAFT_LOG_HEADER_SIZE];
    let mut cursor = 0usize;
    header[cursor..cursor + 8].copy_from_slice(RAFT_LOG_MAGIC);
    cursor += 8;
    header[cursor..cursor + 2].copy_from_slice(&RAFT_LOG_VERSION.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    Ok(())
}

fn read_and_validate_header(file: &mut std::fs::File) -> Result<(), WrongoDBError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; RAFT_LOG_HEADER_SIZE];
    file.read_exact(&mut header)?;

    if &header[0..8] != RAFT_LOG_MAGIC {
        return Err(StorageError(format!("invalid raft log magic: {:?}", &header[0..8])).into());
    }

    let version = u16::from_le_bytes(
        header[8..10]
            .try_into()
            .map_err(|_| StorageError("invalid raft log version".into()))?,
    );
    if version != RAFT_LOG_VERSION {
        return Err(StorageError(format!("unsupported raft log version: {version}")).into());
    }

    Ok(())
}

fn scan_entries(
    file: &mut std::fs::File,
) -> Result<(Vec<ProtocolLogEntry>, Vec<u64>, Option<u64>), WrongoDBError> {
    let mut entries = Vec::new();
    let mut offsets = Vec::new();
    let mut cursor = RAFT_LOG_HEADER_SIZE as u64;
    let file_len = file.metadata()?.len();

    loop {
        if cursor == file_len {
            return Ok((entries, offsets, None));
        }

        if cursor + RAFT_LOG_RECORD_HEADER_SIZE as u64 > file_len {
            return Ok((entries, offsets, Some(cursor)));
        }

        file.seek(SeekFrom::Start(cursor))?;
        let mut record_header = [0u8; RAFT_LOG_RECORD_HEADER_SIZE];
        file.read_exact(&mut record_header)?;

        let term = u64::from_le_bytes(
            record_header[0..8]
                .try_into()
                .map_err(|_| StorageError("invalid raft log record term".into()))?,
        );
        let payload_len = u32::from_le_bytes(
            record_header[8..12]
                .try_into()
                .map_err(|_| StorageError("invalid raft log payload length".into()))?,
        ) as usize;
        let expected_crc = u32::from_le_bytes(
            record_header[12..16]
                .try_into()
                .map_err(|_| StorageError("invalid raft log checksum".into()))?,
        );

        let payload_offset = cursor + RAFT_LOG_RECORD_HEADER_SIZE as u64;
        let payload_end = payload_offset + payload_len as u64;
        if payload_end > file_len {
            return Ok((entries, offsets, Some(cursor)));
        }

        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)?;

        let actual_crc = record_crc32(term, payload_len as u32, &payload);
        if actual_crc != expected_crc {
            return Ok((entries, offsets, Some(cursor)));
        }

        offsets.push(cursor);
        entries.push(ProtocolLogEntry { term, payload });
        cursor = payload_end;
    }
}

fn record_crc32(term: u64, payload_len: u32, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&term.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::tempdir;

    use super::*;

    fn entry(term: u64, payload: &[u8]) -> ProtocolLogEntry {
        ProtocolLogEntry {
            term,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn open_missing_file_initializes_empty_log() {
        let dir = tempdir().unwrap();
        let path = RaftLogStore::path_for_db(dir.path());
        assert!(!path.exists());

        let store = RaftLogStore::open_or_create(dir.path()).unwrap();
        assert!(path.exists());
        assert!(store.entries().is_empty());
        assert_eq!(store.last_log_index_term(), (0, 0));
        assert_eq!(store.term_at(0), Some(0));
        assert_eq!(store.term_at(1), None);
    }

    #[test]
    fn append_and_reopen_round_trip() {
        let dir = tempdir().unwrap();
        {
            let mut store = RaftLogStore::open_or_create(dir.path()).unwrap();
            store
                .append_entries(&[entry(1, b"a"), entry(2, b"b"), entry(2, b"c")])
                .unwrap();
            store.sync().unwrap();
        }

        let reopened = RaftLogStore::open_or_create(dir.path()).unwrap();
        assert_eq!(reopened.last_log_index_term(), (3, 2));
        assert_eq!(reopened.term_at(1), Some(1));
        assert_eq!(reopened.term_at(2), Some(2));
        assert_eq!(reopened.term_at(3), Some(2));
    }

    #[test]
    fn truncate_then_append_persists_expected_suffix() {
        let dir = tempdir().unwrap();
        {
            let mut store = RaftLogStore::open_or_create(dir.path()).unwrap();
            store
                .append_entries(&[entry(1, b"a"), entry(2, b"b"), entry(3, b"c")])
                .unwrap();
            store.truncate_from(2).unwrap();
            store
                .append_entries(&[entry(4, b"x"), entry(4, b"y")])
                .unwrap();
            store.sync().unwrap();
        }

        let reopened = RaftLogStore::open_or_create(dir.path()).unwrap();
        assert_eq!(reopened.entries().len(), 3);
        assert_eq!(reopened.entries()[0], entry(1, b"a"));
        assert_eq!(reopened.entries()[1], entry(4, b"x"));
        assert_eq!(reopened.entries()[2], entry(4, b"y"));
    }

    #[test]
    fn crc_mismatch_is_treated_as_corrupt_tail() {
        let dir = tempdir().unwrap();
        let path = RaftLogStore::path_for_db(dir.path());

        {
            let mut store = RaftLogStore::open_or_create(dir.path()).unwrap();
            store
                .append_entries(&[entry(1, b"a"), entry(2, b"b"), entry(3, b"c")])
                .unwrap();
            store.sync().unwrap();
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let second_record_crc_offset =
            RAFT_LOG_HEADER_SIZE as u64 + (RAFT_LOG_RECORD_HEADER_SIZE as u64 + 1) + 12;
        file.seek(SeekFrom::Start(second_record_crc_offset))
            .unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();

        let reopened = RaftLogStore::open_or_create(dir.path()).unwrap();
        assert_eq!(reopened.entries().len(), 1);
        assert_eq!(reopened.entries()[0], entry(1, b"a"));
    }
}
