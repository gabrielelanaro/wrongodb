use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::errors::StorageError;
use crate::{Document, WrongoDBError};

#[derive(Debug)]
pub struct AppendOnlyStorage {
    path: PathBuf,
    sync_every_write: bool,
}

impl AppendOnlyStorage {
    pub fn new<P: AsRef<Path>>(path: P, sync_every_write: bool) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            sync_every_write,
        }
    }

    pub fn append(&self, doc: &Document) -> Result<u64, WrongoDBError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;

        file.seek(SeekFrom::End(0))?;
        let offset = file.stream_position()?;

        let mut payload = serde_json::to_vec(doc)?;
        payload.push(b'\n');

        file.write_all(&payload)?;
        file.flush()?;
        if self.sync_every_write {
            file.sync_all()?;
        }

        Ok(offset)
    }

    pub fn read_all(&self) -> Result<Vec<(u64, Document)>, WrongoDBError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        let mut results = Vec::new();
        loop {
            let offset = reader.stream_position()?;
            let mut line = String::new();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(&line)?;
            let Some(obj) = value.as_object() else {
                return Err(StorageError("encountered non-object document in log".into()).into());
            };
            results.push((offset, obj.clone()));
        }

        Ok(results)
    }
}
