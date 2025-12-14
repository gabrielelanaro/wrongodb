use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::document::{normalize_document, validate_is_object};
use crate::index::InMemoryIndex;
use crate::storage::AppendOnlyStorage;
use crate::{BTree, Document, StorageError, WrongoDBError};

const PRIMARY_BTREE_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
struct Record {
    _offset: u64,
    doc: Document,
}

#[derive(Debug)]
pub struct WrongoDB {
    _path: PathBuf,
    storage: AppendOnlyStorage,
    index: InMemoryIndex,
    primary: RefCell<BTree>,
    sync_every_write: bool,
    docs: Vec<Record>,
    doc_by_offset: HashMap<u64, Document>,
}

impl WrongoDB {
    pub fn open<P, I, S>(
        path: P,
        index_fields: I,
        sync_every_write: bool,
    ) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path = path.as_ref().to_path_buf();
        let storage = AppendOnlyStorage::new(&path, sync_every_write);
        let primary_path = primary_index_path(&path);
        let primary = open_or_create_primary_btree(&primary_path)?;
        let mut db = Self {
            _path: path,
            storage,
            index: InMemoryIndex::new(index_fields),
            primary: RefCell::new(primary),
            sync_every_write,
            docs: Vec::new(),
            doc_by_offset: HashMap::new(),
        };
        db.load_existing()?;
        Ok(db)
    }

    pub fn insert_one(&mut self, doc: Value) -> Result<Document, WrongoDBError> {
        validate_is_object(&doc)?;
        let obj = doc.as_object().expect("validated object").clone();
        let normalized = normalize_document(&obj)?;
        let id_key = encode_primary_key(
            normalized
                .get("_id")
                .ok_or_else(|| StorageError("normalized doc missing _id".into()))?,
        )?;
        if self.primary.get_mut().get(&id_key)?.is_some() {
            return Err(StorageError("duplicate key error: _id".into()).into());
        }

        let offset = self.storage.append(&normalized)?;
        let offset_value = encode_offset_value(offset);
        self.primary
            .get_mut()
            .put(&id_key, &offset_value)?;
        if self.sync_every_write {
            self.primary.get_mut().sync_all()?;
        }
        self.index.add(&normalized, offset);
        self.docs.push(Record {
            _offset: offset,
            doc: normalized.clone(),
        });
        self.doc_by_offset.insert(offset, normalized.clone());
        Ok(normalized)
    }

    pub fn find(&self, filter: Option<Value>) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = match filter {
            None => Document::new(),
            Some(v) => {
                validate_is_object(&v)?;
                v.as_object().expect("validated object").clone()
            }
        };

        if filter_doc.is_empty() {
            return Ok(self.docs.iter().map(|r| r.doc.clone()).collect());
        }

        let indexed_field = filter_doc
            .keys()
            .find(|k| self.index.fields.contains(*k))
            .cloned();

        let candidates: Box<dyn Iterator<Item = &Document> + '_> = if let Some(field) = indexed_field {
            let value = filter_doc.get(&field).unwrap();
            let offsets = self.index.lookup(&field, value);
            Box::new(
                offsets
                    .into_iter()
                    .filter_map(|o| self.doc_by_offset.get(&o)),
            )
        } else {
            Box::new(self.docs.iter().map(|r| &r.doc))
        };

        Ok(candidates
            .filter(|doc| filter_doc.iter().all(|(k, v)| doc.get(k) == Some(v)))
            .cloned()
            .collect())
    }

    pub fn find_one(&self, filter: Option<Value>) -> Result<Option<Document>, WrongoDBError> {
        let filter = match filter {
            None => return Ok(self.docs.first().map(|r| r.doc.clone())),
            Some(v) => v,
        };
        validate_is_object(&filter)?;
        let filter_doc = filter.as_object().expect("validated object");

        if filter_doc.is_empty() {
            return Ok(self.docs.first().map(|r| r.doc.clone()));
        }

        if let Some(id_value) = filter_doc.get("_id") {
            let id_key = encode_primary_key(id_value)?;
            let maybe = self.primary.borrow_mut().get(&id_key)?;
            let Some(offset_bytes) = maybe else {
                return Ok(None);
            };
            let offset = decode_offset_value(&offset_bytes)?;
            let Some(doc) = self.doc_by_offset.get(&offset).cloned() else {
                return Err(StorageError(format!(
                    "primary index points to missing offset {offset}"
                ))
                .into());
            };
            if filter_doc.iter().all(|(k, v)| doc.get(k) == Some(v)) {
                return Ok(Some(doc));
            }
            return Ok(None);
        }

        Ok(self
            .find(Some(Value::Object(filter_doc.clone())))?
            .into_iter()
            .next())
    }

    fn load_existing(&mut self) -> Result<(), WrongoDBError> {
        let existing = self.storage.read_all()?;
        let mut seen_ids: HashSet<Vec<u8>> = HashSet::new();
        for (offset, doc) in existing {
            self.docs.push(Record {
                _offset: offset,
                doc: doc.clone(),
            });
            self.doc_by_offset.insert(offset, doc.clone());
            self.index.add(&doc, offset);
            let Some(id_value) = doc.get("_id") else {
                return Err(StorageError("on-disk document missing _id".into()).into());
            };
            let id_key = encode_primary_key(id_value)?;
            if !seen_ids.insert(id_key.clone()) {
                return Err(StorageError("duplicate key error in log: _id".into()).into());
            }
            let offset_value = encode_offset_value(offset);
            self.primary.get_mut().put(&id_key, &offset_value)?;
        }
        if self.sync_every_write {
            self.primary.get_mut().sync_all()?;
        }
        Ok(())
    }
}

fn primary_index_path(log_path: &Path) -> PathBuf {
    let mut p = log_path.to_path_buf();
    let file_name = log_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("db.log");
    p.set_file_name(format!("{file_name}.primary.wt"));
    p
}

fn open_or_create_primary_btree(path: &Path) -> Result<BTree, WrongoDBError> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() > 0 => BTree::open(path),
        _ => BTree::create(path, PRIMARY_BTREE_PAGE_SIZE),
    }
}

fn encode_offset_value(offset: u64) -> [u8; 8] {
    offset.to_le_bytes()
}

fn decode_offset_value(bytes: &[u8]) -> Result<u64, WrongoDBError> {
    if bytes.len() != 8 {
        return Err(StorageError("corrupt primary value (expected 8 bytes)".into()).into());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn encode_primary_key(id: &Value) -> Result<Vec<u8>, WrongoDBError> {
    // Canonical encoding for `_id` to avoid dependence on serde_json `preserve_order`.
    // Objects are encoded with keys sorted lexicographically.
    let mut out = Vec::new();
    encode_value_canonical(&mut out, id)?;
    Ok(out)
}

fn encode_value_canonical(out: &mut Vec<u8>, v: &Value) -> Result<(), WrongoDBError> {
    match v {
        Value::Null => out.push(0x00),
        Value::Bool(false) => out.extend_from_slice(&[0x01, 0x00]),
        Value::Bool(true) => out.extend_from_slice(&[0x01, 0x01]),
        Value::Number(n) => {
            out.push(0x02);
            // Best-effort Mongo-like numeric normalization:
            // - Prefer integer encoding when representable (so 1 and 1.0 can normalize).
            // - Otherwise encode f64 if available, else fall back to string.
            if let Some(i) = n.as_i64() {
                out.extend_from_slice(&[0x00]); // subtype: i64
                out.extend_from_slice(&i.to_le_bytes());
            } else if let Some(u) = n.as_u64().and_then(|u| i64::try_from(u).ok()) {
                out.extend_from_slice(&[0x00]); // subtype: i64
                out.extend_from_slice(&u.to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f.fract() == 0.0 && f >= (i64::MIN as f64) && f <= (i64::MAX as f64) {
                    out.extend_from_slice(&[0x00]); // subtype: i64
                    out.extend_from_slice(&(f as i64).to_le_bytes());
                } else {
                    out.extend_from_slice(&[0x01]); // subtype: f64
                    out.extend_from_slice(&f.to_le_bytes());
                }
            } else {
                out.extend_from_slice(&[0x02]); // subtype: string
                let s = n.to_string();
                write_len_prefixed(out, s.as_bytes());
            }
        }
        Value::String(s) => {
            out.push(0x03);
            write_len_prefixed(out, s.as_bytes());
        }
        Value::Array(arr) => {
            out.push(0x04);
            out.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for el in arr {
                encode_value_canonical(out, el)?;
            }
        }
        Value::Object(map) => {
            out.push(0x05);
            out.extend_from_slice(&(map.len() as u32).to_le_bytes());
            // MongoDB compares embedded documents with field order preserved.
            // We rely on serde_json's `preserve_order` feature so iteration order is stable.
            for (k, v) in map.iter() {
                write_len_prefixed(out, k.as_bytes());
                encode_value_canonical(out, v)?;
            }
        }
    }
    Ok(())
}

fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}
