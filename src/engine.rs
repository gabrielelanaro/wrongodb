use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::document::{normalize_document, validate_is_object};
use crate::index::InMemoryIndex;
use crate::storage::AppendOnlyStorage;
use crate::{Document, WrongoDBError};

#[derive(Debug, Clone)]
struct Record {
    offset: u64,
    doc: Document,
}

#[derive(Debug)]
pub struct WrongoDB {
    path: PathBuf,
    storage: AppendOnlyStorage,
    index: InMemoryIndex,
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
        let mut db = Self {
            path,
            storage,
            index: InMemoryIndex::new(index_fields),
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
        let offset = self.storage.append(&normalized)?;
        self.index.add(&normalized, offset);
        self.docs.push(Record {
            offset,
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
        Ok(self.find(filter)?.into_iter().next())
    }

    fn load_existing(&mut self) -> Result<(), WrongoDBError> {
        let existing = self.storage.read_all()?;
        for (offset, doc) in existing {
            self.docs.push(Record {
                offset,
                doc: doc.clone(),
            });
            self.doc_by_offset.insert(offset, doc.clone());
            self.index.add(&doc, offset);
        }
        Ok(())
    }
}

pub type MiniMongo = WrongoDB;
