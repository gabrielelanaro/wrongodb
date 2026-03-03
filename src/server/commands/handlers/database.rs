use std::fs;
use std::path::Path;

use crate::commands::Command;
use crate::{document_ops, Connection, WrongoDBError};
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};

/// Handles: listDatabases
pub struct ListDatabasesCommand;

impl Command for ListDatabasesCommand {
    fn names(&self) -> &[&str] {
        &["listDatabases"]
    }

    fn execute(&self, _doc: &Document, _conn: &Connection) -> Result<Document, WrongoDBError> {
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "databases": Bson::Array(vec![
                Bson::Document(doc! {
                    "name": "test",
                    "sizeOnDisk": Bson::Int64(0),
                    "empty": Bson::Boolean(false),
                })
            ]),
            "totalSize": Bson::Int64(0),
            "totalSizeMb": Bson::Int64(0),
        })
    }
}

/// Handles: listCollections
pub struct ListCollectionsCommand;

impl Command for ListCollectionsCommand {
    fn names(&self) -> &[&str] {
        &["listCollections"]
    }

    fn execute(&self, _doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let collections = list_collections(conn.base_path())?;
        let collections_bson: Vec<Bson> = collections
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                let mut uuid_bytes = [0u8; 16];
                let name_bytes = name.as_bytes();
                for (j, &b) in name_bytes.iter().take(16).enumerate() {
                    uuid_bytes[j] = b;
                }
                uuid_bytes[0] = (i as u8).wrapping_add(uuid_bytes[0]);

                Bson::Document(doc! {
                    "name": name.clone(),
                    "type": "collection",
                    "options": Bson::Document(Document::new()),
                    "info": {
                        "readOnly": Bson::Boolean(false),
                        "uuid": Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: uuid_bytes.to_vec() }),
                    },
                    "idIndex": {
                        "v": Bson::Int32(2),
                        "key": { "_id": Bson::Int32(1) },
                        "name": "_id_",
                    },
                })
            })
            .collect();

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(0),
                "ns": "test.$cmd.listCollections",
                "firstBatch": Bson::Array(collections_bson),
            },
        })
    }
}

/// Handles: dbStats
pub struct DbStatsCommand;

impl Command for DbStatsCommand {
    fn names(&self) -> &[&str] {
        &["dbStats"]
    }

    fn execute(&self, _doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let collections = list_collections(conn.base_path())?;
        let mut document_count = 0usize;
        let mut index_count = 0usize;

        for name in &collections {
            let mut session = conn.open_session();
            document_count += document_ops::count(&mut session, name, None)?;
            index_count += document_ops::list_indexes(&mut session, name)?.len();
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "db": "test",
            "collections": Bson::Int32(collections.len() as i32),
            "objects": Bson::Int64(document_count as i64),
            "avgObjSize": Bson::Double(0.0),
            "dataSize": Bson::Int64(0),
            "storageSize": Bson::Int64(0),
            "indexes": Bson::Int32(index_count as i32),
            "indexSize": Bson::Int64(0),
        })
    }
}

/// Handles: collStats
pub struct CollStatsCommand;

impl Command for CollStatsCommand {
    fn names(&self) -> &[&str] {
        &["collStats"]
    }

    fn execute(&self, doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("collStats").unwrap_or("test");
        let mut session = conn.open_session();
        let count = document_ops::count(&mut session, coll_name, None)?;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "ns": format!("test.{}", coll_name),
            "count": Bson::Int64(count as i64),
            "size": Bson::Int64(0),
            "avgObjSize": Bson::Double(0.0),
            "storageSize": Bson::Int64(0),
            "nindexes": Bson::Int32(1),
            "totalIndexSize": Bson::Int64(0),
        })
    }
}

pub(crate) fn list_collections(base: &Path) -> Result<Vec<String>, WrongoDBError> {
    let mut names = Vec::new();
    for entry in fs::read_dir(base)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = match file_name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some(name) = file_name.strip_suffix(".main.wt") {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}
