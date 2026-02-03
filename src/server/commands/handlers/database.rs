use crate::commands::Command;
use crate::{WrongoDB, WrongoDBError};
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};

/// Handles: listDatabases
pub struct ListDatabasesCommand;

impl Command for ListDatabasesCommand {
    fn names(&self) -> &[&str] {
        &["listDatabases"]
    }

    fn execute(&self, _doc: &Document, _db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        // TODO: When multi-database support is added, return actual databases
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

    fn execute(&self, _doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let collections = db.list_collections()?;
        let collections_bson: Vec<Bson> = collections
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                // Create a UUID-like binary from a simple hash
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

    fn execute(&self, _doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let stats = db.stats()?;
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "db": "test",
            "collections": Bson::Int32(stats.collection_count as i32),
            "objects": Bson::Int64(stats.document_count as i64),
            "avgObjSize": Bson::Double(0.0),
            "dataSize": Bson::Int64(0),
            "storageSize": Bson::Int64(0),
            "indexes": Bson::Int32(stats.index_count as i32),
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

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("collStats").unwrap_or("test");
        let coll = db.collection(coll_name);
        let mut session = db.open_session();

        let count = coll.count(&mut session, None)?;

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
