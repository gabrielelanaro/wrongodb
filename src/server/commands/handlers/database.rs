use crate::api::DatabaseContext;
use crate::core::errors::DocumentValidationError;
use crate::server::commands::Command;
use crate::WrongoDBError;
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};

/// Handles: listDatabases
pub struct ListDatabasesCommand;

impl Command for ListDatabasesCommand {
    fn names(&self) -> &[&str] {
        &["listDatabases"]
    }

    fn execute(&self, _doc: &Document, _db: &DatabaseContext) -> Result<Document, WrongoDBError> {
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

    fn execute(&self, _doc: &Document, db: &DatabaseContext) -> Result<Document, WrongoDBError> {
        let collections = db.list_collections()?;
        let collections_bson: Vec<Bson> = collections
            .into_iter()
            .filter_map(|name| {
                let definition = db.collection_definition(&name).ok()??;
                Some(Bson::Document(doc! {
                    "name": name.clone(),
                    "type": "collection",
                    "options": Bson::Document(definition.options().clone()),
                    "info": {
                        "readOnly": Bson::Boolean(false),
                        "uuid": Bson::Binary(Binary {
                            subtype: BinarySubtype::Uuid,
                            bytes: definition.uuid().bytes.clone(),
                        }),
                    },
                    "idIndex": {
                        "v": Bson::Int32(2),
                        "key": { "_id": Bson::Int32(1) },
                        "name": "_id_",
                    },
                }))
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

/// Handles: createCollection, create
pub struct CreateCollectionCommand;

impl Command for CreateCollectionCommand {
    fn names(&self) -> &[&str] {
        &["createCollection", "create"]
    }

    fn execute(&self, doc: &Document, db: &DatabaseContext) -> Result<Document, WrongoDBError> {
        let collection = doc
            .get_str("createCollection")
            .or_else(|_| doc.get_str("create"))
            .map_err(|_| {
                WrongoDBError::DocumentValidation(DocumentValidationError(
                    "createCollection requires a collection name".into(),
                ))
            })?;

        let storage_columns = parse_storage_columns(doc)?;
        let mut session = db.connection().open_session();
        db.collection_write_path()
            .create_collection(&mut session, collection, storage_columns)?;

        Ok(doc! {
            "ok": Bson::Double(1.0),
        })
    }
}

/// Handles: dbStats
pub struct DbStatsCommand;

impl Command for DbStatsCommand {
    fn names(&self) -> &[&str] {
        &["dbStats"]
    }

    fn execute(&self, _doc: &Document, db: &DatabaseContext) -> Result<Document, WrongoDBError> {
        let collections = db.list_collections()?;
        let mut document_count = 0usize;
        let mut index_count = 0usize;

        for name in &collections {
            let mut session = db.connection().open_session();
            document_count += db.document_query().count(&mut session, name, None)?;
            index_count += db.list_indexes(name)?.len();
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

    fn execute(&self, doc: &Document, db: &DatabaseContext) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("collStats").unwrap_or("test");
        let mut session = db.connection().open_session();
        let count = db.document_query().count(&mut session, coll_name, None)?;
        let index_count = db.list_indexes(coll_name)?.len() as i32 + 1;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "ns": format!("test.{}", coll_name),
            "count": Bson::Int64(count as i64),
            "size": Bson::Int64(0),
            "avgObjSize": Bson::Double(0.0),
            "storageSize": Bson::Int64(0),
            "nindexes": Bson::Int32(index_count),
            "totalIndexSize": Bson::Int64(0),
        })
    }
}

fn parse_storage_columns(doc: &Document) -> Result<Vec<String>, WrongoDBError> {
    let values = doc.get_array("storageColumns").map_err(|_| {
        WrongoDBError::DocumentValidation(DocumentValidationError(
            "createCollection requires a storageColumns array".into(),
        ))
    })?;

    values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                WrongoDBError::DocumentValidation(DocumentValidationError(
                    "storageColumns entries must be strings".into(),
                ))
            })
        })
        .collect()
}
