use super::crud::namespace_from_field;
use super::cursor::{create_materialized_cursor_response, default_batch_size};
use crate::api::DatabaseContext;
use crate::core::errors::DocumentValidationError;
use crate::core::Namespace;
use crate::server::commands::{Command, CommandContext};
use crate::WrongoDBError;
use async_trait::async_trait;
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};

/// Handles: listDatabases
pub struct ListDatabasesCommand;

#[async_trait]
impl Command for ListDatabasesCommand {
    fn names(&self) -> &[&str] {
        &["listDatabases"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        _doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        if ctx.db_name().as_str() != "admin" {
            return Err(WrongoDBError::Protocol(
                "listDatabases must run against the admin database".into(),
            ));
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "databases": Bson::Array(
                db.list_databases()
                    .into_iter()
                    .map(|name| {
                        Bson::Document(doc! {
                            "name": name.as_str(),
                            "sizeOnDisk": Bson::Int64(0),
                            "empty": Bson::Boolean(false),
                        })
                    })
                    .collect()
            ),
            "totalSize": Bson::Int64(0),
            "totalSizeMb": Bson::Int64(0),
        })
    }
}

/// Handles: listCollections
pub struct ListCollectionsCommand;

#[async_trait]
impl Command for ListCollectionsCommand {
    fn names(&self) -> &[&str] {
        &["listCollections"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        _doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let collections = db.list_collections(ctx.db_name())?;
        let collections_docs: Vec<Document> = collections
            .into_iter()
            .filter_map(|name| {
                let namespace = Namespace::new(ctx.db_name().clone(), name.clone()).ok()?;
                let definition = db.collection_definition(&namespace).ok()??;
                Some(doc! {
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
                })
            })
            .collect();

        let cursor_namespace = Namespace::list_collections_cursor(ctx.db_name().clone())?;
        Ok(create_materialized_cursor_response(
            ctx,
            cursor_namespace,
            collections_docs,
            default_batch_size(_doc),
        ))
    }
}

/// Handles: createCollection, create
pub struct CreateCollectionCommand;

#[async_trait]
impl Command for CreateCollectionCommand {
    fn names(&self) -> &[&str] {
        &["createCollection", "create"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let collection = doc
            .get_str("createCollection")
            .or_else(|_| doc.get_str("create"))
            .map_err(|_| {
                WrongoDBError::DocumentValidation(DocumentValidationError(
                    "createCollection requires a collection name".into(),
                ))
            })?;

        let storage_columns = parse_storage_columns(doc)?;
        let namespace = Namespace::new(ctx.db_name().clone(), collection)?;
        db.ddl_path()
            .create_collection(&namespace, storage_columns)?;

        Ok(doc! {
            "ok": Bson::Double(1.0),
        })
    }
}

/// Handles: dbStats
pub struct DbStatsCommand;

#[async_trait]
impl Command for DbStatsCommand {
    fn names(&self) -> &[&str] {
        &["dbStats"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        _doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let collections = db.list_collections(ctx.db_name())?;
        let mut document_count = 0usize;
        let mut index_count = 0usize;

        for name in &collections {
            let namespace = Namespace::new(ctx.db_name().clone(), name.clone())?;
            let mut session = db.connection().open_session();
            document_count += db.document_query().count(&mut session, &namespace, None)?;
            index_count += db.list_indexes(&namespace)?.len();
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "db": ctx.db_name().as_str(),
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

#[async_trait]
impl Command for CollStatsCommand {
    fn names(&self) -> &[&str] {
        &["collStats"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "collStats")?;
        let mut session = db.connection().open_session();
        let count = db.document_query().count(&mut session, &namespace, None)?;
        let index_count = db.list_indexes(&namespace)?.len() as i32 + 1;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "ns": namespace.full_name(),
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
