use super::crud::namespace_from_field;
use super::cursor::{create_materialized_cursor_response, default_batch_size};
use crate::api::DatabaseContext;
use crate::catalog::CreateIndexRequest;
use crate::server::commands::{Command, CommandContext};
use crate::WrongoDBError;
use async_trait::async_trait;
use bson::{doc, Bson, Document};

/// Handles: listIndexes
pub struct ListIndexesCommand;

#[async_trait]
impl Command for ListIndexesCommand {
    fn names(&self) -> &[&str] {
        &["listIndexes"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "listIndexes")?;
        let indexes = db.list_indexes(&namespace)?;

        let indexes_docs: Vec<Document> = indexes
            .into_iter()
            .map(|index| {
                let key_doc = index
                    .spec()
                    .get_document("key")
                    .cloned()
                    .unwrap_or_default();
                doc! {
                    "v": Bson::Int32(2),
                    "key": key_doc,
                    "name": index.name(),
                    "ns": namespace.full_name(),
                }
            })
            .collect();

        let mut result_indexes = vec![doc! {
            "v": Bson::Int32(2),
            "key": { "_id": Bson::Int32(1) },
            "name": "_id_",
            "ns": namespace.full_name(),
        }];
        result_indexes.extend(indexes_docs);

        Ok(create_materialized_cursor_response(
            ctx,
            namespace,
            result_indexes,
            default_batch_size(doc),
        ))
    }
}

/// Handles: createIndexes
pub struct CreateIndexesCommand;

#[async_trait]
impl Command for CreateIndexesCommand {
    fn names(&self) -> &[&str] {
        &["createIndexes"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "createIndexes")?;
        let mut created = 0i32;
        let mut requests = Vec::new();

        if let Ok(indexes) = doc.get_array("indexes") {
            for index_spec in indexes {
                if let Bson::Document(spec) = index_spec {
                    requests.push(CreateIndexRequest::from_bson_spec(spec)?);
                }
            }
        }

        for request in requests {
            db.ddl_path().create_index(&namespace, request)?;
            created += 1;
        }

        let total_indexes = db.list_indexes(&namespace)?.len() as i32 + 1;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "numIndexesBefore": Bson::Int32(total_indexes - created),
            "numIndexesAfter": Bson::Int32(total_indexes),
            "createdCollectionAutomatically": Bson::Boolean(false),
        })
    }
}
