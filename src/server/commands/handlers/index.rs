use super::crud::namespace_from_field;
use crate::api::DatabaseContext;
use crate::catalog::CreateIndexRequest;
use crate::server::commands::{Command, CommandContext};
use crate::WrongoDBError;
use bson::{doc, Bson, Document};

/// Handles: listIndexes
pub struct ListIndexesCommand;

impl Command for ListIndexesCommand {
    fn names(&self) -> &[&str] {
        &["listIndexes"]
    }

    fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "listIndexes")?;
        let indexes = db.list_indexes(&namespace)?;

        let indexes_bson: Vec<Bson> = indexes
            .into_iter()
            .map(|index| {
                let key_doc = index
                    .spec()
                    .get_document("key")
                    .cloned()
                    .unwrap_or_default();
                Bson::Document(doc! {
                    "v": Bson::Int32(2),
                    "key": key_doc,
                    "name": index.name(),
                    "ns": namespace.full_name(),
                })
            })
            .collect();

        let mut result_indexes = vec![Bson::Document(doc! {
            "v": Bson::Int32(2),
            "key": { "_id": Bson::Int32(1) },
            "name": "_id_",
            "ns": namespace.full_name(),
        })];
        result_indexes.extend(indexes_bson);

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(0),
                "ns": namespace.full_name(),
                "firstBatch": Bson::Array(result_indexes),
            },
        })
    }
}

/// Handles: createIndexes
pub struct CreateIndexesCommand;

impl Command for CreateIndexesCommand {
    fn names(&self) -> &[&str] {
        &["createIndexes"]
    }

    fn execute(
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
