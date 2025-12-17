use crate::commands::Command;
use crate::{WrongoDB, WrongoDBError};
use bson::{doc, Bson, Document};

/// Handles: listIndexes
pub struct ListIndexesCommand;

impl Command for ListIndexesCommand {
    fn names(&self) -> &[&str] {
        &["listIndexes"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("listIndexes").unwrap_or("test");
        let indexes = db.list_indexes(coll_name);

        let indexes_bson: Vec<Bson> = indexes
            .into_iter()
            .map(|idx| {
                let mut key_doc = Document::new();
                key_doc.insert(idx.field.clone(), Bson::Int32(1));
                Bson::Document(doc! {
                    "v": Bson::Int32(2),
                    "key": key_doc,
                    "name": format!("{}_1", idx.field),
                    "ns": format!("test.{}", coll_name),
                })
            })
            .collect();

        // Always include the _id index
        let mut result_indexes = vec![Bson::Document(doc! {
            "v": Bson::Int32(2),
            "key": { "_id": Bson::Int32(1) },
            "name": "_id_",
            "ns": format!("test.{}", coll_name),
        })];
        result_indexes.extend(indexes_bson);

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(0),
                "ns": format!("test.{}", coll_name),
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

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("createIndexes").unwrap_or("test");
        let mut created = 0i32;

        if let Ok(indexes) = doc.get_array("indexes") {
            for index_spec in indexes {
                if let Bson::Document(spec) = index_spec {
                    if let Ok(key_doc) = spec.get_document("key") {
                        for (field, _) in key_doc {
                            db.create_index(coll_name, field);
                            created += 1;
                        }
                    }
                }
            }
        }

        let total_indexes = db.list_indexes(coll_name).len() as i32 + 1; // +1 for _id

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "numIndexesBefore": Bson::Int32(total_indexes - created),
            "numIndexesAfter": Bson::Int32(total_indexes),
            "createdCollectionAutomatically": Bson::Boolean(false),
        })
    }
}
