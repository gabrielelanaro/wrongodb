use crate::commands::Command;
use crate::{WrongoDB, WrongoDBError};
use bson::{doc, Bson, Document};

/// Handles: getMore
/// Note: Currently a stub - real cursor support requires server-side cursor state
pub struct GetMoreCommand;

impl Command for GetMoreCommand {
    fn names(&self) -> &[&str] {
        &["getMore"]
    }

    fn execute(&self, doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("collection").unwrap_or("test");

        // Since cursor ID is always 0 and we return all results in firstBatch,
        // getMore always returns an empty nextBatch
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(0),
                "ns": format!("test.{}", coll_name),
                "nextBatch": Bson::Array(vec![]),
            },
        })
    }
}

/// Handles: killCursors
/// Note: Currently a stub - no real cursors to kill
pub struct KillCursorsCommand;

impl Command for KillCursorsCommand {
    fn names(&self) -> &[&str] {
        &["killCursors"]
    }

    fn execute(&self, doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        let cursors = doc.get_array("cursors").cloned().unwrap_or_else(|_| vec![]);

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursorsKilled": Bson::Array(vec![]),
            "cursorsNotFound": Bson::Array(vec![]),
            "cursorsAlive": Bson::Array(vec![]),
            "cursorsUnknown": Bson::Array(cursors),
        })
    }
}
