use async_trait::async_trait;
use bson::{doc, Bson, Document};

use crate::api::DatabaseContext;
use crate::replication::OpTime;
use crate::server::commands::{Command, CommandContext};
use crate::WrongoDBError;

/// Handles: replSetUpdatePosition
pub struct ReplSetUpdatePositionCommand;

#[async_trait]
impl Command for ReplSetUpdatePositionCommand {
    fn names(&self) -> &[&str] {
        &["replSetUpdatePosition"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        _db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let member = doc
            .get_str("member")
            .map(str::to_string)
            .map_err(|_| WrongoDBError::Protocol("replSetUpdatePosition requires member".into()))?;
        let last_written = parse_optime(doc.get("lastWritten"))?;
        let last_applied = parse_optime(doc.get("lastApplied"))?;

        ctx.replication()
            .update_follower_progress(member, last_written, last_applied);

        Ok(doc! { "ok": Bson::Double(1.0) })
    }
}

fn parse_optime(value: Option<&Bson>) -> Result<Option<OpTime>, WrongoDBError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value, Bson::Null) {
        return Ok(None);
    }

    let document = value.as_document().ok_or_else(|| {
        WrongoDBError::Protocol("replSetUpdatePosition optime must be a document".into())
    })?;
    let term = match document.get("term") {
        Some(Bson::Int64(value)) if *value >= 0 => *value as u64,
        Some(Bson::Int32(value)) if *value >= 0 => *value as u64,
        _ => {
            return Err(WrongoDBError::Protocol(
                "replSetUpdatePosition optime is missing term".into(),
            ))
        }
    };
    let index = match document.get("index") {
        Some(Bson::Int64(value)) if *value >= 0 => *value as u64,
        Some(Bson::Int32(value)) if *value >= 0 => *value as u64,
        _ => {
            return Err(WrongoDBError::Protocol(
                "replSetUpdatePosition optime is missing index".into(),
            ))
        }
    };

    Ok(Some(OpTime { term, index }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bson::doc;
    use tempfile::tempdir;

    use super::ReplSetUpdatePositionCommand;
    use crate::api::DatabaseContext;
    use crate::core::DatabaseName;
    use crate::replication::{
        OpTime, OplogAwaitService, ReplicationConfig, ReplicationCoordinator,
    };
    use crate::server::commands::{Command, CommandContext, CursorManager};
    use crate::storage::api::{Connection, ConnectionConfig};

    // EARS: When a follower reports written and applied positions through the
    // replication command, the primary shall store that progress by node name.
    #[tokio::test]
    async fn repl_set_update_position_updates_follower_progress() {
        let dir = tempdir().unwrap();
        let connection =
            Arc::new(Connection::open(dir.path(), ConnectionConfig::default()).unwrap());
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());
        let db = DatabaseContext::new(connection, coordinator.clone()).unwrap();
        let ctx = CommandContext::new(
            DatabaseName::new("admin").unwrap(),
            Arc::new(CursorManager::new()),
            OplogAwaitService::new(0),
            coordinator.clone(),
        );

        let command = ReplSetUpdatePositionCommand;
        command
            .execute(
                &ctx,
                &doc! {
                    "replSetUpdatePosition": 1,
                    "member": "secondary-a",
                    "lastWritten": { "term": 1, "index": 7 },
                    "lastApplied": { "term": 1, "index": 6 },
                },
                &db,
            )
            .await
            .unwrap();

        let progress = coordinator.follower_progress("secondary-a").unwrap();
        assert_eq!(progress.last_written, Some(OpTime { term: 1, index: 7 }));
        assert_eq!(progress.last_applied, Some(OpTime { term: 1, index: 6 }));
    }
}
