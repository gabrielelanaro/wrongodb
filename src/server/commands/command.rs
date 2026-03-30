use async_trait::async_trait;
use bson::Document;

use crate::api::DatabaseContext;
use crate::WrongoDBError;

use super::CommandContext;

/// Trait for implementing MongoDB commands.
#[async_trait]
pub(crate) trait Command: Send + Sync {
    /// Return the command names this handler responds to.
    fn names(&self) -> &[&str];

    /// Execute the command with the given document and database context.
    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError>;
}
