use bson::Document;

use crate::database_context::DatabaseContext;
use crate::WrongoDBError;

pub(crate) mod handlers;
mod registry;

pub(crate) use registry::CommandRegistry;

/// Trait for implementing MongoDB commands.
pub(crate) trait Command: Send + Sync {
    /// Returns the command names this handler responds to
    fn names(&self) -> &[&str];

    /// Execute the command with the given document and connection
    fn execute(&self, doc: &Document, db: &DatabaseContext) -> Result<Document, WrongoDBError>;
}
