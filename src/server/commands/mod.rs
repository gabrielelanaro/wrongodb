use bson::Document;

use crate::api::DatabaseContext;
use crate::core::DatabaseName;
use crate::WrongoDBError;

pub(crate) mod handlers;
mod registry;

pub(crate) use registry::CommandRegistry;

/// Parsed command-scoped context supplied by the wire protocol layer.
#[derive(Debug, Clone)]
pub(crate) struct CommandContext {
    db_name: DatabaseName,
}

impl CommandContext {
    /// Build the context for one command request.
    pub(crate) fn new(db_name: DatabaseName) -> Self {
        Self { db_name }
    }

    /// Return the database the command is running against.
    pub(crate) fn db_name(&self) -> &DatabaseName {
        &self.db_name
    }
}

/// Trait for implementing MongoDB commands.
pub(crate) trait Command: Send + Sync {
    /// Returns the command names this handler responds to
    fn names(&self) -> &[&str];

    /// Execute the command with the given document and connection
    fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError>;
}
