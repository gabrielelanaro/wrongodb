use bson::Document;

use crate::{WrongoDB, WrongoDBError};

pub mod handlers;
mod registry;

pub use handlers::*;
pub use registry::CommandRegistry;

/// Trait for implementing MongoDB commands.
pub trait Command: Send + Sync {
    /// Returns the command names this handler responds to
    fn names(&self) -> &[&str];

    /// Execute the command with the given document and database
    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError>;
}
