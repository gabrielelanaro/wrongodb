use std::collections::HashMap;

use bson::{doc, Bson, Document};

use super::{handlers, Command, CommandContext};
use crate::api::DatabaseContext;
use crate::WrongoDBError;

/// Registry that maps command names to their handlers.
pub(crate) struct CommandRegistry {
    name_to_handler: HashMap<String, usize>,
    handlers: Vec<Box<dyn Command>>,
}

impl CommandRegistry {
    pub(crate) fn new() -> Self {
        let mut registry = Self {
            name_to_handler: HashMap::new(),
            handlers: Vec::new(),
        };
        registry.register_defaults();
        registry
    }

    pub(crate) fn register(&mut self, handler: Box<dyn Command>) {
        let idx = self.handlers.len();
        for name in handler.names() {
            self.name_to_handler.insert(name.to_lowercase(), idx);
        }
        self.handlers.push(handler);
    }

    pub(crate) async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        for key in doc.keys() {
            if let Some(&idx) = self.name_to_handler.get(&key.to_lowercase()) {
                return self.handlers[idx].execute(ctx, doc, db).await;
            }
        }
        Ok(doc! { "ok": Bson::Double(0.0), "errmsg": "Command not found" })
    }

    fn register_defaults(&mut self) {
        self.register(Box::new(handlers::HelloCommand));
        self.register(Box::new(handlers::PingCommand));
        self.register(Box::new(handlers::BuildInfoCommand));
        self.register(Box::new(handlers::ServerStatusCommand));
        self.register(Box::new(handlers::ConnectionStatusCommand));
        self.register(Box::new(handlers::ListDatabasesCommand));
        self.register(Box::new(handlers::ListCollectionsCommand));
        self.register(Box::new(handlers::CreateCollectionCommand));
        self.register(Box::new(handlers::DbStatsCommand));
        self.register(Box::new(handlers::CollStatsCommand));
        self.register(Box::new(handlers::InsertCommand));
        self.register(Box::new(handlers::FindCommand));
        self.register(Box::new(handlers::UpdateCommand));
        self.register(Box::new(handlers::DeleteCommand));
        self.register(Box::new(handlers::ListIndexesCommand));
        self.register(Box::new(handlers::CreateIndexesCommand));
        self.register(Box::new(handlers::GetMoreCommand));
        self.register(Box::new(handlers::KillCursorsCommand));
        self.register(Box::new(handlers::CountCommand));
        self.register(Box::new(handlers::DistinctCommand));
        self.register(Box::new(handlers::AggregateCommand));
        self.register(Box::new(handlers::ReplSetUpdatePositionCommand));
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}
