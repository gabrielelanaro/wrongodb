use std::sync::Arc;

use crate::core::DatabaseName;
use crate::replication::{OplogAwaitService, ReplicationCoordinator};

use super::CursorManager;

/// Parsed command-scoped context supplied by the wire protocol layer.
#[derive(Debug, Clone)]
pub(crate) struct CommandContext {
    db_name: DatabaseName,
    cursor_manager: Arc<CursorManager>,
    oplog_await_service: OplogAwaitService,
    replication: ReplicationCoordinator,
}

impl CommandContext {
    /// Build the context for one command request.
    pub(crate) fn new(
        db_name: DatabaseName,
        cursor_manager: Arc<CursorManager>,
        oplog_await_service: OplogAwaitService,
        replication: ReplicationCoordinator,
    ) -> Self {
        Self {
            db_name,
            cursor_manager,
            oplog_await_service,
            replication,
        }
    }

    /// Return the database the command is running against.
    pub(crate) fn db_name(&self) -> &DatabaseName {
        &self.db_name
    }

    /// Return the process-local cursor manager.
    pub(crate) fn cursor_manager(&self) -> &CursorManager {
        self.cursor_manager.as_ref()
    }

    /// Return the oplog await service used by tailable cursors.
    pub(crate) fn oplog_await_service(&self) -> &OplogAwaitService {
        &self.oplog_await_service
    }

    /// Return the replication coordinator for replication-aware commands.
    pub(crate) fn replication(&self) -> &ReplicationCoordinator {
        &self.replication
    }
}
