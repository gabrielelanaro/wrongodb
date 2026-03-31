use std::sync::Arc;
use std::time::Duration;

use bson::{doc, Bson, Document as BsonDocument};
use mongodb::{options::ClientOptions, Client, Database};
use tokio::time::sleep;

use crate::core::errors::StorageError;
use crate::replication::{
    oplog_entry_from_bson_document, LocalReplicationStateStore, OpTime, OplogApplier, OplogEntry,
    OplogStore, ReplicationCoordinator,
};
use crate::Connection;
use crate::WrongoDBError;

const FETCH_BATCH_SIZE: i32 = 64;
const GET_MORE_AWAIT_MS: i64 = 1_000;
const RETRY_DELAY: Duration = Duration::from_millis(500);

/// Background runtime that keeps a secondary caught up with one primary.
#[derive(Clone)]
pub(crate) struct SecondaryReplicator {
    connection: Arc<Connection>,
    oplog_store: OplogStore,
    state_store: LocalReplicationStateStore,
    applier: OplogApplier,
    replication: ReplicationCoordinator,
}

impl SecondaryReplicator {
    /// Builds the secondary replication loop from explicit replication dependencies.
    pub(crate) fn new(
        connection: Arc<Connection>,
        oplog_store: OplogStore,
        state_store: LocalReplicationStateStore,
        applier: OplogApplier,
        replication: ReplicationCoordinator,
    ) -> Self {
        Self {
            connection,
            oplog_store,
            state_store,
            applier,
            replication,
        }
    }

    /// Keep the secondary caught up, retrying after transient failures.
    pub(crate) async fn run_forever(&self) {
        loop {
            if let Err(err) = self.replicate_from_configured_source().await {
                eprintln!("secondary replication loop failed: {err}");
                sleep(RETRY_DELAY).await;
            }
        }
    }

    async fn replicate_from_configured_source(&self) -> Result<(), WrongoDBError> {
        self.drain_local_backlog()?;

        let sync_source_uri = self
            .replication
            .sync_source_uri()
            .ok_or_else(|| StorageError("secondary replication requires sync_source_uri".into()))?;
        let sync_source = SyncSourceClient::connect(&sync_source_uri).await?;
        let node_name = self.replication.node_name();

        let mut batch = self.open_remote_batch(&sync_source).await?;
        loop {
            self.persist_remote_entries(&batch.entries)?;
            let last_applied = self.drain_local_backlog()?;
            self.report_progress(&sync_source, &node_name, last_applied)
                .await?;
            batch = self
                .fetch_next_remote_batch(&sync_source, batch.cursor_id)
                .await?;
        }
    }

    async fn open_remote_batch(
        &self,
        sync_source: &SyncSourceClient,
    ) -> Result<RemoteOplogBatch, WrongoDBError> {
        sync_source
            .open_oplog_cursor(self.load_last_written_index()?)
            .await
    }

    async fn fetch_next_remote_batch(
        &self,
        sync_source: &SyncSourceClient,
        cursor_id: i64,
    ) -> Result<RemoteOplogBatch, WrongoDBError> {
        if cursor_id == 0 {
            return self.open_remote_batch(sync_source).await;
        }

        sync_source.get_more(cursor_id).await
    }

    async fn report_progress(
        &self,
        sync_source: &SyncSourceClient,
        node_name: &str,
        last_applied: Option<OpTime>,
    ) -> Result<(), WrongoDBError> {
        sync_source
            .update_position(node_name, self.load_last_written()?, last_applied)
            .await
    }

    fn persist_remote_entries(&self, entries: &[OplogEntry]) -> Result<(), WrongoDBError> {
        let latest_local_index = self.load_last_written_index()?;
        let pending_entries = entries
            .iter()
            .filter(|entry| entry.op_time.index > latest_local_index)
            .collect::<Vec<_>>();
        if pending_entries.is_empty() {
            return Ok(());
        }

        let mut session = self.connection.open_session();
        session.with_transaction(|session| {
            for entry in &pending_entries {
                self.oplog_store.append(session, entry)?;
            }
            Ok(())
        })
    }

    fn drain_local_backlog(&self) -> Result<Option<OpTime>, WrongoDBError> {
        let mut last_applied = self.load_last_applied()?;
        loop {
            let after_index = last_applied.map(|op_time| op_time.index).unwrap_or(0);
            let mut session = self.connection.open_session();
            let entries = self.oplog_store.list_after(
                &mut session,
                after_index,
                FETCH_BATCH_SIZE as usize,
            )?;
            drop(session);

            if entries.is_empty() {
                return Ok(last_applied);
            }

            for entry in entries {
                self.applier.apply(&entry)?;
                last_applied = Some(entry.op_time);
            }
        }
    }

    fn load_last_written(&self) -> Result<Option<OpTime>, WrongoDBError> {
        let mut session = self.connection.open_session();
        self.oplog_store.load_last_op_time(&mut session)
    }

    fn load_last_written_index(&self) -> Result<u64, WrongoDBError> {
        Ok(self
            .load_last_written()?
            .map(|op_time| op_time.index)
            .unwrap_or(0))
    }

    fn load_last_applied(&self) -> Result<Option<OpTime>, WrongoDBError> {
        let mut session = self.connection.open_session();
        self.state_store.load_last_applied(&mut session)
    }
}

struct RemoteOplogBatch {
    cursor_id: i64,
    entries: Vec<OplogEntry>,
}

struct SyncSourceClient {
    admin_db: Database,
    local_db: Database,
}

impl SyncSourceClient {
    async fn connect(uri: &str) -> Result<Self, WrongoDBError> {
        let options = ClientOptions::parse(uri).await?;
        let client = Client::with_options(options)?;
        Ok(Self {
            admin_db: client.database("admin"),
            local_db: client.database("local"),
        })
    }

    async fn open_oplog_cursor(&self, after_index: u64) -> Result<RemoteOplogBatch, WrongoDBError> {
        let response = self
            .local_db
            .run_command(doc! {
                "find": "oplog.rs",
                "filter": { "_id": { "$gt": Bson::Int64(after_index as i64) } },
                "batchSize": FETCH_BATCH_SIZE,
                "tailable": true,
                "awaitData": true,
            })
            .await?;
        parse_remote_oplog_batch(&response, "firstBatch")
    }

    async fn get_more(&self, cursor_id: i64) -> Result<RemoteOplogBatch, WrongoDBError> {
        let response = self
            .local_db
            .run_command(doc! {
                "getMore": Bson::Int64(cursor_id),
                "collection": "oplog.rs",
                "batchSize": FETCH_BATCH_SIZE,
                "maxTimeMS": GET_MORE_AWAIT_MS,
            })
            .await?;
        parse_remote_oplog_batch(&response, "nextBatch")
    }

    async fn update_position(
        &self,
        node_name: &str,
        last_written: Option<OpTime>,
        last_applied: Option<OpTime>,
    ) -> Result<(), WrongoDBError> {
        let _ = self
            .admin_db
            .run_command(doc! {
                "replSetUpdatePosition": 1_i32,
                "member": node_name,
                "lastWritten": op_time_bson(last_written),
                "lastApplied": op_time_bson(last_applied),
            })
            .await?;
        Ok(())
    }
}

fn parse_remote_oplog_batch(
    response: &BsonDocument,
    batch_field: &str,
) -> Result<RemoteOplogBatch, WrongoDBError> {
    let cursor = response
        .get_document("cursor")
        .map_err(|_| WrongoDBError::Protocol("replication response is missing cursor".into()))?;
    let cursor_id = match cursor.get("id") {
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Int32(value)) => i64::from(*value),
        _ => {
            return Err(WrongoDBError::Protocol(
                "replication response cursor is missing id".into(),
            ))
        }
    };
    let entries = cursor
        .get_array(batch_field)
        .map_err(|_| {
            WrongoDBError::Protocol(format!(
                "replication response cursor is missing {batch_field}"
            ))
        })?
        .iter()
        .map(|value| match value {
            Bson::Document(document) => oplog_entry_from_bson_document(document),
            _ => Err(WrongoDBError::Protocol(
                "replication batch entry is not a document".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RemoteOplogBatch { cursor_id, entries })
}

fn op_time_bson(op_time: Option<OpTime>) -> Bson {
    op_time
        .map(|op_time| {
            Bson::Document(doc! {
                "term": Bson::Int64(op_time.term as i64),
                "index": Bson::Int64(op_time.index as i64),
            })
        })
        .unwrap_or(Bson::Null)
}
