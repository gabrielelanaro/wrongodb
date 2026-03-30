use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bson::Document;
use parking_lot::RwLock;
use serde_json::Value;

use crate::core::Namespace;
use crate::document_query::{FindPlan, FindResumeToken};
/// Process-local storage for live server-side cursors.
#[derive(Debug, Clone)]
pub(crate) struct CursorManager {
    state: Arc<CursorManagerState>,
}

#[derive(Debug)]
struct CursorManagerState {
    next_id: AtomicU64,
    cursors: RwLock<HashMap<u64, CursorState>>,
}

/// Saved cursor state used by `getMore` and `killCursors`.
#[derive(Debug, Clone)]
pub(crate) enum CursorState {
    /// Bookmark-based `find` cursor.
    Find(Box<FindCursorState>),
    /// Materialized command cursor such as `listCollections`.
    Materialized(MaterializedCursorState),
}

/// Saved state for a command-backed `find` cursor.
#[derive(Debug, Clone)]
pub(crate) struct FindCursorState {
    /// Namespace being read.
    pub(crate) namespace: Namespace,
    /// Original filter reused on later `getMore` calls.
    pub(crate) filter: Option<Value>,
    /// Chosen scan family for this cursor.
    pub(crate) plan: FindPlan,
    /// Bookmark for the next batch.
    pub(crate) resume: Option<FindResumeToken>,
    /// Default batch size when `getMore` omits one.
    pub(crate) batch_size: usize,
    /// Remaining total result budget across the cursor lifetime.
    pub(crate) remaining_limit: Option<usize>,
    /// Whether the cursor stays open at the end of `local.oplog.rs`.
    pub(crate) tailable: bool,
    /// Whether `getMore` may wait for new oplog rows.
    pub(crate) await_data: bool,
    /// Whether the cursor must close after the first batch regardless of remaining rows.
    pub(crate) single_batch: bool,
}

/// Saved state for a materialized command cursor such as `listIndexes`.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedCursorState {
    /// Namespace reported back to the client.
    pub(crate) namespace: Namespace,
    /// Precomputed command results.
    pub(crate) docs: Vec<Document>,
    /// Offset of the next document to emit.
    pub(crate) next_offset: usize,
}

impl CursorManager {
    /// Build an empty cursor manager.
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(CursorManagerState {
                next_id: AtomicU64::new(1),
                cursors: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Store one live cursor state and return its non-zero id.
    pub(crate) fn create(&self, cursor: CursorState) -> u64 {
        let cursor_id = self.state.next_id.fetch_add(1, Ordering::SeqCst);
        self.state.cursors.write().insert(cursor_id, cursor);
        cursor_id
    }

    /// Remove one cursor temporarily so `getMore` can operate on it exclusively.
    pub(crate) fn take(&self, cursor_id: u64) -> Option<CursorState> {
        self.state.cursors.write().remove(&cursor_id)
    }

    /// Put a still-live cursor back under the same id.
    pub(crate) fn restore(&self, cursor_id: u64, cursor: CursorState) {
        self.state.cursors.write().insert(cursor_id, cursor);
    }

    /// Kill one cursor and return whether it existed.
    pub(crate) fn kill(&self, cursor_id: u64) -> bool {
        self.state.cursors.write().remove(&cursor_id).is_some()
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new()
    }
}
