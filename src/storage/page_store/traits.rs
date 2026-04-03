use crate::core::errors::WrongoDBError;
use crate::storage::wal::Lsn;

use super::page::Page;
use super::types::{PageEdit, ReadPin};

pub(crate) trait PageRead: std::fmt::Debug + Send + Sync {
    fn page_payload_len(&self) -> usize;
    fn pin_page(&mut self, page_id: u64) -> Result<ReadPin, WrongoDBError>;
    fn get_page(&self, pin: &ReadPin) -> &Page;
    fn get_page_mut(&mut self, pin: &ReadPin) -> &mut Page;
    fn unpin_page(&mut self, pin: ReadPin);
}

pub(crate) trait PageWrite: std::fmt::Debug + Send + Sync {
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PageEdit, WrongoDBError>;
    fn commit_page_edit(&mut self, edit: PageEdit) -> Result<u64, WrongoDBError>;
    fn abort_page_edit(&mut self, edit: PageEdit) -> Result<(), WrongoDBError>;
    fn write_new_page(&mut self, page: Page) -> Result<u64, WrongoDBError>;
}

pub(crate) trait RootStore: std::fmt::Debug + Send + Sync {
    /// Returns true if the store has a root page.
    fn has_root(&self) -> bool {
        self.root_page_id() != 0
    }

    fn root_page_id(&self) -> u64;
    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError>;
}

pub(crate) trait CheckpointStore: std::fmt::Debug + Send + Sync {
    /// Capture the root page that should become durable for this checkpoint.
    fn checkpoint_prepare(&self) -> u64;

    /// Flush dirty page data needed by the prepared root.
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError>;

    /// Publish the prepared root and the WAL replay boundary it covers.
    ///
    /// `checkpoint_lsn` means "this store already includes every committed WAL
    /// record before this LSN".
    fn checkpoint_commit(
        &mut self,
        new_root: u64,
        checkpoint_lsn: Lsn,
    ) -> Result<(), WrongoDBError>;
}

/// Combined trait for a complete page store implementation.
pub(crate) trait PageStore: PageRead + PageWrite + RootStore + CheckpointStore {}

impl<T> PageStore for T where T: PageRead + PageWrite + RootStore + CheckpointStore {}
