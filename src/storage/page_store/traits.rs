use crate::core::errors::WrongoDBError;

use super::types::{PinnedPage, PinnedPageMut};

pub trait PageRead: std::fmt::Debug + Send + Sync {
    fn page_payload_len(&self) -> usize;
    fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError>;
    fn unpin_page(&mut self, page_id: u64);
}

pub trait PageWrite: std::fmt::Debug + Send + Sync {
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError>;
    fn unpin_page_mut_commit(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn unpin_page_mut_abort(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError>;
}

pub trait RootStore: std::fmt::Debug + Send + Sync {
    fn root_page_id(&self) -> u64;
    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError>;
}

pub trait CheckpointStore: std::fmt::Debug + Send + Sync {
    fn checkpoint_prepare(&self) -> u64;
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError>;
    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError>;
    fn sync_all(&mut self) -> Result<(), WrongoDBError>;
}

/// Combined trait for a complete page store implementation.
pub trait PageStore: PageRead + PageWrite + RootStore + CheckpointStore {}

impl<T> PageStore for T where T: PageRead + PageWrite + RootStore + CheckpointStore {}
