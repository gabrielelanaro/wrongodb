use crate::core::errors::WrongoDBError;

use super::page::Page;
use super::types::{PageEdit, ReadPin};

pub trait PageRead: std::fmt::Debug + Send + Sync {
    fn page_payload_len(&self) -> usize;
    fn pin_page(&mut self, page_id: u64) -> Result<ReadPin, WrongoDBError>;
    fn get_page(&self, pin: &ReadPin) -> &Page;
    fn unpin_page(&mut self, pin: ReadPin);
}

pub trait PageWrite: std::fmt::Debug + Send + Sync {
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PageEdit, WrongoDBError>;
    fn commit_page_edit(&mut self, edit: PageEdit) -> Result<u64, WrongoDBError>;
    fn abort_page_edit(&mut self, edit: PageEdit) -> Result<(), WrongoDBError>;
    fn write_new_page(&mut self, page: Page) -> Result<u64, WrongoDBError>;
}

pub trait RootStore: std::fmt::Debug + Send + Sync {
    fn root_page_id(&self) -> u64;
    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError>;
}

pub trait CheckpointStore: std::fmt::Debug + Send + Sync {
    fn checkpoint_prepare(&self) -> u64;
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError>;
    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError>;
}

/// Combined trait for a complete page store implementation.
pub trait PageStore: PageRead + PageWrite + RootStore + CheckpointStore {}

impl<T> PageStore for T where T: PageRead + PageWrite + RootStore + CheckpointStore {}
