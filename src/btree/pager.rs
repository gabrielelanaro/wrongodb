use std::path::Path;

use crate::{BlockFile, WrongoDBError};

#[derive(Debug)]
pub(super) struct Pager {
    bf: BlockFile,
}

impl Pager {
    pub(super) fn create<P: AsRef<Path>>(
        path: P,
        page_size: usize,
    ) -> Result<Self, WrongoDBError> {
        Ok(Self {
            bf: BlockFile::create(path, page_size)?,
        })
    }

    pub(super) fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        Ok(Self {
            bf: BlockFile::open(path)?,
        })
    }

    pub(super) fn page_payload_len(&self) -> usize {
        self.bf.page_payload_len()
    }

    pub(super) fn root_page_id(&self) -> u64 {
        self.bf.header.root_block_id
    }

    pub(super) fn set_root_page_id(
        &mut self,
        root_page_id: u64,
    ) -> Result<(), WrongoDBError> {
        self.bf.set_root_block_id(root_page_id)
    }

    pub(super) fn read_page(&mut self, page_id: u64) -> Result<Vec<u8>, WrongoDBError> {
        self.bf.read_block(page_id, true)
    }

    pub(super) fn write_page(
        &mut self,
        page_id: u64,
        payload: &[u8],
    ) -> Result<(), WrongoDBError> {
        self.bf.write_block(page_id, payload)
    }

    fn allocate_page(&mut self) -> Result<u64, WrongoDBError> {
        self.bf.allocate_block()
    }

    pub(super) fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError> {
        let page_id = self.allocate_page()?;
        self.write_page(page_id, payload)?;
        Ok(page_id)
    }

    pub(super) fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.bf.sync_all()
    }
}
