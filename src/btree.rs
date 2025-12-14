use std::path::Path;

use crate::errors::StorageError;
use crate::{BlockFile, InternalPage, InternalPageError, LeafPage, LeafPageError, WrongoDBError};

const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

#[derive(Debug)]
pub struct BTree {
    bf: BlockFile,
}

impl BTree {
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let mut bf = BlockFile::create(path, page_size)?;
        init_root_if_missing(&mut bf)?;
        Ok(Self { bf })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let mut bf = BlockFile::open(path)?;
        init_root_if_missing(&mut bf)?;
        Ok(Self { bf })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let root = self.bf.header.root_block_id;
        if root == 0 {
            return Ok(None);
        }

        let root_payload = self.bf.read_block(root, true)?;
        match page_type(&root_payload)? {
            PageType::Leaf => {
                let mut root_payload = root_payload;
                let page = LeafPage::open(&mut root_payload)
                    .map_err(|e| StorageError(format!("corrupt root leaf: {e}")))?;
                Ok(page.get(key).map_err(map_leaf_err)?)
            }
            PageType::Internal => {
                let mut root_payload = root_payload;
                let internal = InternalPage::open(&mut root_payload)
                    .map_err(|e| StorageError(format!("corrupt root internal: {e}")))?;
                let child = internal
                    .child_for_key(key)
                    .map_err(|e| StorageError(format!("root routing failed: {e}")))?;
                let mut leaf_payload = self.bf.read_block(child, true)?;
                let leaf = LeafPage::open(&mut leaf_payload)
                    .map_err(|e| StorageError(format!("corrupt leaf {child}: {e}")))?;
                Ok(leaf.get(key).map_err(map_leaf_err)?)
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let root = self.bf.header.root_block_id;
        if root == 0 {
            return Err(StorageError("btree missing root".into()).into());
        }

        let root_payload = self.bf.read_block(root, true)?;
        match page_type(&root_payload)? {
            PageType::Leaf => self.put_into_root_leaf(root, root_payload, key, value),
            PageType::Internal => self.put_into_two_level(root, root_payload, key, value),
        }
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.bf.sync_all()
    }

    fn put_into_root_leaf(
        &mut self,
        root_leaf_id: u64,
        mut leaf_payload: Vec<u8>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        {
            let mut leaf = LeafPage::open(&mut leaf_payload)
                .map_err(|e| StorageError(format!("corrupt root leaf: {e}")))?;
            match leaf.put(key, value) {
                Ok(()) => {
                    self.bf.write_block(root_leaf_id, &leaf_payload)?;
                    return Ok(());
                }
                Err(LeafPageError::PageFull) => { /* split below */ }
                Err(e) => return Err(map_leaf_err(e)),
            }
        }

        let payload_len = leaf_payload.len();
        let mut entries = leaf_entries(&mut leaf_payload)?;
        upsert_entry(&mut entries, key, value);
        let (left_bytes, right_bytes, split_key) = split_leaf_entries(&entries, payload_len)?;

        // Overwrite existing root leaf with the left page and allocate a new right leaf.
        let right_leaf_id = self.bf.write_new_block(&right_bytes)?;
        self.bf.write_block(root_leaf_id, &left_bytes)?;

        // Create a new root internal page with two children.
        let mut root_internal_bytes = vec![0u8; payload_len];
        {
            let mut internal = InternalPage::init(&mut root_internal_bytes, root_leaf_id)
                .map_err(|e| StorageError(format!("init root internal failed: {e}")))?;
            internal
                .put_separator(&split_key, right_leaf_id)
                .map_err(map_internal_err)?;
        }

        let new_root_id = self.bf.write_new_block(&root_internal_bytes)?;
        self.bf.set_root_block_id(new_root_id)?;
        Ok(())
    }

    fn put_into_two_level(
        &mut self,
        root_internal_id: u64,
        mut root_payload: Vec<u8>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let child_id = {
            let root = InternalPage::open(&mut root_payload)
                .map_err(|e| StorageError(format!("corrupt root internal: {e}")))?;
            root.child_for_key(key)
                .map_err(|e| StorageError(format!("root routing failed: {e}")))?
        };

        let mut leaf_payload = self.bf.read_block(child_id, true)?;
        {
            let mut leaf = LeafPage::open(&mut leaf_payload)
                .map_err(|e| StorageError(format!("corrupt leaf {child_id}: {e}")))?;
            match leaf.put(key, value) {
                Ok(()) => {
                    self.bf.write_block(child_id, &leaf_payload)?;
                    return Ok(());
                }
                Err(LeafPageError::PageFull) => { /* split below */ }
                Err(e) => return Err(map_leaf_err(e)),
            }
        }

        // Split the overflowing leaf and update the root.
        let payload_len = leaf_payload.len();
        let mut entries = leaf_entries(&mut leaf_payload)?;
        upsert_entry(&mut entries, key, value);
        let (left_bytes, right_bytes, split_key) = split_leaf_entries(&entries, payload_len)?;

        let right_leaf_id = self.bf.write_new_block(&right_bytes)?;
        self.bf.write_block(child_id, &left_bytes)?;

        {
            let mut root = InternalPage::open(&mut root_payload)
                .map_err(|e| StorageError(format!("corrupt root internal: {e}")))?;
            match root.put_separator(&split_key, right_leaf_id) {
                Ok(()) => {}
                Err(InternalPageError::PageFull) => {
                    return Err(StorageError("root internal page full (Slice E needed)".into()).into())
                }
                Err(e) => return Err(map_internal_err(e)),
            }
        }
        self.bf.write_block(root_internal_id, &root_payload)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageType {
    Leaf,
    Internal,
}

fn page_type(payload: &[u8]) -> Result<PageType, WrongoDBError> {
    let Some(t) = payload.first().copied() else {
        return Err(StorageError("empty page payload".into()).into());
    };
    match t {
        PAGE_TYPE_LEAF => Ok(PageType::Leaf),
        PAGE_TYPE_INTERNAL => Ok(PageType::Internal),
        other => Err(StorageError(format!("unknown page type: {other}")).into()),
    }
}

fn init_root_if_missing(bf: &mut BlockFile) -> Result<(), WrongoDBError> {
    if bf.header.root_block_id != 0 {
        return Ok(());
    }
    let payload_len = bf.page_size - 4;
    let mut leaf_bytes = vec![0u8; payload_len];
    LeafPage::init(&mut leaf_bytes).map_err(map_leaf_err)?;
    let leaf_id = bf.write_new_block(&leaf_bytes)?;
    bf.set_root_block_id(leaf_id)?;
    Ok(())
}

fn map_leaf_err(e: LeafPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

fn map_internal_err(e: InternalPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

fn leaf_entries(buf: &mut [u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
    let leaf = LeafPage::open(buf).map_err(|e| StorageError(format!("corrupt leaf: {e}")))?;
    let n = leaf.slot_count().map_err(map_leaf_err)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push((
            leaf.key_at(i).map_err(map_leaf_err)?.to_vec(),
            leaf.value_at(i).map_err(map_leaf_err)?.to_vec(),
        ));
    }
    Ok(out)
}

fn upsert_entry(entries: &mut Vec<(Vec<u8>, Vec<u8>)>, key: &[u8], value: &[u8]) {
    match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(i) => entries[i].1 = value.to_vec(),
        Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
    }
}

fn split_leaf_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), WrongoDBError> {
    if entries.len() < 2 {
        return Err(StorageError("cannot split leaf with <2 entries".into()).into());
    }
    let mid = entries.len() / 2;

    let mut candidates = Vec::new();
    // split_idx must be in 1..len-1
    for delta in 0..entries.len() {
        let a = mid.saturating_sub(delta);
        let b = mid + delta;
        if a >= 1 && a < entries.len() {
            candidates.push(a);
        }
        if b >= 1 && b < entries.len() && b != a {
            candidates.push(b);
        }
    }
    candidates.retain(|&i| i < entries.len());
    candidates.dedup();

    for split_idx in candidates {
        if split_idx == 0 || split_idx >= entries.len() {
            continue;
        }
        let left = build_leaf_page(&entries[..split_idx], payload_len);
        let right = build_leaf_page(&entries[split_idx..], payload_len);
        if let (Ok(l), Ok(r)) = (left, right) {
            return Ok((l, r, entries[split_idx].0.clone()));
        }
    }

    Err(StorageError("leaf split impossible (record too large?)".into()).into())
}

fn build_leaf_page(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<Vec<u8>, WrongoDBError> {
    let mut bytes = vec![0u8; payload_len];
    let mut page = LeafPage::init(&mut bytes).map_err(map_leaf_err)?;
    for (k, v) in entries {
        page.put(k, v).map_err(map_leaf_err)?;
    }
    Ok(bytes)
}

