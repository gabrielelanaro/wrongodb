use crate::storage::page_store::Page;

use super::page::{LeafPage, LeafPageError, LeafPageMut};
use super::search::search_leaf;

// ============================================================================
// Public Functions
// ============================================================================

#[cfg(test)]
pub(super) fn get(page: &Page, key: &[u8]) -> Result<Option<Vec<u8>>, LeafPageError> {
    let leaf = LeafPage::open(page)?;
    let position = search_leaf(&leaf, key)?;
    if !position.found {
        return Ok(None);
    }
    Ok(Some(leaf.value_at(position.index)?.to_vec()))
}

pub(super) fn contains_key(page: &Page, key: &[u8]) -> Result<bool, LeafPageError> {
    let leaf = LeafPage::open(page)?;
    Ok(search_leaf(&leaf, key)?.found)
}

pub(super) fn put(page: &mut Page, key: &[u8], value: &[u8]) -> Result<(), LeafPageError> {
    let mut leaf = LeafPageMut::open(page)?;
    let position = search_leaf(&LeafPage::open(leaf.page())?, key)?;
    if position.found {
        leaf.delete_at(position.index)?;
    }

    let record_len = leaf.record_len(key.len(), value.len())?;
    let need = record_len + LeafPageMut::slot_size();
    if leaf.free_contiguous() < need {
        compact(&mut leaf)?;
    }
    if leaf.free_contiguous() < need {
        return Err(LeafPageError::PageFull);
    }

    let upper = leaf.upper();
    let new_upper = upper
        .checked_sub(record_len)
        .ok_or_else(|| LeafPageError::Corrupt("upper underflow".into()))?;
    leaf.write_record(new_upper, key, value)?;
    leaf.set_upper(new_upper)?;

    let slot_count = leaf.slot_count();
    if position.index > slot_count {
        return Err(LeafPageError::Corrupt(
            "insertion index out of bounds".into(),
        ));
    }

    if position.index < slot_count {
        leaf.shift_slots_right(position.index)?;
    }

    leaf.write_slot(position.index, new_upper as u16, record_len as u16)?;
    leaf.set_slot_count((slot_count + 1) as u16)?;
    leaf.set_lower(
        (LeafPageMut::header_size() + (slot_count + 1) * LeafPageMut::slot_size()) as u16,
    )?;
    Ok(())
}

pub(super) fn delete(page: &mut Page, key: &[u8]) -> Result<bool, LeafPageError> {
    let mut leaf = LeafPageMut::open(page)?;
    let position = search_leaf(&LeafPage::open(leaf.page())?, key)?;
    if !position.found {
        return Ok(false);
    }
    leaf.delete_at(position.index)?;
    Ok(true)
}

fn compact(leaf: &mut LeafPageMut<'_>) -> Result<(), LeafPageError> {
    leaf.compact_in_place()
}
