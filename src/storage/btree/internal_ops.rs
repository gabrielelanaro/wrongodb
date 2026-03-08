use crate::storage::page_store::Page;

use super::page::{InternalPage, InternalPageError, InternalPageMut};
use super::search::search_internal;

// ============================================================================
// Public Functions
// ============================================================================

pub(super) fn put_separator(
    page: &mut Page,
    key: &[u8],
    child: u64,
) -> Result<(), InternalPageError> {
    let mut internal = InternalPageMut::open(page)?;
    let position = search_internal(&InternalPage::open(internal.page())?, key)?;
    if position.found {
        internal.delete_at(position.index)?;
    }

    let record_len = internal.record_len(key.len(), 8)?;
    let need = record_len + InternalPageMut::slot_size();
    if internal.free_contiguous() < need {
        compact(&mut internal)?;
    }
    if internal.free_contiguous() < need {
        return Err(InternalPageError::PageFull);
    }

    let upper = internal.upper();
    let new_upper = upper
        .checked_sub(record_len)
        .ok_or_else(|| InternalPageError::Corrupt("upper underflow".into()))?;
    internal.write_record(new_upper, key, child)?;
    internal.set_upper(new_upper)?;

    let slot_count = internal.slot_count();
    if position.index > slot_count {
        return Err(InternalPageError::Corrupt(
            "insertion index out of bounds".into(),
        ));
    }

    if position.index < slot_count {
        internal.shift_slots_right(position.index)?;
    }

    internal.write_slot(position.index, new_upper as u16, record_len as u16)?;
    internal.set_slot_count((slot_count + 1) as u16)?;
    internal.set_lower(
        (InternalPageMut::header_size() + (slot_count + 1) * InternalPageMut::slot_size()) as u16,
    )?;
    Ok(())
}

fn compact(internal: &mut InternalPageMut<'_>) -> Result<(), InternalPageError> {
    internal.compact_in_place()
}
