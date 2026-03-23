mod file_cursor;
mod table_cursor;

pub use file_cursor::FileCursor;
pub use table_cursor::TableCursor;


/// A single key/value entry returned by [`TableCursor::next`].
///
/// The public table cursor surface is deliberately key/value-shaped because it is the
/// low-level storage API, not the document/query API.
///
/// This type alias exists to keep that low-level API readable without
/// introducing a heavier public wrapper type.
pub type CursorEntry = (Vec<u8>, Vec<u8>);

pub(crate) type IndexEntry = (usize, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableCursorWriteAccess {
    ReadWrite,
}

#[derive(Debug, Default)]
pub(crate) struct TableCursorState {
    pub(crate) buffered_entries: Vec<CursorEntry>,
    pub(crate) buffer_pos: usize,
    pub(crate) exhausted: bool,
    pub(crate) range_start: Option<Vec<u8>>,
    pub(crate) range_end: Option<Vec<u8>>,
}

impl TableCursorState {
    pub(crate) fn reset_runtime(&mut self) {
        self.buffered_entries.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
    }
}
