pub(super) mod internal;
pub(super) mod leaf;

pub(super) use internal::{InternalPage, InternalPageError, InternalPageMut};
pub(super) use leaf::{LeafPage, LeafPageError, LeafPageMut};
