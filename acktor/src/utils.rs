use std::sync::atomic::{AtomicUsize, Ordering};

static ADDRESS_INDEX_ALLOCATOR: AtomicUsize = AtomicUsize::new(0);

#[inline]
pub(crate) fn new_address_id() -> usize {
    ADDRESS_INDEX_ALLOCATOR.fetch_add(1, Ordering::AcqRel)
}
