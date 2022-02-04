use crate::common::format::GlobalSeqnoAppliedKey;
use crate::table::{AsyncIterator, InternalIterator};

enum InnerIterator {
    Async(Box<dyn AsyncIterator>),
    Sync(Box<dyn InternalIterator>),
}

pub struct CompactionIter {
    inner: InnerIterator,
    applied_key: GlobalSeqnoAppliedKey,
}

impl CompactionIter {
    pub fn new_with_async(iter: Box<dyn AsyncIterator>) -> Self {
        Self {
            inner: InnerIterator::Async(iter),
            applied_key: GlobalSeqnoAppliedKey::new(0, false),
        }
    }
}
