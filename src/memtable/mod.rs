mod arena;
mod concurrent_arena;
mod context;
mod inline_skiplist;
mod memtable;
mod skiplist;
mod skiplist_rep;

use crate::iterator::InternalIterator;
pub use context::MemTableContext;
pub use inline_skiplist::Splice;
pub use memtable::Memtable;
pub use skiplist_rep::{InlineSkipListMemtableRep, SkipListMemtableRep};

use std::cmp::Ordering;

const MAX_HEIGHT: usize = 20;

pub trait MemtableRep: Send + Sync {
    fn new_iterator(&self) -> Box<dyn InternalIterator>;
    fn add(&self, splice: &mut MemTableContext, key: &[u8], value: &[u8], sequence: u64);
    fn delete(&self, splice: &mut MemTableContext, key: &[u8], sequence: u64);
    fn mem_size(&self) -> usize;
    fn name(&self) -> &str;
    fn cmp(&self, start: &[u8], end: &[u8]) -> Ordering {
        start.cmp(end)
    }

    fn scan<F: FnMut(&[u8], &[u8])>(&self, start: &[u8], end: &[u8], mut f: F) {
        let mut iter = self.new_iterator();
        iter.seek(start);
        while iter.valid() && self.cmp(iter.key(), end) == Ordering::Less {
            f(iter.key(), iter.value());
            iter.next();
        }
    }
}
