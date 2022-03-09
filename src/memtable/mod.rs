mod arena;
mod concurrent_arena;
mod inline_skiplist;
mod memtable;
mod skiplist;
mod skiplist_rep;

use crate::iterator::InternalIterator;
pub use memtable::Memtable;
pub use skiplist_rep::{InlineSkipListMemtableRep, SkipListMemtableRep};

const MAX_HEIGHT: usize = 20;

pub trait MemtableRep: Send + Sync {
    fn new_iterator(&self) -> Box<dyn InternalIterator>;
    fn add(&self, key: &[u8], value: &[u8], sequence: u64);
    fn delete(&self, key: &[u8], sequence: u64);
    fn mem_size(&self) -> usize;
}
