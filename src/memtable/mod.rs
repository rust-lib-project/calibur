mod arena;
mod list;
mod memtable;

pub use memtable::Memtable;

const MAX_HEIGHT: usize = 20;

pub use list::Skiplist;
use crate::common::ValueType;
use crate::iterator::InternalIterator;

pub trait MemtableRep: Send + Sync {
    fn new_iterator(&self) -> Box<dyn InternalIterator>;

    fn add(&self, key: &[u8], value: &[u8], sequence: u64, tp: ValueType);

    fn delete(&self, key: &[u8], sequence: u64);

    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    fn should_flush(&self) -> bool;

    fn get_mem_size(&self) -> usize;

    fn is_empty(&self) -> bool;
}
