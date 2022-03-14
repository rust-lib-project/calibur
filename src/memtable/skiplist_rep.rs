use super::inline_skiplist::Comparator;
use crate::common::format::pack_sequence_and_type;
use crate::common::ValueType;
use crate::iterator::InternalIterator;
use crate::memtable::concurrent_arena::SharedArena;
use crate::memtable::inline_skiplist::{InlineSkipList, SkipListIterator};
use crate::memtable::skiplist::{IterRef, Skiplist};
use crate::memtable::{MemTableContext, MemtableRep};
use crate::util::get_var_uint32;
use crate::{InternalKeyComparator, KeyComparator};
use std::cmp::Ordering;

pub struct DefaultComparator {
    comparator: InternalKeyComparator,
}

impl DefaultComparator {
    pub fn new(comparator: InternalKeyComparator) -> Self {
        Self { comparator }
    }
}

impl Comparator for DefaultComparator {
    fn compare(&self, k1: &[u8], k2: &[u8]) -> std::cmp::Ordering {
        self.comparator.compare_key(k1, k2)
    }

    unsafe fn compare_raw_key(&self, k1: *const u8, k2: *const u8) -> Ordering {
        let key1 = if *k1 < 128 {
            std::slice::from_raw_parts(k1.add(1), (*k1) as usize)
        } else {
            let data = std::slice::from_raw_parts(k1, 5);
            let mut offset = 0;
            let l = get_var_uint32(data, &mut offset).unwrap();
            std::slice::from_raw_parts(k1.add(offset), l as usize)
        };
        let key2 = if ((*k2) & 128) == 0 {
            std::slice::from_raw_parts(k2.add(1), (*k2) as usize)
        } else {
            let data = std::slice::from_raw_parts(k2, 5);
            let mut offset = 0;
            let l = get_var_uint32(data, &mut offset).unwrap();
            std::slice::from_raw_parts(k2.add(offset), l as usize)
        };
        self.comparator.compare_key(key1, key2)
    }

    unsafe fn compare_key(&self, k1: *const u8, k2: &[u8]) -> Ordering {
        let key1 = if ((*k1) & 128) == 0 {
            std::slice::from_raw_parts(k1.add(1), (*k1) as usize)
        } else {
            let data = std::slice::from_raw_parts(k1, 5);
            let mut offset = 0;
            let l = get_var_uint32(data, &mut offset).unwrap();
            std::slice::from_raw_parts(k1.add(offset), l as usize)
        };
        self.comparator.compare_key(key1, k2)
    }
}

// TODO: support in memory bloom filter
pub struct InlineSkipListMemtableRep {
    list: InlineSkipList<DefaultComparator, SharedArena>,
    comparator: InternalKeyComparator,
}

pub struct InlineSkipListMemtableIter {
    iter: SkipListIterator<DefaultComparator, SharedArena>,
    current_offset: usize,
    current_key_size: usize,
    buf: Vec<u8>,
}

unsafe impl Send for InlineSkipListMemtableRep {}
unsafe impl Sync for InlineSkipListMemtableRep {}
unsafe impl Send for InlineSkipListMemtableIter {}
unsafe impl Sync for InlineSkipListMemtableIter {}

impl InternalIterator for InlineSkipListMemtableIter {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek(&mut self, key: &[u8]) {
        self.iter.seek(&mut self.buf, key)
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        self.iter.seek_for_prev(&mut self.buf, key)
    }

    fn next(&mut self) {
        self.iter.next();
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }
}

impl InlineSkipListMemtableRep {
    pub fn new(comparator: InternalKeyComparator) -> Self {
        Self {
            list: InlineSkipList::new(
                SharedArena::new(),
                DefaultComparator {
                    comparator: comparator.clone(),
                },
            ),
            comparator,
        }
    }
}

impl MemtableRep for InlineSkipListMemtableRep {
    fn new_iterator(&self) -> Box<dyn InternalIterator> {
        Box::new(InlineSkipListMemtableIter {
            iter: SkipListIterator::new(&self.list),
            current_offset: 0,
            current_key_size: 0,
            buf: vec![],
        })
    }

    fn add(&self, ctx: &mut MemTableContext, key: &[u8], value: &[u8], sequence: u64) {
        self.list.add(ctx, key, value, sequence)
    }

    fn delete(&self, ctx: &mut MemTableContext, key: &[u8], sequence: u64) {
        self.list.delete(ctx, key, sequence)
    }

    fn mem_size(&self) -> usize {
        self.list.mem_size()
    }

    fn name(&self) -> &str {
        "InlineSkipListMemtable"
    }

    fn cmp(&self, start: &[u8], end: &[u8]) -> Ordering {
        self.comparator.compare_key(start, end)
    }
}

pub struct SkipListMemtableRep {
    list: Skiplist,
    comp: InternalKeyComparator,
}

impl SkipListMemtableRep {
    pub fn new(comparator: InternalKeyComparator, write_buffer_size: usize) -> Self {
        Self {
            list: Skiplist::with_capacity(comparator.clone(), write_buffer_size as u32),
            comp: comparator,
        }
    }
}

pub struct MemIterator {
    inner: IterRef<Skiplist>,
}

impl InternalIterator for MemIterator {
    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn seek(&mut self, key: &[u8]) {
        self.inner.seek(key)
    }

    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.inner.seek_to_last()
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        self.inner.seek_for_prev(key)
    }

    fn next(&mut self) {
        self.inner.next()
    }

    fn prev(&mut self) {
        self.inner.prev()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().as_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value().as_ref()
    }
}

impl MemtableRep for SkipListMemtableRep {
    fn new_iterator(&self) -> Box<dyn InternalIterator> {
        Box::new(MemIterator {
            inner: self.list.iter(),
        })
    }

    fn add(&self, _: &mut MemTableContext, key: &[u8], value: &[u8], sequence: u64) {
        let mut ukey = Vec::with_capacity(key.len() + 8);
        ukey.extend_from_slice(key);
        ukey.extend_from_slice(
            &pack_sequence_and_type(sequence, ValueType::TypeValue as u8).to_le_bytes(),
        );
        self.list.put(ukey, value.to_vec());
    }
    fn delete(&self, _: &mut MemTableContext, key: &[u8], sequence: u64) {
        let mut ukey = Vec::with_capacity(key.len() + 8);
        ukey.extend_from_slice(key);
        ukey.extend_from_slice(
            &pack_sequence_and_type(sequence, ValueType::TypeDeletion as u8).to_le_bytes(),
        );
        self.list.put(ukey, vec![]);
    }

    fn mem_size(&self) -> usize {
        self.list.mem_size() as usize
    }

    fn name(&self) -> &str {
        "SkipListMemtable"
    }

    fn cmp(&self, start: &[u8], end: &[u8]) -> Ordering {
        self.comp.compare_key(start, end)
    }
}
