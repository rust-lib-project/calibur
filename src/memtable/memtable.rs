use super::list::{IterRef, Skiplist};
use crate::common::format::{pack_sequence_and_type, ValueType};
use crate::common::InternalKeyComparator;
use crate::table::InternalIterator;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Memtable {
    list: Skiplist,
    mem_next_logfile_number: AtomicU64,
    id: u64,
}

pub struct MemIterator {
    inner: IterRef<Skiplist>,
}

impl Memtable {
    pub fn new(id: u64, comparator: InternalKeyComparator) -> Self {
        Self {
            list: Skiplist::with_capacity(comparator, 4 * 1024 * 1024),
            mem_next_logfile_number: AtomicU64::new(0),
            id,
        }
    }

    pub fn new_iterator(&self) -> Box<dyn InternalIterator> {
        let iter = self.list.iter();
        Box::new(MemIterator { inner: iter })
    }

    pub fn add(&self, key: &[u8], value: &[u8], sequence: u64, tp: ValueType) {
        let mut ukey = Vec::with_capacity(key.len() + 8);
        ukey.extend_from_slice(key);
        ukey.extend_from_slice(&pack_sequence_and_type(sequence, tp as u8).to_le_bytes());
        self.insert_to(ukey.into(), value.into());
    }

    pub fn delete(&self, key: &[u8], sequence: u64) {
        let mut ukey = Vec::with_capacity(key.len() + 8);
        ukey.extend_from_slice(key);
        ukey.extend_from_slice(
            &pack_sequence_and_type(sequence, ValueType::TypeDeletion as u8).to_le_bytes(),
        );
        self.insert_to(ukey.into(), vec![]);
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {
        self.insert_to(key.into(), value.into());
    }

    pub fn insert_to(&self, key: Vec<u8>, value: Vec<u8>) {
        self.list.put(key, value);
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn set_next_log_number(&self, num: u64) {
        self.mem_next_logfile_number.store(num, Ordering::Release);
    }

    pub fn get_next_log_number(&self) -> u64 {
        self.mem_next_logfile_number.load(Ordering::Acquire)
    }
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
