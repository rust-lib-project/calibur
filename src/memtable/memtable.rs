use crate::common::format::extract_user_key;
use crate::common::InternalKeyComparator;
use crate::iterator::{AsyncIterator, InternalIterator};
use crate::memtable::skiplist_rep::SkipListMemtableRep;
use crate::memtable::MemtableRep;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct Memtable {
    rep: Box<dyn MemtableRep>,
    mem_next_logfile_number: AtomicU64,
    id: u64,
    comparator: InternalKeyComparator,
    pending_schedule: AtomicBool,
    first_seqno: AtomicU64,
    earliest_seqno: AtomicU64,
    max_write_buffer_size: usize,
}

impl Memtable {
    pub fn new(
        id: u64,
        max_write_buffer_size: usize,
        comparator: InternalKeyComparator,
        earliest_seq: u64,
    ) -> Self {
        Self {
            rep: Box::new(SkipListMemtableRep::new(
                comparator.clone(),
                max_write_buffer_size,
            )),
            comparator,
            mem_next_logfile_number: AtomicU64::new(0),
            id,
            pending_schedule: AtomicBool::new(false),
            max_write_buffer_size,
            first_seqno: AtomicU64::new(0),
            earliest_seqno: AtomicU64::new(earliest_seq),
        }
    }

    pub fn new_iterator(&self) -> Box<dyn InternalIterator> {
        self.rep.new_iterator()
    }

    pub fn new_async_iterator(&self) -> Box<dyn AsyncIterator> {
        let iter = self.rep.new_iterator();
        Box::new(MemIteratorWrapper { inner: iter })
    }

    pub fn get_comparator(&self) -> InternalKeyComparator {
        self.comparator.clone()
    }

    pub fn add(&self, key: &[u8], value: &[u8], sequence: u64) {
        self.update_first_sequence(sequence);
        self.rep.add(key, value, sequence);
    }

    pub fn delete(&self, key: &[u8], sequence: u64) {
        self.update_first_sequence(sequence);
        self.rep.delete(key, sequence);
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut iter = self.rep.new_iterator();
        iter.seek(key);
        if iter.valid()
            && self
                .comparator
                .get_user_comparator()
                .same_key(extract_user_key(key), extract_user_key(iter.key()))
        {
            return Some(iter.value().to_vec());
        }
        None
    }

    pub fn set_next_log_number(&self, num: u64) {
        self.mem_next_logfile_number.store(num, Ordering::Release);
    }

    pub fn get_next_log_number(&self) -> u64 {
        self.mem_next_logfile_number.load(Ordering::Acquire)
    }

    // TODO: support write buffer manager
    pub fn should_flush(&self) -> bool {
        self.rep.mem_size() as usize > self.max_write_buffer_size
    }

    pub fn get_mem_size(&self) -> usize {
        self.rep.mem_size() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.first_seqno.load(Ordering::Acquire) == 0
    }

    pub fn mark_schedule_flush(&self) {
        self.pending_schedule.store(true, Ordering::Release);
    }

    pub fn is_pending_schedule(&self) -> bool {
        self.pending_schedule.load(Ordering::Acquire)
    }

    fn update_first_sequence(&self, sequence: u64) {
        let mut cur_seq_num = self.first_seqno.load(Ordering::Relaxed);
        while cur_seq_num == 0 || sequence < cur_seq_num {
            match self.first_seqno.compare_exchange_weak(
                cur_seq_num,
                sequence,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(v) => cur_seq_num = v,
            }
        }
        let mut cur_earliest_seqno = self.earliest_seqno.load(Ordering::Relaxed);
        while sequence < cur_earliest_seqno {
            match self.earliest_seqno.compare_exchange_weak(
                cur_earliest_seqno,
                sequence,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(v) => cur_earliest_seqno = v,
            }
        }
    }
}

pub struct MemIteratorWrapper {
    inner: Box<dyn InternalIterator>,
}

#[async_trait::async_trait]
impl AsyncIterator for MemIteratorWrapper {
    fn valid(&self) -> bool {
        self.inner.valid()
    }

    async fn seek(&mut self, key: &[u8]) {
        self.inner.seek(key)
    }

    async fn seek_to_first(&mut self) {
        self.inner.seek_to_first()
    }

    async fn seek_to_last(&mut self) {
        self.inner.seek_to_last()
    }

    async fn seek_for_prev(&mut self, key: &[u8]) {
        self.inner.seek_for_prev(key)
    }

    async fn next(&mut self) {
        self.inner.next()
    }

    async fn prev(&mut self) {
        self.inner.prev()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
