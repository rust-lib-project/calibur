use super::list::Skiplist;
use crate::common::InternalKeyComparator;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Memtable {
    list: Skiplist,
    mem_next_logfile_number: AtomicU64,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            list: Skiplist::with_capacity(InternalKeyComparator::default(), 4 * 1024 * 1024),
            mem_next_logfile_number: AtomicU64::new(0),
        }
    }

    pub fn set_next_log_number(&self, num: u64) {
        self.mem_next_logfile_number.store(num, Ordering::Release);
    }

    pub fn get_next_log_number(&self) -> u64 {
        self.mem_next_logfile_number.load(Ordering::Acquire)
    }
}
