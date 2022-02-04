use crate::version::column_family::ColumnFamily;
use crate::version::Version;
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

pub struct VersionSetKernal {
    next_file_number: atomic::AtomicU64,
    next_mem_number: atomic::AtomicU64,
    last_sequence: atomic::AtomicU64,
}

impl VersionSetKernal {
    pub fn current_next_file_number(&self) -> u64 {
        self.next_file_number.load(atomic::Ordering::Acquire)
    }

    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, atomic::Ordering::SeqCst)
    }

    pub fn new_memtable_number(&self) -> u64 {
        self.next_mem_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(atomic::Ordering::Acquire)
    }

    pub fn fetch_add_file_number(&self, n: u64) -> u64 {
        self.next_file_number.fetch_add(n, atomic::Ordering::SeqCst)
    }

    pub fn set_last_sequence(&self, v: u64) {
        self.last_sequence.store(v, atomic::Ordering::Release);
    }

    pub fn mark_file_number_used(&self, v: u64) {
        let mut old = self.next_file_number.load(atomic::Ordering::Acquire);
        while old < v {
            match self.next_file_number.compare_exchange(
                old,
                v,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(x) => old = x,
            }
        }
    }
}

pub struct VersionSet {
    kernal: Arc<VersionSetKernal>,
    column_family_set: Vec<Option<Arc<ColumnFamily>>>,
}

impl VersionSet {
    pub fn get_column_family(&self, id: usize) -> Option<Arc<ColumnFamily>> {
        if id > self.column_family_set.len() {
            None
        } else {
            self.column_family_set[id].clone()
        }
    }

    pub fn new_file_number(&self) -> u64 {
        self.kernal.new_file_number()
    }

    pub fn set_column_family(&mut self, cf: Arc<ColumnFamily>) {
        let cf_id = cf.get_id();
        if cf_id >= self.column_family_set.len() {
            self.column_family_set.resize(cf_id + 1, None);
        }
        self.column_family_set[cf_id] = Some(cf);
    }

    pub fn should_flush(&self) -> bool {
        for cf in self.column_family_set.iter() {
            if cf.as_ref().map_or(false, |cf| cf.should_flush()) {
                return true;
            }
        }
        false
    }

    pub fn get_column_familys(&self) -> Vec<Arc<ColumnFamily>> {
        let mut cfs = vec![];
        for c in self.column_family_set.iter() {
            if let Some(cf) = c {
                cfs.push(cf.clone());
            }
        }
        cfs
    }

    pub fn install_version(&mut self, cf_id: u32, mems: Vec<u64>, version: Version) {
        if cf_id as usize > self.column_family_set.len() {
            // TODO: create column faimly
        } else {
            if let Some(cf) = self.column_family_set[cf_id as usize].take() {
                cf.install_version(mems, version);
            }
        }
    }
}
