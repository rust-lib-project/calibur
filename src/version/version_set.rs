use crate::common::{Error, InternalKeyComparator, KeyComparator, Result};
use crate::memtable::Memtable;
use crate::version::column_family::ColumnFamily;
use crate::version::{Version, VersionEdit};
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

pub struct VersionSetKernel {
    next_file_number: atomic::AtomicU64,
    next_mem_number: atomic::AtomicU64,
    last_sequence: atomic::AtomicU64,
}

impl VersionSetKernel {
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
    kernal: Arc<VersionSetKernel>,
    column_family_set: Vec<Option<Arc<ColumnFamily>>>,
    comparator: InternalKeyComparator,
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

    pub fn get_comparator_name(&self) -> &str {
        self.comparator.name()
    }

    pub fn create_column_family(&mut self, edit: VersionEdit) -> Result<Arc<Version>> {
        let id = edit.column_family as usize;
        let name = edit.column_family_name.clone();
        let m = Memtable::new(self.kernal.new_memtable_number(), self.comparator.clone());
        let log_number = edit.log_number;
        let new_version = Arc::new(Version::new(vec![edit]));
        let mut cf = ColumnFamily::new(id, name, m, self.comparator.clone(), new_version.clone());
        cf.set_log_number(log_number);
        while self.column_family_set.len() <= id {
            self.column_family_set.push(None);
        }
        self.column_family_set[id] = Some(Arc::new(cf));
        Ok(new_version)
    }

    pub fn install_version(
        &mut self,
        cf_id: u32,
        mems: Vec<u64>,
        version: Version,
    ) -> Result<Arc<Version>> {
        let pos = cf_id as usize;
        if pos > self.column_family_set.len() {
            Err(Error::CompactionError(format!(
                "column faimly has not been created"
            )))
        } else {
            if let Some(cf) = self.column_family_set[cf_id as usize].take() {
                let new_cf = cf.install_version(mems, version);
                let version = new_cf.get_version();
                self.column_family_set[cf_id as usize] = Some(Arc::new(new_cf));
                Ok(version)
            } else {
                Err(Error::CompactionError(format!(
                    "column faimly has been dropped"
                )))
            }
        }
    }
}
