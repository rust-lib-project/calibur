use crate::memtable::Memtable;
use crate::version::{MemtableList, SuperVersion, Version};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{atomic::AtomicU64, Arc, Mutex};

pub struct ColumnFamily {
    mem: Arc<Memtable>,
    imms: MemtableList,
    super_version: Arc<SuperVersion>,

    // An ordinal representing the current SuperVersion. Updated by
    // InstallSuperVersion(), i.e. incremented every time super_version_
    // changes.
    super_version_number: Arc<AtomicU64>,
    version: Arc<Version>,
    valid: AtomicBool,
    id: usize,
}

impl ColumnFamily {
    pub fn valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    pub fn invalid_column_family(&self) {
        self.valid.store(false, Ordering::Release);
    }

    pub fn get_memtable(&self) -> Arc<Memtable> {
        self.mem.clone()
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn should_flush(&self) -> bool {
        false
    }

    pub fn switch_memtable(&self, mem: Arc<Memtable>) -> ColumnFamily {
        let imms = self.imms.add(self.mem.clone());
        self.super_version_number.fetch_add(1, Ordering::Release);
        let super_version_number = self.super_version_number.load(Ordering::Relaxed);
        let super_version = Arc::new(SuperVersion::new(
            self.mem.clone(),
            self.imms.clone(),
            self.version.clone(),
            super_version_number,
        ));
        ColumnFamily {
            mem,
            imms,
            version: self.version.clone(),
            super_version_number: self.super_version_number.clone(),
            super_version,
            id: self.id,
            valid: AtomicBool::new(true),
        }
    }

    pub fn create_memtable(&self) -> Memtable {}
}
