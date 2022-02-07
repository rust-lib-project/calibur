use crate::common::InternalKeyComparator;
use crate::memtable::Memtable;
use crate::version::{MemtableList, SuperVersion, Version};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{atomic::AtomicU64, Arc};

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
    comparator: InternalKeyComparator,
    id: usize,
    log_number: u64,
    name: String,
}

impl ColumnFamily {
    pub fn new(
        id: usize,
        name: String,
        m: Memtable,
        comparator: InternalKeyComparator,
        version: Arc<Version>,
    ) -> Self {
        let mem = Arc::new(m);
        Self {
            log_number: 0,
            mem: mem.clone(),
            imms: Default::default(),
            super_version: Arc::new(SuperVersion {
                mem,
                imms: Default::default(),
                current: version.clone(),
                version_number: 0,
            }),
            super_version_number: Arc::new(AtomicU64::new(0)),
            version,
            id,
            comparator,
            valid: AtomicBool::new(true),
            name,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    pub fn invalid_column_family(&self) {
        self.valid.store(false, Ordering::Release);
    }

    pub fn get_memtable(&self) -> Arc<Memtable> {
        self.mem.clone()
    }

    pub fn get_version(&self) -> Arc<Version> {
        self.version.clone()
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub fn set_log_number(&mut self, log_number: u64) {
        self.log_number = log_number;
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn should_flush(&self) -> bool {
        false
    }

    pub fn install_version(&self, mems: Vec<u64>, new_version: Version) -> ColumnFamily {
        let imms = self.imms.remove(mems);
        self.super_version_number.fetch_add(1, Ordering::Release);
        let super_version_number = self.super_version_number.load(Ordering::Relaxed);
        let version = Arc::new(new_version);
        let super_version = Arc::new(SuperVersion::new(
            self.mem.clone(),
            imms.clone(),
            version.clone(),
            super_version_number,
        ));
        ColumnFamily {
            name: self.name.clone(),
            mem: self.mem.clone(),
            log_number: self.log_number,
            imms,
            version,
            super_version_number: self.super_version_number.clone(),
            super_version,
            id: self.id,
            comparator: self.comparator.clone(),
            valid: AtomicBool::new(true),
        }
    }

    pub fn switch_memtable(&self, mem: Arc<Memtable>) -> ColumnFamily {
        let imms = self.imms.add(self.mem.clone());
        self.super_version_number.fetch_add(1, Ordering::Release);
        let super_version_number = self.super_version_number.load(Ordering::Relaxed);
        let super_version = Arc::new(SuperVersion::new(
            mem.clone(),
            self.imms.clone(),
            self.version.clone(),
            super_version_number,
        ));
        ColumnFamily {
            log_number: self.log_number,
            mem,
            name: self.name.clone(),
            imms,
            version: self.version.clone(),
            super_version_number: self.super_version_number.clone(),
            super_version,
            id: self.id,
            valid: AtomicBool::new(true),
            comparator: self.comparator.clone(),
        }
    }

    pub fn create_memtable(&self, id: u64) -> Memtable {
        Memtable::new(id, self.comparator.clone())
    }
}
