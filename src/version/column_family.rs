use crate::common::InternalKeyComparator;
use crate::memtable::{Memtable, Memtable};
use crate::options::ColumnFamilyOptions;
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
    comparator: InternalKeyComparator,
    id: u32,
    log_number: u64,
    name: String,
    options: Arc<ColumnFamilyOptions>,
}

impl ColumnFamily {
    pub fn new(
        id: u32,
        name: String,
        m: Memtable,
        comparator: InternalKeyComparator,
        version: Arc<Version>,
        options: ColumnFamilyOptions,
    ) -> Self {
        let mem = Arc::new(m);
        let options = Arc::new(options);
        Self {
            log_number: version.get_log_number(),
            mem: mem.clone(),
            imms: Default::default(),
            super_version: Arc::new(SuperVersion {
                mem,
                imms: Default::default(),
                current: version.clone(),
                version_number: 0,
                column_family_options: options.clone(),
                valid: AtomicBool::new(true),
            }),
            super_version_number: Arc::new(AtomicU64::new(0)),
            version,
            id,
            comparator,
            name,
            options,
        }
    }

    pub fn get_memtable(&self) -> Arc<Memtable> {
        self.mem.clone()
    }

    pub fn get_version(&self) -> Arc<Version> {
        self.version.clone()
    }

    pub fn get_options(&self) -> Arc<ColumnFamilyOptions> {
        self.options.clone()
    }

    pub fn get_id(&self) -> u32 {
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

    pub fn get_super_version(&self) -> Arc<SuperVersion> {
        self.super_version.clone()
    }

    pub fn install_version(&mut self, mems: Vec<u64>, new_version: Version) -> Arc<Version> {
        let imms = self.imms.remove(mems);
        self.super_version_number.fetch_add(1, Ordering::Release);
        let super_version_number = self.super_version_number.load(Ordering::Relaxed);
        let version = Arc::new(new_version);
        let super_version = Arc::new(SuperVersion::new(
            self.mem.clone(),
            imms.clone(),
            version.clone(),
            self.options.clone(),
            super_version_number,
        ));
        self.super_version.valid.store(false, Ordering::Release);
        self.super_version = super_version;
        self.version = version.clone();
        self.imms = imms;
        version
    }

    pub fn switch_memtable(&mut self, mem: Arc<Memtable>) {
        let imms = self.imms.add(self.mem.clone());
        self.super_version_number.fetch_add(1, Ordering::Release);
        let super_version_number = self.super_version_number.load(Ordering::Relaxed);
        let super_version = Arc::new(SuperVersion::new(
            mem.clone(),
            imms.clone(),
            self.version.clone(),
            self.options.clone(),
            super_version_number,
        ));
        self.super_version.valid.store(false, Ordering::Release);
        self.super_version = super_version;
        self.imms = imms;
        self.mem = mem;
    }

    pub fn create_memtable(&self, id: u64, earliest_seq: u64) -> Memtable {
        Memtable::new(
            id,
            self.options.write_buffer_size,
            self.comparator.clone(),
            earliest_seq,
        )
    }
}
