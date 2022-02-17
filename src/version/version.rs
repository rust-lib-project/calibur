use crate::memtable::Memtable;
use crate::version::version_storage_info::VersionStorageInfo;
use crate::version::{FileMetaData, TableFile};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct MemtableList {
    pub mems: Vec<Arc<Memtable>>,
}

impl MemtableList {
    pub fn add(&self, mem: Arc<Memtable>) -> MemtableList {
        let mut mems = self.mems.clone();
        mems.push(mem);
        MemtableList { mems }
    }

    pub fn remove(&self, dels: Vec<u64>) -> MemtableList {
        let mut imms = vec![];
        for m in &self.mems {
            let mut keep = true;
            for del in &dels {
                if *del == m.get_id() {
                    keep = false;
                }
            }
            if keep {
                imms.push(m.clone());
            }
        }
        MemtableList { mems: imms }
    }
}

pub struct Version {
    cf_id: u32,
    log_number: u64,
    cf_name: String,
    comparator: String,
    storage: VersionStorageInfo,
}

impl Version {
    pub fn new(
        cf_id: u32,
        cf_name: String,
        comparator: String,
        tables: Vec<Arc<TableFile>>,
    ) -> Self {
        Version {
            storage: VersionStorageInfo::new(tables),
            cf_id,
            cf_name,
            log_number: 0,
            comparator,
        }
    }

    pub fn apply(
        &self,
        to_add: Vec<Arc<TableFile>>,
        to_delete: Vec<Arc<TableFile>>,
        log_number: u64,
    ) -> Self {
        let storage = self.storage.apply(to_add, to_delete);
        Version {
            storage,
            cf_id: self.cf_id,
            cf_name: self.cf_name.clone(),
            log_number: std::cmp::max(self.log_number, log_number),
            comparator: self.comparator.clone(),
        }
    }

    pub fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub fn get_cf_id(&self) -> u32 {
        self.cf_id
    }

    pub fn get_cf_name(&self) -> &str {
        &self.cf_name
    }

    pub fn get_comparator_name(&self) -> &str {
        &self.comparator
    }

    pub fn get_level_num(&self) -> usize {
        self.storage.size()
    }

    pub fn scan<F: FnMut(&FileMetaData)>(&self, f: F, level: usize) {
        self.storage.scan(f, level);
    }
}

pub struct SuperVersion {
    pub mem: Arc<Memtable>,
    pub imms: MemtableList,
    pub current: Arc<Version>,
    pub version_number: u64,
    pub valid: AtomicBool,
}

impl SuperVersion {
    pub fn new(
        mem: Arc<Memtable>,
        imms: MemtableList,
        current: Arc<Version>,
        version_number: u64,
    ) -> SuperVersion {
        SuperVersion {
            mem,
            imms,
            current,
            version_number,
            valid: AtomicBool::new(true),
        }
    }
}
