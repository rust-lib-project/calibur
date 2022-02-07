use crate::memtable::Memtable;
use crate::version::version_storage_info::VersionStorageInfo;
use crate::version::{FileMetaData, VersionEdit};
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
    storage: VersionStorageInfo,
}

impl Version {
    pub fn new(edits: Vec<VersionEdit>) -> Self {
        Version {
            storage: VersionStorageInfo::new(edits),
        }
    }

    pub fn apply(&self, edits: Vec<VersionEdit>) -> Self {
        let info = self.storage.apply(edits);
        Version { storage: info }
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
        }
    }
}
