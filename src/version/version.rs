use crate::memtable::Memtable;
use crate::version::version_storage_info::VersionStorageInfo;
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
pub struct MemtableList {
    pub mems: Vec<Arc<Memtable>>,
}

impl MemtableList {
    pub fn add(&self, mem: Arc<Memtable>) -> MemtableList {
        let mut mems = self.mems.clone();
        mems.push(mem);
        MemtableList { mems }
    }
}

pub struct Version {
    storage: VersionStorageInfo,
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
