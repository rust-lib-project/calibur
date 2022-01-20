use crate::memtable::Memtable;
use crate::version::version_storage_info::VersionStorageInfo;
use std::sync::Arc;

pub struct MemtableList {
    mems: Vec<Arc<Memtable>>,
}

pub struct Version {
    storage: VersionStorageInfo,
}

pub struct SuperVersion {
    pub mem: Arc<Memtable>,
    pub imms: Arc<MemtableList>,
    pub current: Arc<Version>,
    pub version_number: u64,
}

impl SuperVersion {
    fn new(
        mem: Arc<Memtable>,
        imms: Arc<MemtableList>,
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
