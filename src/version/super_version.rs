use super::version::MemtableList;
use crate::common::options::ReadOptions;
use crate::common::Result;
use crate::memtable::Memtable;
use crate::version::Version;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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

    pub async fn get(
        &self,
        opts: &ReadOptions,
        key: &[u8],
        sequence: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut ikey = Vec::with_capacity(key.len() + 8);
        ikey.extend_from_slice(key);
        ikey.extend_from_slice(&sequence.to_le_bytes());
        if let Some(v) = self.mem.get(&ikey) {
            return Ok(Some(v));
        }
        let mut l = self.imms.mems.len();
        while l > 0 {
            if let Some(v) = self.imms.mems[l - 1].get(&ikey) {
                return Ok(Some(v));
            }
            l -= 1;
        }
        self.current.get(opts, &ikey).await
    }
}
