use crate::common::format::pack_sequence_and_type;
use crate::common::{Result, VALUE_TYPE_FOR_SEEK};
use crate::iterator::{AsyncIterator, AsyncMergingIterator};
use crate::memtable::Memtable;
use crate::options::ReadOptions;
use crate::version::Version;
use crate::ColumnFamilyOptions;
use std::sync::Arc;

pub struct SuperVersion {
    pub id: u32,
    pub mem: Arc<Memtable>,
    pub imms: Vec<Arc<Memtable>>,
    pub current: Arc<Version>,
    pub column_family_options: Arc<ColumnFamilyOptions>,
    pub version_number: u64,
}

impl SuperVersion {
    pub fn new(
        id: u32,
        mem: Arc<Memtable>,
        imms: Vec<Arc<Memtable>>,
        current: Arc<Version>,
        column_family_options: Arc<ColumnFamilyOptions>,
        version_number: u64,
    ) -> SuperVersion {
        SuperVersion {
            id,
            mem,
            imms,
            current,
            column_family_options,
            version_number,
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
        ikey.extend_from_slice(
            &pack_sequence_and_type(sequence, VALUE_TYPE_FOR_SEEK).to_le_bytes(),
        );
        if let Some(v) = self.mem.get(&ikey) {
            return Ok(Some(v));
        }
        let l = self.imms.len();
        for i in 0..l {
            if let Some(v) = self.imms[l - i - 1].get(&ikey) {
                return Ok(Some(v));
            }
        }
        self.current.get_storage_info().get(opts, &ikey).await
    }

    pub fn new_iterator(&self, opts: &ReadOptions) -> Result<Box<dyn AsyncIterator>> {
        let mut iters = vec![self.mem.new_async_iterator()];
        let l = self.imms.len();
        for i in 0..l {
            iters.push(self.imms[l - i - 1].new_async_iterator());
        }
        self.current
            .get_storage_info()
            .append_iterator_to(opts, &mut iters);
        Ok(Box::new(AsyncMergingIterator::new(
            iters,
            self.column_family_options.comparator.clone(),
        )))
    }
}
