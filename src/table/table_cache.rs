use crate::common::{make_table_file_name, Result};
use crate::table::{TableReader, TableReaderOptions};
use crate::util::{CachableEntry, LRUCache};
use crate::version::FileMetaData;
use crate::{ColumnFamilyOptions, ImmutableDBOptions};
use std::sync::Arc;

pub struct TableReaderWrapper {
    inner: CachableEntry<Box<dyn TableReader>>,
}

impl TableReaderWrapper {
    pub fn as_reader(&self) -> &Box<dyn TableReader> {
        self.inner.value()
    }
}

pub struct TableCache {
    cache: Arc<LRUCache<Box<dyn TableReader>>>,
    options: Arc<ImmutableDBOptions>,
    cf_options: Arc<ColumnFamilyOptions>,
}

impl TableCache {
    pub async fn get_table_reader(
        &self,
        m: &FileMetaData,
    ) -> Result<CachableEntry<Box<dyn TableReader>>> {
        if let Some(entry) = self.cache.lookup(m.id()) {
            return Ok(entry);
        }
        let fname = make_table_file_name(&self.options.db_path, m.id());
        let file = self.options.fs.open_random_access_file(&fname)?;
        let read_opts = TableReaderOptions {
            file_size: m.fd.file_size as usize,
            level: m.level,
            largest_seqno: m.fd.largest_seqno,
            internal_comparator: self.cf_options.comparator.clone(),
            prefix_extractor: self.cf_options.prefix_extractor.clone(),
            ..Default::default()
        };
        let table_reader = self
            .cf_options
            .factory
            .open_reader(&read_opts, file)
            .await?;
        let entry = self.cache.insert(m.id(), 1, table_reader).unwrap();
        Ok(entry)
    }
}
