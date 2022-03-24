mod block_based;
mod format;
mod table_cache;
mod table_properties;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::common::format::ValueType;
use crate::common::{CompressionType, RandomAccessFile};
use crate::common::{
    InternalKeyComparator, InternalKeySliceTransform, Result, SliceTransform, WritableFileWriter,
};
use crate::iterator::{AsyncIterator, InternalIterator};
use crate::options::ReadOptions;
use crate::KeyComparator;
use async_trait::async_trait;
pub use block_based::{
    BlockBasedTableFactory, BlockBasedTableOptions, FilterBlockFactory, FullFilterBlockFactory,
};
pub use table_cache::{TableCache, TableReaderWrapper};

#[async_trait]
pub trait TableReader: 'static + Sync + Send {
    async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn new_iterator_opts(&self, opts: &ReadOptions) -> Box<dyn AsyncIterator>;
    fn new_iterator(&self) -> Box<dyn AsyncIterator> {
        self.new_iterator_opts(&ReadOptions::default())
    }
}

#[async_trait]
pub trait TableFactory: Send + Sync {
    fn name(&self) -> &'static str;
    async fn open_reader(
        &self,
        options: &TableReaderOptions,
        file: Box<dyn RandomAccessFile>,
    ) -> Result<Box<dyn TableReader>>;

    fn new_builder(
        &self,
        options: &TableBuilderOptions,
        w: Box<WritableFileWriter>,
    ) -> Result<Box<dyn TableBuilder>>;
}

#[async_trait]
pub trait TableBuilder: Send {
    fn last_key(&self) -> &[u8];
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn should_flush(&self) -> bool;
    async fn finish(&mut self) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}

pub trait TablePropertiesCollector: Send + Sync {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        _tp: ValueType,
        _seqno: u64,
        _file_size: usize,
    ) -> Result<()> {
        self.add(key, value)
    }
    fn finish(&mut self) -> Result<HashMap<String, Vec<u8>>>;
}

pub trait TablePropertiesCollectorFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_table_properties_collector(&self, cf_id: u32) -> Box<dyn TablePropertiesCollector>;
}

pub struct TableBuilderOptions {
    pub skip_filter: bool,
    pub internal_comparator: InternalKeyComparator,
    pub column_family_name: String,
    pub target_file_size: usize,
    pub compression_type: CompressionType,
    pub column_family_id: u32,
    // TODO: add user properties to sst
    pub table_properties_collector_factories: Vec<Arc<dyn TablePropertiesCollectorFactory>>,
}

impl Default for TableBuilderOptions {
    fn default() -> Self {
        TableBuilderOptions {
            skip_filter: false,
            internal_comparator: InternalKeyComparator::default(),
            column_family_name: "default".to_string(),
            target_file_size: 32 * 1024 * 1024, // 8MB
            compression_type: CompressionType::NoCompression,
            column_family_id: 0,
            table_properties_collector_factories: vec![],
        }
    }
}

pub struct TableReaderOptions {
    pub prefix_extractor: Arc<dyn SliceTransform>,
    pub internal_comparator: InternalKeyComparator,
    pub skip_filters: bool,
    pub level: u32,
    pub file_size: usize,
    pub largest_seqno: u64,
}

impl Default for TableReaderOptions {
    fn default() -> Self {
        TableReaderOptions {
            prefix_extractor: Arc::new(InternalKeySliceTransform::default()),
            internal_comparator: InternalKeyComparator::default(),
            skip_filters: false,
            level: 0,
            file_size: 0,
            largest_seqno: 0,
        }
    }
}

struct InMemTableReaderInner {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

pub struct InMemTableReader {
    inner: Arc<InMemTableReaderInner>,
}

impl InMemTableReader {
    pub fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let mut tree = BTreeMap::default();
        for (k, v) in data {
            tree.insert(k, v);
        }
        Self {
            inner: Arc::new(InMemTableReaderInner { data: tree }),
        }
    }
}

pub struct InMemTableIterator {
    table: Vec<(Vec<u8>, Vec<u8>)>,
    cursor: usize,
}

impl InMemTableIterator {
    pub fn new(mut table: Vec<(Vec<u8>, Vec<u8>)>, comparator: &dyn KeyComparator) -> Self {
        table.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        InMemTableIterator { table, cursor: 0 }
    }
}

#[async_trait]
impl TableReader for InMemTableReader {
    async fn get(&self, _opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = self.inner.data.get(key).cloned();
        Ok(v)
    }

    fn new_iterator_opts(&self, _opts: &ReadOptions) -> Box<dyn AsyncIterator> {
        let mut data = vec![];
        for (k, v) in self.inner.data.iter() {
            data.push((k.clone(), v.clone()));
        }
        Box::new(InMemTableIterator {
            table: data,
            cursor: 0,
        })
    }
}

#[async_trait]
impl AsyncIterator for InMemTableIterator {
    fn valid(&self) -> bool {
        self.cursor < self.table.len()
    }

    async fn seek(&mut self, key: &[u8]) {
        self.cursor = match self.table.binary_search_by(|a| a.0.as_slice().cmp(key)) {
            Ok(offset) => offset,
            Err(offset) => offset,
        };
    }

    async fn seek_for_prev(&mut self, _key: &[u8]) {
        unimplemented!()
    }

    async fn seek_to_first(&mut self) {
        self.cursor = 0;
    }

    async fn seek_to_last(&mut self) {
        self.cursor = self.table.len() - 1;
    }

    async fn next(&mut self) {
        self.cursor += 1;
    }

    async fn prev(&mut self) {
        if self.cursor == 0 {
            self.cursor = self.table.len();
            return;
        }
        self.cursor -= 1;
    }

    fn key(&self) -> &[u8] {
        &self.table[self.cursor].0
    }

    fn value(&self) -> &[u8] {
        &self.table[self.cursor].1
    }
}
