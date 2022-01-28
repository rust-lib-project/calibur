mod block_based;
mod format;
mod table_properties;

use crate::common::options::{CompressionType, ReadOptions};
use crate::common::{InternalKeyComparator, Result, SliceTransform};
use crate::common::{RandomAccessFile, WritableFileWriter};
use async_trait::async_trait;
use std::sync::Arc;

pub trait TableReaderIterator {
    fn valid(&self) -> bool;
    fn seek(&mut self, key: &[u8]);
    fn seek_for_prev(&mut self, key: &[u8]);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

#[async_trait]
pub trait TableReader: 'static + Sync + Send {
    async fn get(&self, opts: &ReadOptions, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>>;
    fn new_iterator(&self, opts: &ReadOptions) -> Box<dyn TableReaderIterator>;
}

pub trait TableBuilder {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}

#[async_trait]
pub trait TableFactory {
    fn name(&self) -> &'static str;
    async fn open_reader(
        &self,
        options: &TableReaderOptions,
        file: Arc<dyn RandomAccessFile>,
    ) -> Result<Arc<dyn TableReader>>;

    fn new_builder(
        &self,
        options: &TableBuilderOptions,
        w: WritableFileWriter,
    ) -> Result<Box<dyn TableBuilder>>;
}

pub struct TableBuilderOptions {
    skip_filter: bool,
    internal_comparator: InternalKeyComparator,
    column_family_name: String,
    target_file_size: usize,
    compression_type: CompressionType,
    column_family_id: u32,
}

pub struct TableReaderOptions {
    prefix_extractor: Arc<dyn SliceTransform>,
    internal_comparator: InternalKeyComparator,
}
