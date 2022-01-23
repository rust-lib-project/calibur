mod block_based;
mod table_properties;

use crate::common::Result;
use crate::common::{RandomAccessFileReader, WritableFileWriter};
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

pub trait TableReader {
    fn get(&self, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>>;
    fn new_iterator(&self) -> Box<dyn TableReaderIterator>;
}

pub trait TableBuilder {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}

pub trait TableFactory {
    fn name(&self) -> &'static str;
    fn new_reader(&self, file: Arc<dyn RandomAccessFileReader>) -> Result<Arc<dyn TableReader>>;
    fn new_builder(&self, w: WritableFileWriter) -> Result<Box<dyn TableBuilder>>;
}
