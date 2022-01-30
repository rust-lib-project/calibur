use std::sync::Arc;
use crate::common::{InternalKeyComparator, KeyComparator, RandomAccessFileReader, Result};
use crate::table::block_based::block::{Block, read_block_from_file};
use crate::table::format::BlockHandle;
use crate::table::InternalIterator;

pub struct IndexReader {
    index_block: Box<Block>,
}

impl IndexReader {
    pub async fn open(file: &RandomAccessFileReader, handle: &BlockHandle,global_seqno: u64) -> Result<IndexReader> {
        let index_block = read_block_from_file(file, handle, global_seqno).await?;
        let reader = IndexReader { index_block };
        Ok(reader)
    }
    pub fn new_iterator(&self, comparator: Arc<dyn KeyComparator>) -> Box<dyn InternalIterator> {
        let iter = self.index_block.new_index_iterator(comparator);
        Box::new(iter)
    }
}