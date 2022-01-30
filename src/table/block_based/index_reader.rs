use crate::common::{RandomAccessFileReader, Result};
use crate::table::block_based::block::{Block, read_block_from_file};
use crate::table::format::BlockHandle;

pub struct IndexReader {
    index_block: Box<Block>,
}

impl IndexReader {
    pub async fn open(file: &RandomAccessFileReader, handle: &BlockHandle,global_seqno: u64) -> Result<IndexReader> {
        let index_block = read_block_from_file(file, handle, global_seqno).await?;
        let reader = IndexReader { index_block };
        Ok(reader)
    }
}