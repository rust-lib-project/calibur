use crate::common::{InternalKeyComparator, RandomAccessFileReader, Result};
use crate::table::block_based::block::{read_block_from_file, Block, IndexBlockIter};
use crate::table::format::BlockHandle;
use std::sync::Arc;

pub struct IndexReader {
    index_block: Arc<Block>,
    index_key_includes_seq: bool,
}

impl IndexReader {
    pub async fn open(
        file: &RandomAccessFileReader,
        handle: &BlockHandle,
        global_seqno: u64,
        index_key_includes_seq: bool,
    ) -> Result<IndexReader> {
        let index_block = read_block_from_file(file, handle, global_seqno).await?;
        let reader = IndexReader {
            index_block,
            index_key_includes_seq,
        };
        Ok(reader)
    }

    pub fn new_iterator(&self, comparator: Arc<InternalKeyComparator>) -> Box<IndexBlockIter> {
        let iter = if self.index_key_includes_seq {
            self.index_block
                .new_index_iterator(comparator, self.index_key_includes_seq)
        } else {
            self.index_block.new_index_iterator(
                comparator.get_user_comparator().clone(),
                self.index_key_includes_seq,
            )
        };
        Box::new(iter)
    }
}
