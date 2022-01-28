use crate::common::{options::ReadOptions, RandomAccessFile, RandomAccessFileReader, Result};
use crate::table::block_based::block::Block;
use crate::table::block_based::BLOCK_TRAILER_SIZE;
use crate::table::format::{
    BlockHandle, Footer, BLOCK_BASED_TABLE_MAGIC_NUMBER, NEW_VERSIONS_ENCODED_LENGTH,
};
use crate::table::{TableReader, TableReaderIterator, TableReaderOptions};
use async_trait::async_trait;
use std::sync::Arc;

pub struct BlockBasedTableIterator {}

impl TableReaderIterator for BlockBasedTableIterator {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }
}

pub struct BlockBasedTable {
    footer: Footer,
    file: Box<RandomAccessFileReader>,
}

#[async_trait]
impl TableReader for BlockBasedTable {
    async fn get(&self, opts: &ReadOptions, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn new_iterator(&self, opts: &ReadOptions) -> Box<dyn TableReaderIterator> {
        let it = BlockBasedTableIterator {};
        Box::new(it)
    }
}

impl BlockBasedTable {
    pub async fn open(
        opts: &TableReaderOptions,
        file: Box<RandomAccessFileReader>,
        file_size: usize,
    ) -> Result<Self> {
        // Read in the following order:
        //    1. Footer
        //    2. [metaindex block]
        //    3. [meta block: properties]
        //    4. [meta block: range deletion tombstone]
        //    5. [meta block: compression dictionary]
        //    6. [meta block: index]
        //    7. [meta block: filter]
        let footer = read_footer_from_file(file.as_ref(), file_size).await?;
        Ok(BlockBasedTable { footer, file })
    }
}

async fn read_footer_from_file(file: &RandomAccessFileReader, file_size: usize) -> Result<Footer> {
    let mut data: [u8; NEW_VERSIONS_ENCODED_LENGTH] = [0u8; NEW_VERSIONS_ENCODED_LENGTH];
    // TODO: prefetch some data to avoid once IO.
    let read_offset = if file_size > NEW_VERSIONS_ENCODED_LENGTH {
        file_size - NEW_VERSIONS_ENCODED_LENGTH
    } else {
        0
    };
    let sz = file
        .read(read_offset, NEW_VERSIONS_ENCODED_LENGTH, &mut data)
        .await?;
    let mut footer = Footer::default();
    footer.decode_from(&data[..sz]);
    assert_eq!(footer.table_magic_number, BLOCK_BASED_TABLE_MAGIC_NUMBER);
    Ok(footer)
}

async fn read_meta_block() -> Box<Block> {}

async fn read_block_from_file(
    file: &RandomAccessFileReader,
    handle: &BlockHandle,
    global_seqno: u64,
) -> Result<Box<Block>> {
    let read_len = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut data = vec![0u8; read_len];
    file.read(handle.offset as usize, read_len, data.as_mut_slice())
        .await?;
    // TODO: uncompress block
    Ok(Box::new(Block::new(data, global_seqno)))
}
