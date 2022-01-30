use std::sync::Arc;
use crate::common::{DefaultUserComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, InternalKeyComparator, options::ReadOptions, RandomAccessFile, RandomAccessFileReader, Result};
use crate::table::block_based::block::{Block, DataBlockIter, read_block_from_file};
use crate::table::block_based::{BLOCK_TRAILER_SIZE, BlockBasedTableOptions};
use crate::table::format::{
    BlockHandle, Footer, BLOCK_BASED_TABLE_MAGIC_NUMBER, NEW_VERSIONS_ENCODED_LENGTH,
};
use crate::table::{TableReader, InternalIterator, TableReaderOptions};
use async_trait::async_trait;
use crate::table::block_based::index_reader::IndexReader;
use crate::table::block_based::meta_block::read_properties;
use crate::table::block_based::table_iterator::BlockBasedTableIterator;
use crate::table::table_properties::{seek_to_properties_block, TableProperties};

pub struct BlockBasedTable {
    footer: Footer,
    file: Box<RandomAccessFileReader>,
    index_reader: IndexReader,
    properties: Option<Box<TableProperties>>,
    internal_comparator: Arc<InternalKeyComparator>,
}


impl BlockBasedTable {
    pub async fn open(
        opts: &TableReaderOptions,
        table_opts: BlockBasedTableOptions,
        file: Box<RandomAccessFileReader>,
    ) -> Result<Self> {
        // Read in the following order:
        //    1. Footer
        //    2. [metaindex block]
        //    3. [meta block: properties]
        //    4. [meta block: range deletion tombstone]
        //    5. [meta block: compression dictionary]
        //    6. [meta block: index]
        //    7. [meta block: filter]

        // TODO: prefetch file for meta block and index block.
        let footer = read_footer_from_file(file.as_ref(), opts.file_size).await?;
        let meta_block = read_block_from_file(file.as_ref(),
                                              &footer.metaindex_handle,
            DISABLE_GLOBAL_SEQUENCE_NUMBER).await?;
        let mut meta_iter = meta_block.new_data_iterator(Arc::new(DefaultUserComparator::default()));
        let mut global_seqno = DISABLE_GLOBAL_SEQUENCE_NUMBER;
        let properties = if seek_to_properties_block(&mut meta_iter)? {
            let (properties, _handle) = read_properties(meta_iter.value(), file.as_ref(), &footer).await?;
            // TODO: checksum
            global_seqno = get_global_seqno(properties.as_ref(), opts.largest_seqno)?;
            Some(properties)
        } else {
            None
        };
        // TODO: open filter reader
        // let policy = if opts.skip_filters {
        //     None
        // } else {
        //     Some(table_opts.filter_factory.clone())
        // };
        let index_reader = IndexReader::open(file.as_ref(), &footer.index_handle, global_seqno).await?;
        let mut table = BlockBasedTable { footer, file, properties, index_reader,
            internal_comparator: Arc::new(opts.internal_comparator.clone()) };
        Ok(table)
    }



    async fn read_properties_block<I: InternalIterator>(&mut self, meta_iter: &mut I) -> Result<()> {
        if seek_to_properties_block(meta_iter)? {
            let (properties, _handle) = read_properties(meta_iter.value(), self.file.as_ref(), &self.footer).await?;
            self.properties = Some(properties);
            // TODO: checksum
        }
        Ok(())
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
    footer.decode_from(&data[..sz])?;
    assert_eq!(footer.table_magic_number, BLOCK_BASED_TABLE_MAGIC_NUMBER);
    Ok(footer)
}

fn get_global_seqno(propertoes: &TableProperties, largest_seqno: u64) -> Result<u64> {
    if propertoes.version < 2 {
        return Ok(DISABLE_GLOBAL_SEQUENCE_NUMBER);
    }
    Ok(propertoes.global_seqno)
}

#[async_trait]
impl TableReader for BlockBasedTable {
    async fn get(&self, opts: &ReadOptions, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn new_iterator(&self, opts: &ReadOptions) -> Box<dyn InternalIterator> {
        let index_iter = self.index_reader.new_iterator(self.internal_comparator.clone());
        let iter = BlockBasedTableIterator::new(
            self.internal_comparator.clone(), index_iter);
        Box::new(iter)
    }
}