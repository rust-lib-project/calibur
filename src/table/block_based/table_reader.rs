use crate::common::format::extract_user_key;
use crate::common::{
    options::ReadOptions, DefaultUserComparator, InternalKeyComparator, RandomAccessFileReader,
    Result, DISABLE_GLOBAL_SEQUENCE_NUMBER,
};
use crate::table::block_based::block::{
    read_block_content_from_file, read_block_from_file, DataBlockIter,
};
use crate::table::block_based::filter_reader::FilterBlockReader;
use crate::table::block_based::index_reader::IndexReader;
use crate::table::block_based::meta_block::read_properties;
use crate::table::block_based::table_iterator::BlockBasedTableIterator;
use crate::table::block_based::BlockBasedTableOptions;
use crate::table::block_based::{FILTER_BLOCK_PREFIX, FULL_FILTER_BLOCK_PREFIX};
use crate::table::format::{
    BlockHandle, Footer, BLOCK_BASED_TABLE_MAGIC_NUMBER, NEW_VERSIONS_ENCODED_LENGTH,
};
use crate::table::table_properties::{
    seek_to_metablock, seek_to_properties_block, TableProperties,
};
use crate::table::{AsyncIterator, InternalIterator, TableReader, TableReaderOptions};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::sync::Arc;

pub struct BlockBasedTableRep {
    footer: Footer,
    file: Box<RandomAccessFileReader>,
    index_reader: IndexReader,
    properties: Option<Box<TableProperties>>,
    internal_comparator: Arc<InternalKeyComparator>,
    filter_reader: Option<Box<dyn FilterBlockReader>>,
    whole_key_filtering: bool,
    global_seqno: u64,
}

impl BlockBasedTableRep {
    pub async fn new_data_block_iterator(&self, handle: &BlockHandle) -> Result<DataBlockIter> {
        // TODO: support block cache.
        let block = read_block_from_file(self.file.as_ref(), handle, self.global_seqno).await?;
        let iter = block.new_data_iterator(self.internal_comparator.clone());
        Ok(iter)
    }
}

pub struct BlockBasedTable {
    rep: Arc<BlockBasedTableRep>,
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
        let meta_block = read_block_from_file(
            file.as_ref(),
            &footer.metaindex_handle,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
        )
        .await?;
        let mut meta_iter =
            meta_block.new_data_iterator(Arc::new(DefaultUserComparator::default()));
        let mut global_seqno = DISABLE_GLOBAL_SEQUENCE_NUMBER;
        let mut index_key_includes_seq = true;
        let properties = if seek_to_properties_block(&mut meta_iter)? {
            let (properties, _handle) = read_properties(meta_iter.value(), file.as_ref()).await?;
            // TODO: checksum
            global_seqno = get_global_seqno(properties.as_ref(), opts.largest_seqno)?;
            index_key_includes_seq = properties.index_key_is_user_key == 0;
            Some(properties)
        } else {
            None
        };
        let index_reader = IndexReader::open(
            file.as_ref(),
            &footer.index_handle,
            global_seqno,
            index_key_includes_seq,
        )
        .await?;
        let mut key = if table_opts.filter_factory.is_block_based() {
            FILTER_BLOCK_PREFIX.to_string()
        } else {
            FULL_FILTER_BLOCK_PREFIX.to_string()
        };
        key.push_str(table_opts.filter_factory.name());
        let mut handle = BlockHandle::default();
        let filter_reader = if seek_to_metablock(&mut meta_iter, &key, Some(&mut handle))? {
            let block = read_block_content_from_file(file.as_ref(), &handle).await?;
            let filter_raeder = table_opts.filter_factory.create_filter_reader(block);
            Some(filter_raeder)
        } else {
            None
        };
        let table = BlockBasedTable {
            rep: Arc::new(BlockBasedTableRep {
                footer,
                file,
                properties,
                index_reader,
                global_seqno,
                internal_comparator: Arc::new(opts.internal_comparator.clone()),
                whole_key_filtering: table_opts.whole_key_filtering,
                filter_reader,
            }),
        };
        Ok(table)
    }

    fn full_filter_key_may_match(&self, key: &[u8]) -> bool {
        if let Some(filter) = self.rep.filter_reader.as_ref() {
            if self.rep.whole_key_filtering {
                let user_key = extract_user_key(key);
                return filter.key_may_match(user_key);
            } else {
                // TODO: finish prefix key match
            }
        }
        true
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
        .read_exact(read_offset, NEW_VERSIONS_ENCODED_LENGTH, &mut data)
        .await?;
    let mut footer = Footer::default();
    footer.decode_from(&data[..sz])?;
    assert_eq!(footer.table_magic_number, BLOCK_BASED_TABLE_MAGIC_NUMBER);
    Ok(footer)
}

fn get_global_seqno(propertoes: &TableProperties, _largest_seqno: u64) -> Result<u64> {
    if propertoes.version < 2 {
        return Ok(DISABLE_GLOBAL_SEQUENCE_NUMBER);
    }
    Ok(propertoes.global_seqno)
}

#[async_trait]
impl TableReader for BlockBasedTable {
    async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !opts.skip_filter {
            if !self.full_filter_key_may_match(key) {
                return Ok(None);
            }
        }
        let mut iter = self
            .rep
            .index_reader
            .new_iterator(self.rep.internal_comparator.clone());
        iter.seek(key);
        if iter.valid() {
            let mut biter = self
                .rep
                .new_data_block_iterator(&iter.index_value().handle)
                .await?;
            biter.seek(key);
            // TODO: support merge operator
            if biter.valid()
                && self
                    .rep
                    .internal_comparator
                    .get_user_comparator()
                    .compare_key(extract_user_key(biter.key()), extract_user_key(key))
                    == Ordering::Equal
            {
                return Ok(Some(biter.value().to_vec()));
            }
        }
        Ok(None)
    }

    fn new_iterator_opts(&self, _opts: &ReadOptions) -> Box<dyn AsyncIterator> {
        let index_iter = self
            .rep
            .index_reader
            .new_iterator(self.rep.internal_comparator.clone());
        let iter = BlockBasedTableIterator::new(
            self.rep.clone(),
            self.rep.internal_comparator.clone(),
            index_iter,
        );
        Box::new(iter)
    }
}
