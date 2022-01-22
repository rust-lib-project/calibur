use crate::common::format::{
    extract_value_type, is_value_type, kTypeDeletion, kTypeMerge, BlockHandle,
};
use crate::common::Result;
use crate::common::{extract_user_key, FixedLengthSuffixComparator};
use crate::table::block_based::block_builder::BlockBuilder;
use crate::table::block_based::filter_block_builder::FilterBlockBuilder;
use crate::table::block_based::index_builder::IndexBuilder;
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::table_properties::TableProperties;
use crate::table::TableBuilder;

pub struct BlockBasedTableBuilder {
    comparator: FixedLengthSuffixComparator,
    props: TableProperties,
    data_block_builder: BlockBuilder,
    index_builder: Box<dyn IndexBuilder>,
    filter_builder: Option<FilterBlockBuilder>,
    options: BlockBasedTableOptions,
    target_file_size: u64,

    data_begin_offset: u64,
    last_key: Vec<u8>,
    pending_handle: BlockHandle,
}

impl BlockBasedTableBuilder {
    fn should_flush(&self) -> bool {
        self.data_block_builder.current_size_estimate() >= self.options.block_size
    }

    fn flush(&mut self) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }
        Ok(())
    }
}

impl TableBuilder for BlockBasedTableBuilder {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let value_type = extract_value_type(key);
        // TODO: check out of order
        let should_flush = self.should_flush();
        if should_flush {
            self.flush()?;
            self.index_builder
                .add_index_entry(&mut self.last_key, key, self.pending_handle);
        }
        // TODO: add key to filter block builder
        self.last_key = key.to_vec();
        self.data_block_builder.add(key, value);
        self.index_builder.on_key_added(key);
        self.props.num_entries += 1;
        self.props.raw_key_size += key.len();
        self.props.raw_value_size += value.len();
        if value_type == kTypeDeletion {
            self.props.num_deletions += 1;
        } else if value_type == kTypeMerge {
            self.props.num_merge_operands += 1;
        }

        Ok(())
    }

    fn finish(&mut self) -> crate::common::Result<()> {
        Ok(())
    }

    fn file_size(&self) -> u64 {}

    fn num_entries(&self) -> u64 {}
}
