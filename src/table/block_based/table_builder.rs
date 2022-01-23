use crate::common::format::{extract_value_type, BlockHandle, ValueType};
use crate::common::options::CompressionType;
use crate::common::FixedLengthSuffixComparator;
use crate::common::{Result, WritableFileWriter};
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
    file: WritableFileWriter,
    target_file_size: u64,

    offset: u64,
    alignment: usize,
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

    fn write_block(
        &mut self,
        block_builder: &mut BlockBuilder,
        handle: &mut BlockHandle,
        is_data_block: bool,
    ) -> Result<()> {
        let buf = block_builder.finish();
        self.write_raw_block(buf, handle, is_data_block)?;
        if is_data_block {
            if let Some(builder) = self.filter_builder.as_mut() {
                builder.start_block(self.offset);
            }
            self.props.data_size = self.offset;
            self.props.num_data_blocks += 1;
        }
        block_builder.clear();
        Ok(())
    }

    fn write_raw_block(
        &mut self,
        block: &[u8],
        handle: &mut BlockHandle,
        is_data_block: bool,
    ) -> Result<()> {
        handle.offset = self.offset;
        handle.size = block.len() as u64;
        self.file.append(block)?;
        let mut trailer: [u8; 5] = [0; 5];
        // todo: Add checksum for every block.
        trailer[0] = CompressionType::NoCompression as u8;
        trailer[1..].copy_from_slice(&(0 as u32).to_le_bytes());
        self.file.append(&trailer)?;
        self.offset += block.len() as u64 + trailer.len() as u64;
        if self.options.block_align && is_data_block {
            let pad_bytes = (self.alignment - ((block.len() + 5) & (self.alignment - 1)))
                & (self.alignment - 1);
            self.file.pad(pad_bytes)?;
            self.offset += pad_bytes as u64;
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
        self.props.raw_key_size += key.len() as u64;
        self.props.raw_value_size += value.len() as u64;
        if value_type == ValueType::kTypeDeletion as u8 {
            self.props.num_deletions += 1;
        } else if value_type == ValueType::kTypeMerge as u8 {
            self.props.num_merge_operands += 1;
        }
        Ok(())
    }

    fn finish(&mut self) -> crate::common::Result<()> {
        Ok(())
    }

    fn file_size(&self) -> u64 {
        self.offset
    }

    fn num_entries(&self) -> u64 {
        0
    }
}
