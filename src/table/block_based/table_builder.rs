use crate::common::format::{extract_value_type, ValueType};
use crate::common::options::CompressionType;
use crate::common::InternalKeyComparator;
use crate::common::{Result, WritableFileWriter};
use crate::table::block_based::block_builder::BlockBuilder;
use crate::table::block_based::filter_block_builder::FilterBlockBuilder;
use crate::table::block_based::index_builder::{create_index_builder, IndexBuilder};
use crate::table::block_based::meta_block::{MetaIndexBuilder, PropertyBlockBuilder};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::block_based::options::DataBlockIndexType;
use crate::table::block_based::{FILTER_BLOCK_PREFIX, FULL_FILTER_BLOCK_PREFIX};
use crate::table::format::*;
use crate::table::table_properties::{TableProperties, PROPERTIES_BLOCK};
use crate::table::{TableBuilder, TableBuilderOptions};
use crate::util::extract_user_key;

// const PartitionedFilterBlockPrefix: &str = "partitionedfilter.";

pub struct BuilderRep {
    offset: u64,
    last_key: Vec<u8>,
    pending_handle: BlockHandle,
    props: TableProperties,
    file: Box<WritableFileWriter>,
    alignment: usize,
    options: BlockBasedTableOptions,
    column_family_name: String,
    column_family_id: u32,
}

impl BuilderRep {
    async fn write_raw_block(&mut self, block: &[u8], is_data_block: bool) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            size: block.len() as u64,
        };
        self.file.append(block).await?;
        let mut trailer: [u8; 5] = [0; 5];
        trailer[0] = CompressionType::NoCompression as u8;
        // todo: Add checksum for every block.
        trailer[1..].copy_from_slice(&(0 as u32).to_le_bytes());
        self.file.append(&trailer).await?;
        self.offset += block.len() as u64 + trailer.len() as u64;
        if self.options.block_align && is_data_block {
            let pad_bytes = (self.alignment - ((block.len() + 5) & (self.alignment - 1)))
                & (self.alignment - 1);
            self.file.pad(pad_bytes).await?;
            self.offset += pad_bytes as u64;
        }
        Ok(handle)
    }
}

pub struct BlockBasedTableBuilder {
    comparator: InternalKeyComparator,
    data_block_builder: BlockBuilder,
    index_builder: Box<dyn IndexBuilder>,
    filter_builder: Option<Box<dyn FilterBlockBuilder>>,
    after_flush: bool,
    rep: BuilderRep,
}

impl BlockBasedTableBuilder {
    pub fn new(
        options: &TableBuilderOptions,
        table_options: BlockBasedTableOptions,
        file: Box<WritableFileWriter>,
    ) -> Self {
        let data_block_builder = BlockBuilder::new(
            table_options.block_restart_interval,
            table_options.use_delta_encoding,
            DataBlockIndexType::DataBlockBinarySearch,
            table_options.data_block_hash_table_util_ratio,
        );

        let index_builder = create_index_builder(
            table_options.index_type,
            options.internal_comparator.clone(),
            &table_options,
        );
        let rep = BuilderRep {
            offset: 0,
            last_key: vec![],
            pending_handle: Default::default(),
            props: Default::default(),
            file,
            alignment: 0,
            options: table_options,
            column_family_name: options.column_family_name.clone(),
            column_family_id: options.column_family_id,
        };
        let filter_builder = if options.skip_filter {
            None
        } else {
            Some(rep.options.filter_factory.create_builder(&rep.options))
        };
        BlockBasedTableBuilder {
            comparator: options.internal_comparator.clone(),
            data_block_builder,
            index_builder,
            filter_builder,
            after_flush: false,
            rep,
        }
    }

    async fn flush_data_block(&mut self) -> Result<BlockHandle> {
        let buf = self.data_block_builder.finish();
        let handle = self.rep.write_raw_block(buf, true).await?;
        if let Some(builder) = self.filter_builder.as_mut() {
            builder.start_block(self.rep.offset);
        }
        self.rep.props.data_size = self.rep.offset;
        self.rep.props.num_data_blocks += 1;
        self.data_block_builder.clear();
        Ok(handle)
    }

    async fn write_index_block(&mut self) -> Result<BlockHandle> {
        let index_blocks = self.index_builder.finish()?;
        // TODO: build index block for hash index table.
        self.rep.write_raw_block(index_blocks, false).await
    }

    async fn write_filter_block(
        &mut self,
        meta_index_builder: &mut MetaIndexBuilder,
    ) -> Result<()> {
        if let Some(builder) = self.filter_builder.as_mut() {
            if builder.num_added() == 0 {
                return Ok(());
            }
            let content = builder.finish()?;
            self.rep.props.filter_size += content.len() as u64;
            let handle = self.rep.write_raw_block(content, false).await?;
            let mut key = if builder.is_block_based() {
                FILTER_BLOCK_PREFIX.to_string()
            } else {
                FULL_FILTER_BLOCK_PREFIX.to_string()
            };
            key.push_str(self.rep.options.filter_factory.name());
            meta_index_builder.add(key.as_bytes(), &handle);
        }
        Ok(())
    }

    async fn write_properties_block(
        &mut self,
        meta_index_builder: &mut MetaIndexBuilder,
    ) -> Result<()> {
        let mut property_block_builder = PropertyBlockBuilder::new();
        self.rep.props.column_family_id = self.rep.column_family_id;
        self.rep.props.filter_policy_name = self.rep.options.filter_factory.name().to_string();
        self.rep.props.index_size = self.index_builder.index_size() as u64 + 5;
        if self.index_builder.seperator_is_key_plus_seq() {
            self.rep.props.index_key_is_user_key = 0;
        } else {
            self.rep.props.index_key_is_user_key = 1;
        }
        // TODO: add the whole properties
        property_block_builder.add_table_properties(&self.rep.props);
        let data = property_block_builder.finish();
        let properties_block_handle = self.rep.write_raw_block(data, false).await?;
        meta_index_builder.add(PROPERTIES_BLOCK.as_bytes(), &properties_block_handle);
        Ok(())
    }

    async fn write_footer(
        &mut self,
        metaindex_block_handle: BlockHandle,
        index_block_handle: BlockHandle,
    ) -> Result<()> {
        let legacy = self.rep.options.format_version == 0;
        let magic_number = if legacy {
            LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER
        } else {
            BLOCK_BASED_TABLE_MAGIC_NUMBER
        };
        let footer = Footer {
            version: self.rep.options.format_version,
            checksum: self.rep.options.checksum as u8,
            metaindex_handle: metaindex_block_handle,
            index_handle: index_block_handle,
            table_magic_number: magic_number,
        };
        let mut buf = vec![];
        footer.encode_to(&mut buf);
        self.rep.file.append(&buf).await?;
        self.rep.offset += buf.len() as u64;
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableBuilder for BlockBasedTableBuilder {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let value_type = extract_value_type(key);
        // TODO: check out of order
        if self.after_flush {
            self.index_builder.add_index_entry(
                &mut self.rep.last_key,
                key,
                &self.rep.pending_handle,
            );
            self.after_flush = false;
        }
        if let Some(builder) = self.filter_builder.as_mut() {
            builder.add(extract_user_key(key));
        }
        // TODO: add key to filter block builder
        self.rep.last_key = key.to_vec();
        self.data_block_builder.add(key, value);
        self.index_builder.on_key_added(key);
        self.rep.props.num_entries += 1;
        self.rep.props.raw_key_size += key.len() as u64;
        self.rep.props.raw_value_size += value.len() as u64;
        if value_type == ValueType::TypeDeletion as u8 {
            self.rep.props.num_deletions += 1;
        } else if value_type == ValueType::TypeMerge as u8 {
            self.rep.props.num_merge_operands += 1;
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.data_block_builder.current_size_estimate() >= self.rep.options.block_size
    }

    async fn finish(&mut self) -> crate::common::Result<()> {
        self.flush().await?;
        if self.after_flush {
            self.index_builder.add_index_entry(
                &mut self.rep.last_key,
                &[],
                &self.rep.pending_handle,
            );
        }
        // Write meta blocks, metaindex block and footer in the following order.
        //    1. [meta block: filter]
        //    2. [meta block: index]
        //    3. [meta block: compression dictionary]
        //    4. [meta block: range deletion tombstone]
        //    5. [meta block: properties]
        //    6. [metaindex block]
        //    7. Footer
        let mut meta_index_builder = MetaIndexBuilder::new();
        self.write_filter_block(&mut meta_index_builder).await?;
        let index_block_handle = self.write_index_block().await?;
        self.write_properties_block(&mut meta_index_builder).await?;
        let metaindex_block_handle = self
            .rep
            .write_raw_block(meta_index_builder.finish(), false)
            .await?;
        self.write_footer(metaindex_block_handle, index_block_handle)
            .await?;
        self.rep.file.sync().await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }
        self.rep.pending_handle = self.flush_data_block().await?;
        self.after_flush = true;
        Ok(())
    }

    fn file_size(&self) -> u64 {
        self.rep.offset
    }

    fn num_entries(&self) -> u64 {
        self.rep.props.num_entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{FileSystem, InMemFileSystem};
    use crate::table::block_based::table_reader::BlockBasedTable;
    use crate::table::{TableReader, TableReaderOptions};
    use crate::util::next_key;
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    #[test]
    fn test_table_builder() {
        let mut opts = BlockBasedTableOptions::default();
        opts.block_size = 128;
        let fs = InMemFileSystem::default();
        let w = fs.open_writable_file_writer(PathBuf::from("sst0")).unwrap();
        let tbl_opts = TableBuilderOptions::default();
        let mut builder = BlockBasedTableBuilder::new(&tbl_opts, opts.clone(), w);
        let mut key = b"abcdef".to_vec();
        let mut kvs = vec![];
        let runtime = Runtime::new().unwrap();
        for _ in 0..1000u64 {
            next_key(&mut key);
            let mut k1 = key.clone();
            k1.extend_from_slice(&200u64.to_le_bytes());
            builder.add(&k1, b"v0").unwrap();
            kvs.push((k1.clone(), b"v0".to_vec()));
            k1.resize(key.len(), 0);

            k1.extend_from_slice(&100u64.to_le_bytes());
            builder.add(&k1, b"v1").unwrap();
            kvs.push((k1, b"v1".to_vec()));
            if builder.should_flush() {
                runtime.block_on(builder.flush()).unwrap();
            }
        }
        runtime.block_on(builder.finish()).unwrap();
        assert_eq!(builder.num_entries(), 2000);
        let r = fs.open_random_access_file(PathBuf::from("sst0")).unwrap();
        let mut tbl_opts = TableReaderOptions::default();
        tbl_opts.file_size = r.file_size();
        let reader = runtime
            .block_on(BlockBasedTable::open(&tbl_opts, opts, r))
            .unwrap();
        let mut iter = reader.new_iterator();
        runtime.block_on(iter.seek_to_first());
        let mut ret = vec![];
        while iter.valid() {
            ret.push((iter.key().to_vec(), iter.value().to_vec()));
            runtime.block_on(iter.next());
        }
        for i in 0..ret.len() {
            assert_eq!(ret[i], kvs[i]);
        }
        assert_eq!(ret, kvs);
    }
}
