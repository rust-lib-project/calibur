use crate::table::block_based::block_builder::BlockBuilder;
use crate::table::block_based::options::DataBlockIndexType;
use crate::table::format::{BlockHandle, Footer};
use crate::table::table_properties::*;
use crate::util::{encode_var_uint64, get_var_uint64};
use crate::common::{DefaultUserComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, RandomAccessFileReader};
use crate::common::Result;
use crate::table::block_based::block::Block;
use crate::table::block_based::BLOCK_TRAILER_SIZE;
use std::collections::HashMap;
use std::sync::Arc;
use crate::table::InternalIterator;

pub struct MetaIndexBuilder {
    meta_index_block: BlockBuilder,
    meta_block_handles: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MetaIndexBuilder {
    pub fn new() -> MetaIndexBuilder {
        MetaIndexBuilder {
            meta_block_handles: vec![],
            meta_index_block: BlockBuilder::new(
                1usize,
                true,
                DataBlockIndexType::DataBlockBinarySearch,
                0.75,
            ),
        }
    }

    pub fn add(&mut self, key: &[u8], handle: &BlockHandle) {
        let mut handle_encoding = Vec::with_capacity(20);
        handle.encode_to(&mut handle_encoding);
        self.meta_block_handles
            .push((key.to_vec(), handle_encoding));
    }

    pub fn finish(&mut self) -> &[u8] {
        self.meta_block_handles.sort_by(|x, y| x.0.cmp(&y.0));
        for (k, v) in self.meta_block_handles.iter() {
            self.meta_index_block.add(k.as_slice(), v.as_slice());
        }
        self.meta_index_block.finish()
    }
}

pub struct PropertyBlockBuilder {
    properties_block: BlockBuilder,
    props: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PropertyBlockBuilder {
    pub fn new() -> PropertyBlockBuilder {
        PropertyBlockBuilder {
            props: vec![],
            properties_block: BlockBuilder::new(
                u32::MAX as usize,
                true,
                DataBlockIndexType::DataBlockBinarySearch,
                0.75,
            ),
        }
    }

    pub fn add_u64(&mut self, key: &[u8], val: u64) {
        let mut tmp: [u8; 10] = [0u8; 10];
        encode_var_uint64(&mut tmp, val);
        self.add_key(key, &tmp);
    }

    pub fn add_key(&mut self, key: &[u8], val: &[u8]) {
        self.props.push((key.to_vec(), val.to_vec()));
    }

    pub fn add_properties(&mut self, user_collected_properties: &HashMap<String, Vec<u8>>) {
        for (k, v) in user_collected_properties {
            self.props.push((k.clone().into_bytes(), v.clone()));
        }
    }

    pub fn add_table_properties(&mut self, props: &TableProperties) {
        self.add_u64(PROPERTIES_RAW_KEY_SIZE.as_bytes(), props.raw_key_size);
        self.add_u64(PROPERTIES_RAW_VALUE_SIZE.as_bytes(), props.raw_value_size);
        self.add_u64(PROPERTIES_DATA_SIZE.as_bytes(), props.data_size);
        self.add_u64(PROPERTIES_INDEX_SIZE.as_bytes(), props.index_size);
        // if props.index_partitions != 0 {
        //     self.add_u64(PROPERTIES_IndexPartitions, props.index_partitions);
        //     self.add_u64(PROPERTIES_TopLevelIndexSize, props.top_level_index_size);
        // }
        self.add_u64(
            PROPERTIES_INDEX_KEY_IS_USER_KEY.as_bytes(),
            props.index_key_is_user_key,
        );
        self.add_u64(
            PROPERTIES_INDEX_VALUE_IS_DELTA_ENCODED.as_bytes(),
            props.index_value_is_delta_encoded,
        );
        self.add_u64(PROPERTIES_NUM_ENTRIES.as_bytes(), props.num_entries);
        self.add_u64(PROPERTIES_DELETED_KEYS.as_bytes(), props.num_deletions);
        self.add_u64(
            PROPERTIES_MERGE_OPERANDS.as_bytes(),
            props.num_merge_operands,
        );
        self.add_u64(
            PROPERTIES_NUM_RANGE_DELETIONS.as_bytes(),
            props.num_range_deletions,
        );
        self.add_u64(PROPERTIES_NUM_DATA_BLOCKS.as_bytes(), props.num_data_blocks);
        self.add_u64(PROPERTIES_FILTER_SIZE.as_bytes(), props.filter_size);
        self.add_u64(PROPERTIES_FORMAT_VERSION.as_bytes(), props.format_version);
        self.add_u64(PROPERTIES_FIXED_KEY_LEN.as_bytes(), props.fixed_key_len);
        self.add_u64(
            PROPERTIES_COLUMN_FAMILY_ID.as_bytes(),
            props.column_family_id as u64,
        );
        // self.add_u64(PROPERTIES_CreationTime, props.creation_time);
        // self.add_u64(PROPERTIES_OldestKeyTime, props.oldest_key_time);
        // if props.file_creation_time > 0 {
        //     Add(PROPERTIES_FileCreationTime, props.file_creation_time);
        // }

        if !props.filter_policy_name.is_empty() {
            self.add_key(
                PROPERTIES_FILTER_POLICY.as_bytes(),
                props.filter_policy_name.as_bytes(),
            );
        }
        if !props.comparator_name.is_empty() {
            self.add_key(
                PROPERTIES_COMPARATOR.as_bytes(),
                props.comparator_name.as_bytes(),
            );
        }

        // if (!props.merge_operator_name.empty()) {
        //     Add(TablePropertiesNames::kMergeOperator, props.merge_operator_name);
        // }
        if !props.prefix_extractor_name.is_empty() {
            self.add_key(
                PROPERTIES_PREFIX_EXTRACTOR_NAME.as_bytes(),
                props.prefix_extractor_name.as_bytes(),
            );
        }
        // if (!props.property_collectors_names.empty()) {
        //     Add(TablePropertiesNames::kPropertyCollectors,
        //         props.property_collectors_names);
        // }
        if !props.column_family_name.is_empty() {
            self.add_key(
                PROPERTIES_COLUMN_FAMILY_NAME.as_bytes(),
                props.column_family_name.as_bytes(),
            );
        }

        if !props.compression_name.is_empty() {
            self.add_key(
                PROPERTIES_COMPRESSION.as_bytes(),
                props.compression_name.as_bytes(),
            );
        }
        // if (!props.compression_options.empty()) {
        //     Add(TablePropertiesNames::kCompressionOptions, props.compression_options);
        // }
    }

    pub fn finish(&mut self) -> &[u8] {
        self.props.sort_by(|x, y| x.0.cmp(&y.0));
        for (k, v) in self.props.iter() {
            self.properties_block.add(k.as_slice(), v.as_slice());
        }
        self.properties_block.finish()
    }
}

pub async fn read_properties(data: &[u8], file: &RandomAccessFileReader, footer: &Footer) -> Result<(Box<TableProperties>, BlockHandle)> {
    let mut handle = BlockHandle::default();
    handle.decode_from(data)?;
    let read_len = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut data = vec![0u8; read_len];
    file.read(handle.offset as usize, read_len, data.as_mut_slice())
        .await?;
    // TODO: uncompress block
    let block = Block::new(data, DISABLE_GLOBAL_SEQUENCE_NUMBER);
    let mut iter = block.new_data_iterator(Arc::new(DefaultUserComparator::default()));
    iter.seek_to_first();
    let mut properties = Box::new(TableProperties::default());
    let mut tmp_properties = HashMap::new();
    while iter.valid() {
        if let Ok(key) = std::str::from_utf8(iter.key()) {
            tmp_properties.insert(key.to_string(), iter.value().to_vec());
        }
        iter.next();
    }
    tmp_properties.get(PROPERTIES_DATA_SIZE).map(|v| {
       if let Some((_, v)) = get_var_uint64(v) {
           properties.data_size = v;
       }
    });
    tmp_properties.get(PROPERTIES_INDEX_SIZE).map(|v| {
        if let Some((_, v)) = get_var_uint64(v) {
            properties.index_size = v;
        }
    });
    tmp_properties.get(PROPERTIES_NUM_ENTRIES).map(|v| {
        if let Some((_, v)) = get_var_uint64(v) {
            properties.num_entries = v;
        }
    });
    tmp_properties.get(PROPERTIES_DELETED_KEYS).map(|v| {
        if let Some((_, v)) = get_var_uint64(v) {
            properties.num_deletions = v;
        }
    });
    // TODO: finish all properties
    Ok((properties, handle))
}
