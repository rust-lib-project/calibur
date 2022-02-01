use crate::common::Result;
use crate::table::format::BlockHandle;
use crate::table::InternalIterator;

#[derive(Default, Debug, Clone)]
pub struct TableProperties {
    pub data_size: u64,
    // the size of index block.
    pub index_size: u64,
    // Total number of index partitions if kTwoLevelIndexSearch is used
    pub index_partitions: u64,
    // Size of the top-level index if kTwoLevelIndexSearch is used
    pub top_level_index_size: u64,
    // Whether the index key is user key. Otherwise it includes 8 byte of sequence
    // number added by internal key format.
    pub index_key_is_user_key: u64,
    // Whether delta encoding is used to encode the index values.
    pub index_value_is_delta_encoded: u64,
    // the size of filter block.
    pub filter_size: u64,
    // total raw key size
    pub raw_key_size: u64,
    // total raw value size
    pub raw_value_size: u64,
    // the number of blocks in this table
    pub num_data_blocks: u64,
    pub num_entries: u64,
    // the number of deletions in the table
    pub num_deletions: u64,
    // the number of merge operands in the table
    pub num_merge_operands: u64,
    // the number of range deletions in this table
    pub num_range_deletions: u64,
    // format version, reserved for backward compatibility
    pub format_version: u64,
    // If 0, key is variable length. Otherwise number of bytes for each key.
    pub fixed_key_len: u64,
    // ID of column family for this SST file, corresponding to the CF identified
    // by column_family_name.
    pub column_family_id: u32,
    pub creation_time: u64,
    // Timestamp of the earliest key. 0 means unknown.
    pub oldest_key_time: u64,
    // Actual SST file creation time. 0 means unknown.
    pub file_creation_time: u64,

    // Name of the column family with which this SST file is associated.
    // If column family is unknown, `column_family_name` will be an empty string.
    pub(crate) column_family_name: String,

    // The name of the filter policy used in this table.
    // If no filter policy is used, `filter_policy_name` will be an empty string.
    pub filter_policy_name: String,

    // The name of the comparator used in this table.
    pub comparator_name: String,
    // The name of the merge operator used in this table.
    // If no merge operator is used, `merge_operator_name` will be "nullptr".
    pub merge_operator_name: String,

    pub prefix_extractor_name: String,

    pub compression_name: String,
    pub version: u32,
    pub global_seqno: u64,
}

pub const PROPERTIES_BLOCK: &str = "rocksdb.properties";
// Old property block name for backward compatibility
pub const PROPERTIES_BLOCK_OLD_NAME: &str = "rocksdb.stats";
pub const COMPRESSION_DICT_BLOCK: &str = "rocksdb.compression_dict";
pub const PROPERTIES_DATA_SIZE: &str = "rocksdb.data.size";
pub const PROPERTIES_INDEX_SIZE: &str = "rocksdb.index.size";
pub const PROPERTIES_INDEX_PARTITIONS: &str = "rocksdb.index.partitions";
pub const PROPERTIES_TOP_LEVEL_INDEX_SIZE: &str = "rocksdb.top-level.index.size";
pub const PROPERTIES_INDEX_KEY_IS_USER_KEY: &str = "rocksdb.index.key.is.user.key";
pub const PROPERTIES_INDEX_VALUE_IS_DELTA_ENCODED: &str = "rocksdb.index.value.is.delta.encoded";
pub const PROPERTIES_FILTER_SIZE: &str = "rocksdb.filter.size";
pub const PROPERTIES_RAW_KEY_SIZE: &str = "rocksdb.raw.key.size";
pub const PROPERTIES_RAW_VALUE_SIZE: &str = "rocksdb.raw.value.size";
pub const PROPERTIES_NUM_DATA_BLOCKS: &str = "rocksdb.num.data.blocks";
pub const PROPERTIES_NUM_ENTRIES: &str = "rocksdb.num.entries";
pub const PROPERTIES_DELETED_KEYS: &str = "rocksdb.deleted.keys";
pub const PROPERTIES_MERGE_OPERANDS: &str = "rocksdb.merge.operands";
pub const PROPERTIES_NUM_RANGE_DELETIONS: &str = "rocksdb.num.range-deletions";
pub const PROPERTIES_FILTER_POLICY: &str = "rocksdb.filter.policy";
pub const PROPERTIES_FORMAT_VERSION: &str = "rocksdb.format.version";
pub const PROPERTIES_FIXED_KEY_LEN: &str = "rocksdb.fixed.key.length";
pub const PROPERTIES_COLUMN_FAMILY_ID: &str = "rocksdb.column.family.id";
pub const PROPERTIES_COLUMN_FAMILY_NAME: &str = "rocksdb.column.family.name";
pub const PROPERTIES_COMPARATOR: &str = "rocksdb.comparator";
pub const PROPERTIES_MERGE_OPERATOR: &str = "rocksdb.merge.operator";
pub const PROPERTIES_PREFIX_EXTRACTOR_NAME: &str = "rocksdb.prefix.extractor.name";
pub const PROPERTIES_PROPERTY_COLLECTORS: &str = "rocksdb.property.collectors";
pub const PROPERTIES_COMPRESSION: &str = "rocksdb.compression";
pub const PROPERTIES_COMPRESSION_OPTIONS: &str = "rocksdb.compression_options";
pub const PROPERTIES_CREATION_TIME: &str = "rocksdb.creation.time";
pub const PROPERTIES_OLDEST_KEY_TIME: &str = "rocksdb.oldest.key.time";
pub const PROPERTIES_FILE_CREATION_TIME: &str = "rocksdb.file.creation.time";

// value of this property is a fixed uint32 number.
pub const PROPERTIES_VERSION: &str = "rocksdb.external_sst_file.version";
// value of this property is a fixed uint64 number.
pub const PROPERTIES_GLOBAL_SEQNO: &str = "rocksdb.external_sst_file.global_seqno";

pub fn seek_to_metablock<I: InternalIterator>(
    meta_iter: &mut I,
    block_name: &str,
    block_handle: Option<&mut BlockHandle>,
) -> Result<bool> {
    meta_iter.seek(block_name.as_bytes());
    if meta_iter.valid() {
        if meta_iter.key().eq(block_name.as_bytes()) {
            if let Some(handle) = block_handle {
                handle.decode_from(meta_iter.value())?;
            }
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn seek_to_properties_block<I: InternalIterator>(meta_iter: &mut I) -> Result<bool> {
    if !seek_to_metablock(meta_iter, PROPERTIES_BLOCK, None)? {
        return seek_to_metablock(meta_iter, PROPERTIES_BLOCK_OLD_NAME, None);
    }
    Ok(true)
}

// pub struct UserCollectedProperties{
//     inner: BTreeMap<String, Vec<u8>>,
// }
//
// impl UserCollectedProperties {
//     pub fn get_ref(&self, key: &str) -> Option<&Vec<u8>> {
//         self.inner.get(key)
//     }
//     pub fn get(&self, key: &str) -> Option<Vec<u8>> {
//         self.inner.get(key).map(|v|v.clone())
//     }
//     pub fn insert(&mut self, key: &str, value: Vec<u8>) {
//         self.inner.insert(key.to_string(), value);
//     }
// }
