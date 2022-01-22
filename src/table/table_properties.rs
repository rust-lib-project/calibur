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
    pub column_family_id: u64,
    pub creation_time: u64,
    // Timestamp of the earliest key. 0 means unknown.
    pub oldest_key_time: u64,
    // Actual SST file creation time. 0 means unknown.
    pub file_creation_time: u64,

    // Name of the column family with which this SST file is associated.
    // If column family is unknown, `column_family_name` will be an empty string.
    column_family_name: String,

    // The name of the filter policy used in this table.
    // If no filter policy is used, `filter_policy_name` will be an empty string.
    pub filter_policy_name: String,

    // The name of the comparator used in this table.
    pub comparator_name: String,
    // The name of the merge operator used in this table.
    // If no merge operator is used, `merge_operator_name` will be "nullptr".
    pub merge_operator_name: String,
}
