use crate::common::SliceTransform;
use crate::table::block_based::filter_block_builder::FilterBuilderFactory;
use std::sync::Arc;

#[derive(Eq, PartialEq, Clone)]
pub enum DataBlockIndexType {
    DataBlockBinarySearch,
    DataBlockBinaryAndHash,
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum IndexType {
    BinarySearch,
    HashSearch,
    TwoLevelIndexSearch,
    BinarySearchWithFirstKey,
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum IndexShorteningMode {
    // Use full keys.
    NoShortening,
    // Shorten index keys between blocks, but use full key for the last index
    // key, which is the upper bound of the whole file.
    ShortenSeparators,
    // Shorten both keys between blocks and key after last block.
    ShortenSeparatorsAndSuccessor,
}

#[derive(Clone)]
pub struct BlockBasedTableOptions {
    pub block_align: bool,
    pub block_restart_interval: usize,
    pub block_size: usize,
    pub data_block_hash_table_util_ratio: f64,
    pub data_block_index_type: DataBlockIndexType,
    pub filter_factory: Arc<dyn FilterBuilderFactory>,
    pub prefix_extractor: Option<Arc<dyn SliceTransform>>,
    pub format_version: u32,
    pub index_block_restart_interval: usize,
    pub index_shortening: IndexShorteningMode,
    pub index_type: IndexType,
    pub use_delta_encoding: bool,
    pub whole_key_filtering: bool,
}
