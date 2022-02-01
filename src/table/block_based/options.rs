use crate::common::SliceTransform;
use crate::table::block_based::filter_block_builder::FilterBlockFactory;
use crate::table::block_based::FullFilterBlockFactory;
use crate::table::format::ChecksumType;
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
    // Not support
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
    pub checksum: ChecksumType,
    pub data_block_hash_table_util_ratio: f64,
    pub data_block_index_type: DataBlockIndexType,
    pub filter_factory: Arc<dyn FilterBlockFactory>,
    pub format_version: u32,
    pub prefix_extractor: Option<Arc<dyn SliceTransform>>,
    pub index_block_restart_interval: usize,
    pub index_shortening: IndexShorteningMode,
    pub index_type: IndexType,
    pub use_delta_encoding: bool,
    pub whole_key_filtering: bool,
}

impl Default for BlockBasedTableOptions {
    fn default() -> Self {
        Self {
            block_align: false,
            block_restart_interval: 16,
            block_size: 0,
            checksum: ChecksumType::NoChecksum,
            data_block_hash_table_util_ratio: 0.75,
            data_block_index_type: DataBlockIndexType::DataBlockBinarySearch,
            filter_factory: Arc::new(FullFilterBlockFactory::new(10)),
            format_version: 2,
            prefix_extractor: None,
            index_block_restart_interval: 1,
            index_shortening: IndexShorteningMode::NoShortening,
            index_type: IndexType::BinarySearch,
            use_delta_encoding: true,
            whole_key_filtering: false,
        }
    }
}
