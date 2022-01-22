pub enum DataBlockIndexType {
    DataBlockBinarySearch,
    DataBlockBinaryAndHash,
}

pub enum IndexType {
    BinarySearch,
    HashSearch,
    TwoLevelIndexSearch,
    BinarySearchWithFirstKey,
}

pub enum IndexShorteningMode {
    // Use full keys.
    NoShortening,
    // Shorten index keys between blocks, but use full key for the last index
    // key, which is the upper bound of the whole file.
    ShortenSeparators,
    // Shorten both keys between blocks and key after last block.
    ShortenSeparatorsAndSuccessor,
}

pub struct BlockBasedTableOptions {
    pub block_size: usize,
    pub index_type: IndexType,
    pub data_block_index_type: DataBlockIndexType,
}
