use super::options::IndexShorteningMode;
use crate::common::{InternalKeyComparator, KeyComparator, Result};
use crate::table::block_based::block_builder::{BlockBuilder, DEFAULT_HASH_TABLE_UTIL_RATIO};
use crate::table::block_based::options::{BlockBasedTableOptions, DataBlockIndexType, IndexType};
use crate::table::format::*;
pub use crate::util::extract_user_key;
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct IndexBlocks {
    pub index_block_contents: Vec<u8>,
    // pub meta_blocks: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

pub trait IndexBuilder {
    fn add_index_entry(
        &mut self,
        last_key_in_current_block: &mut Vec<u8>,
        first_key_in_next_block: &[u8],
        block_handle: BlockHandle,
    );
    fn on_key_added(&mut self, key: &[u8]);
    fn finish(&mut self) -> Result<IndexBlocks>;
    fn index_size(&self) -> usize;
    fn seperator_is_key_plus_seq(&self) -> bool {
        true
    }
}

pub struct ShortenedIndexBuilder {
    index_block_builder: BlockBuilder,
    index_block_builder_without_seq: BlockBuilder,
    include_first_key: bool,
    shortening_mode: IndexShorteningMode,
    last_encoded_handle: BlockHandle,
    current_block_first_internal_key: Vec<u8>,
    comparator: InternalKeyComparator,
    seperator_is_key_plus_seq: bool,
    index_size: usize,
}

impl ShortenedIndexBuilder {
    fn new(
        comparator: InternalKeyComparator,
        index_block_restart_interval: usize,
        format_version: u32,
        include_first_key: bool,
        shortening_mode: IndexShorteningMode,
    ) -> Self {
        let index_block_builder = BlockBuilder::new(
            index_block_restart_interval,
            true,
            DataBlockIndexType::DataBlockBinarySearch,
            DEFAULT_HASH_TABLE_UTIL_RATIO,
        );
        let index_block_builder_without_seq = BlockBuilder::new(
            index_block_restart_interval,
            true,
            DataBlockIndexType::DataBlockBinarySearch,
            DEFAULT_HASH_TABLE_UTIL_RATIO,
        );
        ShortenedIndexBuilder {
            index_block_builder,
            index_block_builder_without_seq,
            include_first_key,
            shortening_mode,
            last_encoded_handle: Default::default(),
            current_block_first_internal_key: vec![],
            comparator,
            seperator_is_key_plus_seq: format_version <= 2,
            index_size: 0,
        }
    }
}

impl IndexBuilder for ShortenedIndexBuilder {
    fn add_index_entry(
        &mut self,
        last_key_in_current_block: &mut Vec<u8>,
        first_key_in_next_block: &[u8],
        block_handle: BlockHandle,
    ) {
        if !first_key_in_next_block.is_empty() {
            if self.shortening_mode != IndexShorteningMode::NoShortening {
                self.comparator
                    .find_shortest_separator(last_key_in_current_block, first_key_in_next_block);
            }
            if !self.seperator_is_key_plus_seq
                && self.comparator.get_user_comparator().compare_key(
                    extract_user_key(last_key_in_current_block.as_slice()),
                    extract_user_key(first_key_in_next_block),
                ) == Ordering::Equal
            {
                self.seperator_is_key_plus_seq = true;
            }
        } else {
            if self.shortening_mode == IndexShorteningMode::ShortenSeparatorsAndSuccessor {
                self.comparator
                    .find_short_successor(last_key_in_current_block);
            }
        }
        let sep = last_key_in_current_block.as_slice();
        let entry =
            IndexValueRef::new(block_handle.clone(), &self.current_block_first_internal_key);
        let mut encoded_entry = vec![];
        entry.encode_to(&mut encoded_entry, self.include_first_key);
        self.last_encoded_handle = block_handle;
        self.index_block_builder.add(sep, &encoded_entry);
        if !self.seperator_is_key_plus_seq {
            self.index_block_builder_without_seq
                .add(extract_user_key(sep), &encoded_entry);
        }
        self.current_block_first_internal_key.clear();
    }

    fn on_key_added(&mut self, key: &[u8]) {
        if self.include_first_key && self.current_block_first_internal_key.is_empty() {
            self.current_block_first_internal_key = key.to_vec();
        }
    }

    fn finish(&mut self) -> Result<IndexBlocks> {
        let buf = if self.seperator_is_key_plus_seq {
            self.index_block_builder.finish()
        } else {
            self.index_block_builder_without_seq.finish()
        };
        self.index_size = buf.len();
        Ok(IndexBlocks {
            index_block_contents: buf.to_vec(),
            // meta_blocks: HashMap::default(),
        })
    }

    fn index_size(&self) -> usize {
        self.index_size
    }
}

pub fn create_index_builder(
    _: IndexType,
    comparator: InternalKeyComparator,
    opts: &BlockBasedTableOptions,
) -> Box<dyn IndexBuilder> {
    let builder = ShortenedIndexBuilder::new(
        comparator,
        opts.index_block_restart_interval,
        opts.format_version,
        false,
        opts.index_shortening,
    );
    Box::new(builder)
}
