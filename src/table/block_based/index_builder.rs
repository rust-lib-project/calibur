use super::options::IndexShorteningMode;
use crate::common::{InternalKeyComparator, KeyComparator, Result};
use crate::table::block_based::block_builder::{BlockBuilder, DEFAULT_HASH_TABLE_UTIL_RATIO};
use crate::table::block_based::options::{BlockBasedTableOptions, DataBlockIndexType, IndexType};
use crate::table::format::*;
pub use crate::util::extract_user_key;
use std::cmp::Ordering;

pub struct IndexBlocks {
    pub index_block_contents: Vec<u8>,
    // pub meta_blocks: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

pub trait IndexBuilder: Send {
    fn add_index_entry(
        &mut self,
        last_key_in_current_block: &mut Vec<u8>,
        first_key_in_next_block: &[u8],
        block_handle: &BlockHandle,
    );
    fn on_key_added(&mut self, key: &[u8]);
    fn finish(&mut self) -> Result<&[u8]>;
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
        block_handle: &BlockHandle,
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
        let entry = IndexValueRef::new(block_handle);
        let mut encoded_entry = vec![];
        entry.encode_to(&mut encoded_entry);
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

    fn finish(&mut self) -> Result<&[u8]> {
        let buf = if self.seperator_is_key_plus_seq {
            self.index_block_builder.finish()
        } else {
            self.index_block_builder_without_seq.finish()
        };
        self.index_size = buf.len();
        Ok(buf)
        // meta_blocks: HashMap::default(),
    }

    fn index_size(&self) -> usize {
        self.index_size
    }

    fn seperator_is_key_plus_seq(&self) -> bool {
        self.seperator_is_key_plus_seq
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        FileSystem, InMemFileSystem, InternalKeyComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER,
    };
    use crate::table::block_based::index_reader::IndexReader;
    use crate::table::InternalIterator;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[test]
    fn test_index_builder() {
        let mut builder = ShortenedIndexBuilder::new(
            InternalKeyComparator::default(),
            1,
            2,
            false,
            IndexShorteningMode::ShortenSeparators,
        );
        let mut kvs = vec![];
        kvs.push((b"abcdeeeeee".to_vec(), BlockHandle::new(100, 50)));
        kvs.push((b"abcdefffff".to_vec(), BlockHandle::new(150, 50)));
        kvs.push((b"abcdeggggg".to_vec(), BlockHandle::new(200, 50)));
        kvs.push((b"abcdehhhhh".to_vec(), BlockHandle::new(250, 50)));
        kvs.push((b"abcdeiiiii".to_vec(), BlockHandle::new(300, 50)));
        kvs.push((b"abcdejjjjj".to_vec(), BlockHandle::new(350, 50)));
        for (k, _) in kvs.iter_mut() {
            k.extend_from_slice(&0u64.to_le_bytes());
        }
        for (k, v) in kvs.iter() {
            builder.add_index_entry(&mut k.clone(), &[], v);
        }
        let data = builder.finish().unwrap().to_vec();
        let seperate = builder.seperator_is_key_plus_seq();
        let fs = InMemFileSystem::default();
        let mut f = fs
            .open_writable_file_writer(PathBuf::from("index_block".to_string()))
            .unwrap();
        let r = Runtime::new().unwrap();
        r.block_on(f.append(&data)).unwrap();
        let trailer: [u8; 5] = [0; 5];
        r.block_on(f.append(&trailer)).unwrap();
        r.block_on(f.sync()).unwrap();
        let readfile = fs
            .open_random_access_file(PathBuf::from("index_block"))
            .unwrap();
        let handle = BlockHandle::new(0, data.len() as u64);

        let f = IndexReader::open(
            readfile.as_ref(),
            &handle,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            seperate,
        );
        let reader = r.block_on(f).unwrap();
        let mut iter = reader.new_iterator(Arc::new(InternalKeyComparator::default()));
        iter.seek_to_first();
        for (k, v) in kvs {
            assert!(iter.valid());
            assert_eq!(iter.key(), k.as_slice());
            assert_eq!(iter.index_value().handle, v);
            iter.next();
        }
        let mut w = b"abcde".to_vec();
        w.extend_from_slice(&0u64.to_le_bytes());
        iter.seek(&w);
        assert!(iter.valid());
        assert_eq!(iter.index_value().handle.offset, 100);
    }
}
