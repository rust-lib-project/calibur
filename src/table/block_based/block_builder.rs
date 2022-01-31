use super::data_block_hash_index_builder::DataBlockHashIndexBuilder;
use super::options::DataBlockIndexType;
use crate::table::block_based::block::pack_index_type_and_num_restarts;
use crate::table::format::MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX;
use crate::util::{difference_offset, encode_var_uint32, extract_user_key};

pub const DEFAULT_HASH_TABLE_UTIL_RATIO: f64 = 0.75;

// TODO: support encode delta value encoding
pub struct BlockBuilder {
    buff: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    count: usize,
    block_restart_interval: usize,
    use_delta_encoding: bool,
    hash_index_builder: DataBlockHashIndexBuilder,
    estimate: usize,
}

impl BlockBuilder {
    pub fn new(
        block_restart_interval: usize,
        use_delta_encoding: bool,
        index_type: DataBlockIndexType,
        data_block_hash_table_util_ratio: f64,
    ) -> BlockBuilder {
        let mut hash_index_builder = DataBlockHashIndexBuilder::default();
        if index_type == DataBlockIndexType::DataBlockBinaryAndHash {
            hash_index_builder.init(data_block_hash_table_util_ratio);
        }
        BlockBuilder {
            buff: vec![],
            block_restart_interval,
            use_delta_encoding,
            hash_index_builder,
            restarts: vec![0],
            estimate: std::mem::size_of::<u32>() * 2,
            count: 0,
            last_key: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buff.is_empty()
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0;
        if self.count >= self.block_restart_interval {
            self.restarts.push(self.buff.len() as u32);
            self.estimate += std::mem::size_of::<u32>();
            self.count = 0;
            if self.use_delta_encoding {
                self.last_key = key.to_vec();
            }
        } else if self.use_delta_encoding {
            shared = difference_offset(&self.last_key, key) as usize;
            self.last_key = key.to_vec();
        }
        let mut tmp: [u8; 15] = [0u8; 15];
        let non_shared = key.len() - shared;
        let curr_size = self.buff.len();
        let mut offset = encode_var_uint32(&mut tmp, shared as u32);
        let mut offset2 = encode_var_uint32(&mut tmp[offset..], non_shared as u32);
        offset += offset2;
        offset2 = encode_var_uint32(&mut tmp[offset..], value.len() as u32);
        self.buff.extend_from_slice(&tmp[0..(offset + offset2)]);
        self.buff.extend_from_slice(&key[shared..]);
        self.buff.extend_from_slice(value);
        if self.hash_index_builder.valid() {
            self.hash_index_builder
                .add(extract_user_key(key), self.restarts.len() - 1);
        }
        self.estimate += self.buff.len() - curr_size;
        self.count += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        for i in self.restarts.iter() {
            self.buff.extend_from_slice(&i.to_le_bytes());
        }
        let index_type = if self.hash_index_builder.valid()
            && self.current_size_estimate() < MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX
        {
            self.hash_index_builder.finish(&mut self.buff);
            DataBlockIndexType::DataBlockBinaryAndHash
        } else {
            DataBlockIndexType::DataBlockBinarySearch
        };
        let block_footer = pack_index_type_and_num_restarts(index_type, self.restarts.len() as u32);
        self.buff.extend_from_slice(&block_footer.to_le_bytes());
        &self.buff
    }

    pub fn clear(&mut self) {
        self.buff.clear();
        self.restarts.clear();
        self.restarts.push(0); // First restart point is at offset 0
        self.estimate = std::mem::size_of::<u32>() * 2;
        if self.hash_index_builder.valid() {
            self.hash_index_builder.clear();
        }
    }

    pub fn current_size_estimate(&self) -> usize {
        let x = if self.hash_index_builder.valid() {
            self.hash_index_builder.estimate_size()
        } else {
            0
        };
        self.estimate + x
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        DefaultUserComparator, InternalKeyComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER,
    };
    use crate::table::block_based::block::Block;
    use crate::table::InternalIterator;
    use std::sync::Arc;

    #[test]
    fn test_block_build_without_global_seqno() {
        let mut builder =
            BlockBuilder::new(5, true, DataBlockIndexType::DataBlockBinarySearch, 0.0);
        let mut kvs = vec![];
        kvs.push((b"abcdeeeeee00000001".to_vec(), b"v0"));
        kvs.push((b"abcdefffff00000001".to_vec(), b"v0"));
        kvs.push((b"abcdeggggg00000001".to_vec(), b"v0"));
        kvs.push((b"abcdehhhhh00000001".to_vec(), b"v0"));
        kvs.push((b"abcdeiiiii00000001".to_vec(), b"v0"));
        kvs.push((b"abcdejjjjj00000001".to_vec(), b"v0"));
        for i in 0..100u64 {
            let mut b = b"abcdek".to_vec();
            b.extend_from_slice(&i.to_le_bytes());
            kvs.push((b, b"v1"));
        }
        for (k, &v) in kvs.iter() {
            builder.add(&k, &v);
        }
        let data = builder.finish().to_vec();
        let block = Block::new(data, DISABLE_GLOBAL_SEQUENCE_NUMBER);
        let mut iter = block.new_data_iterator(Arc::new(InternalKeyComparator::new(Arc::new(
            DefaultUserComparator::default(),
        ))));
        iter.seek_to_first();
        let mut i = 0;
        for (k, v) in kvs {
            assert!(iter.valid());
            assert_eq!(iter.key(), k.as_slice());
            assert_eq!(iter.value(), v.as_slice());
            iter.next();
            i += 1;
        }
    }
}
