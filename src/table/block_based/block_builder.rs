use super::options::DataBlockIndexType;
use super::DataBlockHashIndexBuilder;
use crate::common::{difference_offset, encode_var_uint32, extract_user_key};

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
            shared = difference_offset(&self.last_key, key) as u32;
            self.last_key = key.to_vec();
        }
        let mut tmp: [u8; 15];
        let non_shared = key.len() as u32 - shared;
        let curr_size = self.buff.len();
        let mut offset = encode_var_uint32(&mut tmp, shared);
        let mut offset2 = encode_var_uint32(&mut tmp[offset..], non_shared);
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

    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buff)
    }

    pub fn clear(&mut self) {
        self.buff.clear();
        self.restarts.clear();
        self.restarts.push_back(0); // First restart point is at offset 0
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
