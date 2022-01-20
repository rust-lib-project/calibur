
use crate::common::{difference_offset, encode_var_uint32, extract_user_key};
use super::DataBlockHashIndexBuilder;

pub struct IndexBuilder {
    buff: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    count: usize,
    block_restart_interval: usize,
    use_delta_encoding: bool,
    use_value_delta_encoding: bool,
    hash_index_builder: DataBlockHashIndexBuilder,
}

impl IndexBuilder {
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0;
        if self.count >= self.block_restart_interval {
            self.restarts.push(self.buff.len() as u32);
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
        let mut offset= encode_var_uint32(&mut tmp, shared);
        let mut offset2 = encode_var_uint32(&mut tmp[offset..], non_shared);
        offset += offset2;
        offset2 = encode_var_uint32(&mut tmp[offset..], value.len() as u32);
        self.buff.extend_from_slice(&tmp[0..(offset + offset2)]);
        self.buff.extend_from_slice(&key[shared..]);
        self.buff.extend_from_slice(value);
        self.count += 1;
   }

    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buff)
    }

    pub fn clear(&mut self) {
        self.buff.clear();
    }


}